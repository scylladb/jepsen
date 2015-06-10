(ns cassandra.collections.map
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [checker   :as checker]
             [model     :as model]
             [generator :as gen]
             [nemesis   :as nemesis]
             [store     :as store]
             [report    :as report]
             [tests     :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control [net :as net]
             [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [cassandra.core :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLMapClient [conn]
  client/Client
  (setup! [_ test node]
    (Thread/sleep 20000)
    (locking setup-lock
      (let [conn (cassandra/connect (->> test :nodes (map name)))]
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {:class "SimpleStrategy"
                                     :replication_factor 3}}))
        (cql/use-keyspace conn "jepsen_keyspace")
        (cql/create-table conn "maps"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :elements (map-type :int :int)
                                               :primary-key [:id]}))
        (cql/insert conn "maps"
                    {:id 0
                     :elements {}})
        (CQLMapClient. conn))))
  (invoke! [this test op]
    (case (:f op)
      :add (try (with-consistency-level ConsistencyLevel/ONE
                  (cql/update conn
                              "maps"
                              {:elements [+ {(:value op) (:value op)}]}
                              (where [[= :id 0]])))
                (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :value (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All hosts are down - maybe we kill -9ed them all")
                  (assoc op :type :value :value (.getMessage e))))
      :read (try (let [value (->> (with-retry-policy aggressive-read
                                    (with-consistency-level ConsistencyLevel/ALL
                                      (cql/select conn "maps"
                                                  (where [[= :id 0]]))))
                                  first
                                  :elements
                                  vals
                                  (into (sorted-set)))]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :info :value :timed-out)))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cql-map-client
  "A set implemented using CQL maps"
  []
  (->CQLMapClient nil))

(defn cql-map-test
  [name opts]
  (merge (cassandra-test (str "cql map " name)
                         {:client (cql-map-client)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> (adds)
                                           (gen/stagger 1/10)
                                           (gen/delay 1/2)
                                           (gen/nemesis
                                            (gen/seq (cycle
                                                      [(gen/sleep 10)
                                                       {:type :info :f :start}
                                                       (gen/sleep 120)
                                                       {:type :info :f :stop}])))
                                           (gen/time-limit 600))
                                      (read-once))
                          :checker (checker/compose
                                    {:timeline timeline/html
                                     :set checker/set
                                     :latency (checker/latency-graph
                                               "report")})})
         opts))

(def bridge-test
  (cql-map-test "bridge"
                {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (cql-map-test "halves"
                {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (cql-map-test "isolate node"
                {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (cql-map-test "crash"
                {:nemesis crash-nemesis}))

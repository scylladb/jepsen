(ns cassandra.collections.set
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
           (com.datastax.driver.core ConsistencyLevel)))

(defrecord CQLSetClient [conn]
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
        (cql/create-table conn "sets"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :elements (set-type :int)
                                               :primary-key [:id]}))
        (cql/insert conn "sets"
                    {:id 0
                     :elements #{}})
        (CQLSetClient. conn))))
  (invoke! [_ test op]
    (case (:f op)
      :add (do
             (with-consistency-level ConsistencyLevel/ANY
               (cql/update conn
                           "sets"
                           {:elements [+ #{(:value op)}]}
                           (where [[= :id 0]])))
             (assoc op :type :ok))
      :read (let [value (->> (with-consistency-level ConsistencyLevel/ALL
                               (cql/select conn "sets"
                                           (where [[= :id 0]])))
                             first
                             :elements
                             (into (sorted-set)))]
              (assoc op :type :ok :value value))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cql-set-client
  "A set implemented using CQL sets"
  []
  (->CQLSetClient nil))

(defn cql-set-test
  [opts]
  (merge (cassandra-test "cql set"
                         {:client (cql-set-client)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> (adds)
                                           (gen/stagger 1/10)
                                           (gen/delay 1/5)
                                           (gen/nemesis
                                            (gen/seq (cycle
                                                      [(gen/sleep 10)
                                                       {:type :info :f :start}
                                                       (gen/sleep 120)
                                                       {:type :info :f :stop}])))
                                           (gen/time-limit 60))
                                      (read-once))
                          :checker (checker/compose
                                    {:timeline timeline/html
                                     :set checker/set
                                     :latency (checker/latency-graph
                                               "report")})})
         opts))

(ns cassandra.counter
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

(defrecord CQLCounterClient [conn]
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
        (cql/create-table conn "counters"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :count :counter
                                               :primary-key [:id]}))
        (CQLCounterClient. conn))))
  (invoke! [this test op]
    (case (:f op)
      :add (try (do
                  (with-consistency-level ConsistencyLevel/ONE
                    (cql/update conn
                                "counters"
                                {:count (increment-by (:value op))}
                                (where [[= :id 0]])))
                  (assoc op :type :ok))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :read (try (let [value (->> (with-consistency-level ConsistencyLevel/ALL
                                    (cql/select conn "counters"
                                                (where [[= :id 0]])))
                                  first
                                  :count)]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :value :timed-out))
                 (catch NoHostAvailableException e
                   (info "All the servers are down - waiting 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  []
  (->CQLCounterClient nil))

(defn cql-counter-test
  [name opts]
  (merge (cassandra-test (str "cql counter " name)
                         {:client (cql-counter-client)
                          :model nil
                          :generator (->> (repeat 100 add)
                                          (cons r)
                                          gen/mix
                                          (gen/delay 1/10)
                                          std-gen)
                          :checker (checker/compose
                                    {:timeline timeline/html
                                     :counter checker/counter
                                     :latency (checker/latency-graph
                                               "report")})})
         opts))

(def bridge-test
  (cql-counter-test "bridge"
                    {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (cql-counter-test "halves"
                    {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (cql-counter-test "isolate node"
                    {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (cql-counter-test "crash"
                    {:nemesis crash-nemesis}))

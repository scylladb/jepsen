(ns cassandra.counter
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [client    :as client]
             [checker   :as checker]
             [nemesis   :as nemesis]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :refer :all]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLCounterClient [tbl-created? session writec]
  client/Client

  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLCounterClient tbl-created? session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when
       (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :counters
                                            (if-exists false)
                                            (column-definitions {:id    :int
                                                                 :count    :counter
                                                                 :primary-key [:id]})
                                            (with {:compaction {:class (compaction-strategy)}})))
        (alia/execute session (update :counters
                                      (set-columns :count [+ 0])
                                      (where [[= :id 0]]))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (case (:f op)
      :add (try (do
                  (alia/execute session
                                (update :counters
                                        (set-columns {:count [+ (:value op)]})
                                        (where [[= :id 0]]))
                                {:consistency writec
                                 :retry-policy (retry/fallthrough-retry-policy)})
                  (assoc op :type :ok))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :read (try
              (let [value (->> (alia/execute session
                                             (select :counters (where [[= :id 0]]))
                                             {:consistency :all
                                              :retry-policy (retry/fallthrough-retry-policy)})
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

  (close! [_ _]
    (alia/shutdown session))

  (teardown! [_ _]))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (->CQLCounterClient (atom false) nil :one))
  ([writec] (->CQLCounterClient (atom false) nil writec)))

(defn cql-counter-inc-test
  [name opts]
  (merge (cassandra-test (str "cql counter inc " name)
                         {:client (cql-counter-client)
                          :generator (->> (repeat 100 add)
                                          (cons r)
                                          gen/mix
                                          (gen/delay 1/10)
                                          std-gen)
                          :checker (checker/counter)})
         opts))

(defn cql-counter-inc-dec-test
  [name opts]
  (merge (cassandra-test (str "cql counter inc dec " name)
                         {:client (cql-counter-client)
                          :generator (->> (take 100 (cycle [add sub]))
                                          (cons r)
                                          gen/mix
                                          (gen/delay 1/10)
                                          std-gen)
                          :checker (checker/counter)})
         opts))

(def bridge-inc-test
  (cql-counter-inc-test "bridge"
                        {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}}))

(def halves-inc-test
  (cql-counter-inc-test "halves"
                        {:conductors {:nemesis (nemesis/partition-random-halves)}}))

(def isolate-node-inc-test
  (cql-counter-inc-test "isolate node"
                        {:conductors {:nemesis (nemesis/partition-random-node)}}))

(def flush-compact-inc-test
  (cql-counter-inc-test "flush and compact"
                        {:conductors {:nemesis (conductors/flush-and-compacter)}}))

(def crash-subset-inc-test
  (cql-counter-inc-test "crash"
                        {:conductors {:nemesis (crash-nemesis)}}))

(def bridge-inc-dec-test
  (cql-counter-inc-dec-test "bridge"
                            {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}}))

(def halves-inc-dec-test
  (cql-counter-inc-dec-test "halves"
                            {:conductors {:nemesis (nemesis/partition-random-halves)}}))

(def isolate-node-inc-dec-test
  (cql-counter-inc-dec-test "isolate node"
                            {:conductors {:nemesis (nemesis/partition-random-node)}}))

(def crash-subset-inc-dec-test
  (cql-counter-inc-dec-test "crash"
                            {:conductors {:nemesis (crash-nemesis)}}))

(def flush-compact-inc-dec-test
  (cql-counter-inc-dec-test "flush and compact"
                            {:conductors {:nemesis (conductors/flush-and-compacter)}}))

(def bridge-inc-test-bootstrap
  (cql-counter-inc-test "bridge bootstrap"
                        {:bootstrap (atom #{:n4 :n5})
                         :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                      :bootstrapper (conductors/bootstrapper)}}))

(def halves-inc-test-bootstrap
  (cql-counter-inc-test "halves bootstrap"
                        {:bootstrap (atom #{:n4 :n5})
                         :conductors {:nemesis (nemesis/partition-random-halves)
                                      :bootstrapper (conductors/bootstrapper)}}))

(def isolate-node-inc-test-bootstrap
  (cql-counter-inc-test "isolate node bootstrap"
                        {:bootstrap (atom #{:n4 :n5})
                         :conductors {:nemesis (nemesis/partition-random-node)
                                      :bootstrapper (conductors/bootstrapper)}}))

(def crash-subset-inc-test-bootstrap
  (cql-counter-inc-test "crash bootstrap"
                        {:bootstrap (atom #{:n4 :n5})
                         :conductors {:nemesis (crash-nemesis)
                                      :bootstrapper (conductors/bootstrapper)}}))

(def bridge-inc-dec-test-bootstrap
  (cql-counter-inc-dec-test "bridge bootstrap"
                            {:bootstrap (atom #{:n4 :n5})
                             :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                          :bootstrapper (conductors/bootstrapper)}}))

(def halves-inc-dec-test-bootstrap
  (cql-counter-inc-dec-test "halves bootstrap"
                            {:bootstrap (atom #{:n4 :n5})
                             :conductors {:nemesis (nemesis/partition-random-halves)
                                          :bootstrapper (conductors/bootstrapper)}}))

(def isolate-node-inc-dec-test-bootstrap
  (cql-counter-inc-dec-test "isolate node bootstrap"
                            {:bootstrap (atom #{:n4 :n5})
                             :conductors {:nemesis (nemesis/partition-random-node)
                                          :bootstrapper (conductors/bootstrapper)}}))

(def crash-subset-inc-dec-test-bootstrap
  (cql-counter-inc-dec-test "crash bootstrap"
                            {:bootstrap (atom #{:n4 :n5})
                             :conductors {:nemesis (crash-nemesis)
                                          :bootstrapper (conductors/bootstrapper)}}))

(def bridge-inc-test-decommission
  (cql-counter-inc-test "bridge decommission"
                        {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                      :bootstrapper (conductors/bootstrapper)}}))

(def halves-inc-test-decommission
  (cql-counter-inc-test "halves decommission"
                        {:conductors {:nemesis (nemesis/partition-random-halves)
                                      :decommissioner (conductors/decommissioner)}}))

(def isolate-node-inc-test-decommission
  (cql-counter-inc-test "isolate node decommission"
                        {:conductors {:nemesis (nemesis/partition-random-node)
                                      :decommissioner (conductors/decommissioner)}}))

(def crash-subset-inc-test-decommission
  (cql-counter-inc-test "crash decommission"
                        {:client (cql-counter-client :quorum)
                         :conductors {:nemesis (crash-nemesis)
                                      :decommissioner (conductors/decommissioner)}}))

(def bridge-inc-dec-test-decommission
  (cql-counter-inc-dec-test "bridge decommission"
                            {:conductors {:nemesis (nemesis/partitioner
                                                    (comp nemesis/bridge shuffle))
                                          :decommissioner (conductors/decommissioner)}}))

(def halves-inc-dec-test-decommission
  (cql-counter-inc-dec-test "halves decommission"
                            {:conductors {:nemesis (nemesis/partition-random-halves)
                                          :decommissioner (conductors/decommissioner)}}))

(def isolate-node-inc-dec-test-decommission
  (cql-counter-inc-dec-test "isolate node decommission"
                            {:conductors {:nemesis (nemesis/partition-random-node)
                                          :decommissioner (conductors/decommissioner)}}))

(def crash-subset-inc-dec-test-decommission
  (cql-counter-inc-dec-test "crash decommission"
                            {:client (cql-counter-client :quorum)
                             :conductors {:nemesis (crash-nemesis)
                                          :decommissioner (conductors/decommissioner)}}))
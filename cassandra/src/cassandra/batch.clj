(ns cassandra.batch
  (:require [clojure [pprint :refer :all]
             [string :as str]
             [set :as set]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord BatchSetClient [tbl-created? session]
  client/Client

  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->BatchSetClient tbl-created? session)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :bat
                                            (if-exists false)
                                            (column-definitions {:pid    :int
                                                                 :cid    :int
                                                                 :value  :int
                                                                 :primary-key [:pid :cid]})
                                            (with {:compaction {:class (compaction-strategy)}}))))))

  (invoke! [this test op]
    (alia/execute session (use-keyspace :jepsen_keyspace))

    (case (:f op)
      :add (try (let [value (:value op)]
                  (alia/execute session
                                (str "BEGIN BATCH "
                                     "INSERT INTO bat (pid, cid, value) VALUES ("
                                     value ", 0, " value ");"
                                     "INSERT INTO bat (pid, cid, value) VALUES ("
                                     value ", 1, " value ");"
                                     "APPLY BATCH;")
                                {:consistency :quorum})
                  (assoc op :type :ok))
                (catch UnavailableException e
                  (assoc op :type :fail :value (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All nodes are down - sleeping 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :value (.getMessage e))))
      :read (try (let [results (alia/execute session
                                             (select :bat
                                                     {:consistency :all}))
                       value-a (->> results
                                    (filter (fn [ret] (= (:cid ret) 0)))
                                    (map :value)
                                    (into (sorted-set)))
                       value-b (->> results
                                    (filter (fn [ret] (= (:cid ret) 1)))
                                    (map :value)
                                    (into (sorted-set)))]
                   (if-not (= value-a value-b)
                     (assoc op :type :fail :value [value-a value-b])
                     (assoc op :type :ok :value value-a)))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :value :timed-out))
                 (catch NoHostAvailableException e
                   (info "All nodes are down - sleeping 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :value (.getMessage e))))))
  (close! [_ _]
    (alia/shutdown session))

  (teardown! [_ _]))

(defn batch-set-client
  "A set implemented using batched inserts"
  []
  (->BatchSetClient (atom false) nil))

(defn batch-set-test
  [name opts]
  (merge (cassandra-test (str "batch set " name)
                         {:client (batch-set-client)
                          :generator (gen/phases
                                      (->> (gen/clients (adds))
                                           (gen/delay 1)
                                           std-gen)
                                      (gen/delay 65
                                                 (read-once)))
                          :checker (checker/set)})
         (merge-with merge {:conductors {:replayer (conductors/replayer)}} opts)))

(def bridge-test
  (batch-set-test "bridge"
                  {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}}))

(def halves-test

  (batch-set-test "halves"
                  {:conductors {:nemesis (nemesis/partition-random-halves)}}))

(def isolate-node-test
  (batch-set-test "isolate node"
                  {:conductors {:nemesis (nemesis/partition-random-node)}}))

(def crash-subset-test
  (batch-set-test "crash"
                  {:conductors {:nemesis (crash-nemesis)}}))

(def clock-drift-test
  (batch-set-test "clock drift"
                  {:conductors {:nemesis (nemesis/clock-scrambler 10000)}}))

(def flush-compact-test
  (batch-set-test "flush and compact"
                  {:conductors {:nemesis (conductors/flush-and-compacter)}}))

(def bridge-test-bootstrap
  (batch-set-test "bridge bootstrap"
                  {:bootstrap (atom #{:n4 :n5})
                   :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                :bootstrapper (conductors/bootstrapper)}}))

(def halves-test-bootstrap
  (batch-set-test "halves bootstrap"
                  {:bootstrap (atom #{:n4 :n5})
                   :conductors {:nemesis (nemesis/partition-random-halves)
                                :bootstrapper (conductors/bootstrapper)}}))

(def isolate-node-test-bootstrap
  (batch-set-test "isolate node bootstrap"
                  {:bootstrap (atom #{:n4 :n5})
                   :conductors {:nemesis (nemesis/partition-random-node)
                                :bootstrapper (conductors/bootstrapper)}}))

(def crash-subset-test-bootstrap
  (batch-set-test "crash bootstrap"
                  {:bootstrap (atom #{:n4 :n5})
                   :conductors {:nemesis (crash-nemesis)
                                :bootstrapper (conductors/bootstrapper)}}))

(def clock-drift-test-bootstrap
  (batch-set-test "clock drift bootstrap"
                  {:bootstrap (atom #{:n4 :n5})
                   :conductors {:nemesis (nemesis/clock-scrambler 10000)
                                :bootstrapper (conductors/bootstrapper)}}))

(def bridge-test-decommission
  (batch-set-test "bridge decommission"
                  {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                :decommissioner (conductors/decommissioner)}}))

(def halves-test-decommission
  (batch-set-test "halves decommission"
                  {:conductors {:nemesis (nemesis/partition-random-halves)
                                :decommissioner (conductors/decommissioner)}}))

(def isolate-node-test-decommission
  (batch-set-test "isolate node decommission"
                  {:conductors {:nemesis (nemesis/partition-random-node)
                                :decommissioner (conductors/decommissioner)}}))

(def crash-subset-test-decommission
  (batch-set-test "crash decommission"
                  {:conductors {:nemesis (crash-nemesis)
                                :decommissioner (conductors/decommissioner)}}))

(def clock-drift-test-decommission
  (batch-set-test "clock drift decommission"
                  {:conductors {:nemesis (nemesis/clock-scrambler 10000)
                                :decommissioner (conductors/decommissioner)}}))

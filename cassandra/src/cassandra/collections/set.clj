(ns cassandra.collections.set
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLSetClient [tbl-created? session writec]
  client/Client

  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLSetClient tbl-created? session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :sets
                                            (if-exists false)
                                            (column-definitions {:id    :int
                                                                 :elements    (set-type :int)
                                                                 :primary-key [:id]})
                                            (with {:compaction {:class (compaction-strategy)}})))
        (alia/execute session (insert :sets
                                      (values [[:id 0]
                                               [:elements #{}]])
                                      (if-exists false))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (case (:f op)
      :add (try
             (alia/execute session
                           (update :sets
                                   (set-columns {:elements [+ #{(:value op)}]})
                                   (where [[= :id 0]]))
                           {:consistency writec})
             (assoc op :type :ok)
             (catch UnavailableException e
               (assoc op :type :fail :value (.getMessage e)))
             (catch WriteTimeoutException e
               (assoc op :type :info :value :timed-out))
             (catch NoHostAvailableException e
               (info "All nodes are down - sleeping 2s")
               (Thread/sleep 2000)
               (assoc op :type :fail :value (.getMessage e))))
      :read (try
              (wait-for-recovery 30 session)
              (let [value (->> (alia/execute session
                                             (select :sets
                                                     (where [[= :id 0]]))
                                             {:consistency :all
                                              :retry-policy aggressive-read})
                               first
                               :elements
                               (into (sorted-set)))]
                (assoc op :type :ok :value value))
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

(defn cql-set-client
  "A set implemented using CQL sets"
  ([] (->CQLSetClient (atom false) nil :one))
  ([writec] (->CQLSetClient (atom false) nil writec)))

(defn cql-set-test
  [name opts]
  (merge (cassandra-test (str "cql set " name)
                         {:client (cql-set-client)
                          :generator (gen/phases
                                      (->> (adds)
                                           (gen/stagger 1/10)
                                           (gen/delay 1/2)
                                           std-gen)
                                      (read-once))
                          :checker (checker/set)})
         opts))

(def bridge-test
  (cql-set-test "bridge"
                {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}}))

(def halves-test
  (cql-set-test "halves"
                {:conductors {:nemesis (nemesis/partition-random-halves)}}))

(def isolate-node-test
  (cql-set-test "isolate node"
                {:conductors {:nemesis (nemesis/partition-random-node)}}))

(def crash-subset-test
  (cql-set-test "crash"
                {:conductors {:nemesis (crash-nemesis)}}))

(def flush-compact-test
  (cql-set-test "flush and compact"
                {:conductors {:nemesis (conductors/flush-and-compacter)}}))

(def bridge-test-bootstrap
  (cql-set-test "bridge bootstrap"
                {:bootstrap (atom #{:n4 :n5})
                 :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                              :bootstrapper (conductors/bootstrapper)}}))

(def halves-test-bootstrap
  (cql-set-test "halves bootstrap"
                {:bootstrap (atom #{:n4 :n5})
                 :conductors {:nemesis (nemesis/partition-random-halves)
                              :bootstrapper (conductors/bootstrapper)}}))

(def isolate-node-test-bootstrap
  (cql-set-test "isolate node bootstrap"
                {:bootstrap (atom #{:n4 :n5})
                 :conductors {:nemesis (nemesis/partition-random-node)
                              :bootstrapper (conductors/bootstrapper)}}))

(def crash-subset-test-bootstrap
  (cql-set-test "crash bootstrap"
                {:bootstrap (atom #{:n4 :n5})
                 :conductors {:nemesis (crash-nemesis)
                              :bootstrapper (conductors/bootstrapper)}}))

(def bridge-test-decommission
  (cql-set-test "bridge decommission"
                {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                              :decommissioner (conductors/decommissioner)}}))

(def halves-test-decommission
  (cql-set-test "halves decommission"
                {:conductors {:nemesis (nemesis/partition-random-halves)
                              :decommissioner (conductors/decommissioner)}}))

(def isolate-node-test-decommission
  (cql-set-test "isolate node decommission"
                {:conductors {:nemesis (nemesis/partition-random-node)
                              :decommissioner (conductors/decommissioner)}}))

(def crash-subset-test-decommission
  (cql-set-test "crash decommission"
                {:client (cql-set-client :quorum)
                 :conductors {:nemesis (crash-nemesis)
                              :decommissioner (conductors/decommissioner)}}))

(ns scylla.collections.map
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [scylla.core :refer :all]
            [scylla.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLMapClient [tbl-created? session writec]
  client/Client

  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLMapClient tbl-created? session writec)))


  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :maps
                                            (if-exists false)
                                            (column-definitions {:id    :int
                                                                 :elements    (map-type :int :int)
                                                                 :primary-key [:id]})
                                            (with {:compaction {:class (compaction-strategy)}})))
        (alia/execute session (insert :maps (values [[:id 0]
                                                     [:elements {}]]))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (case (:f op)
      :add (try
             (alia/execute session
                           (update :maps
                                   (set-columns {:elements [+ {(:value op) (:value op)}]})
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
                                             (select :maps (where [[= :id 0]]))
                                             {:consistency :all
                                              :retry-policy aggressive-read})
                               first
                               :elements
                               vals
                               (into (sorted-set)))]
                (assoc op :type :ok :value value))
              (catch UnavailableException e
                (info "Not enough replicas - failing")
                (assoc op :type :fail :value (.getMessage e)))
              (catch ReadTimeoutException e
                (assoc op :type :fail :value :timed-out)))))

  (close! [_ _]
    (alia/shutdown session))

  (teardown! [_ _]))

(defn cql-map-client
  "A set implemented using CQL maps"
  ([] (->CQLMapClient (atom false) nil :one))
  ([writec] (->CQLMapClient (atom false) nil writec)))

(defn cql-map-test
  [name opts]
  (merge (scylla-test (str "cql map " name)
                         {:client (cql-map-client)
                          :generator (gen/phases
                                      (->> (adds)
                                           (gen/stagger 1/10)
                                           (gen/delay 1/2)
                                           std-gen)
                                      (read-once))
                          :checker (checker/set)})
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
                {:nemesis (crash-nemesis)}))

(def flush-compact-test
  (cql-map-test "flush and compact"
                {:nemesis (conductors/flush-and-compacter)}))

;(def bridge-test-bootstrap
;  (cql-map-test "bridge bootstrap"
;                {:bootstrap (atom #{:n4 :n5})
;                 :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                              :bootstrapper (conductors/bootstrapper)}}))
;
;(def halves-test-bootstrap
;  (cql-map-test "halves bootstrap"
;                {:bootstrap (atom #{:n4 :n5})
;                 :conductors {:nemesis (nemesis/partition-random-halves)
;                              :bootstrapper (conductors/bootstrapper)}}))
;
;(def isolate-node-test-bootstrap
;  (cql-map-test "isolate node bootstrap"
;                {:bootstrap (atom #{:n4 :n5})
;                 :conductors {:nemesis (nemesis/partition-random-node)
;                              :bootstrapper (conductors/bootstrapper)}}))
;
;(def crash-subset-test-bootstrap
;  (cql-map-test "crash bootstrap"
;                {:bootstrap (atom #{:n4 :n5})
;                 :conductors {:nemesis (crash-nemesis)
;                              :bootstrapper (conductors/bootstrapper)}}))
;
;(def bridge-test-decommission
;  (cql-map-test "bridge decommission"
;                {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                              :decommissioner (conductors/decommissioner)}}))
;
;(def halves-test-decommission
;  (cql-map-test "halves decommission"
;                {:conductors {:nemesis (nemesis/partition-random-halves)
;                              :decommissioner (conductors/decommissioner)}}))
;
;(def isolate-node-test-decommission
;  (cql-map-test "isolate node decommission"
;                {:conductors {:nemesis (nemesis/partition-random-node)
;                              :decommissioner (conductors/decommissioner)}}))
;
;(def crash-subset-test-decommission
;  (cql-map-test "crash decommission"
;                {:client (cql-map-client :quorum)
;                 :conductors {:nemesis (crash-nemesis)
;                              :decommissioner (conductors/decommissioner)}}))

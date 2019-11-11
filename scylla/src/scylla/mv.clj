(ns scylla.mv
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [qbits.alia :as alia]
            [qbits.alia.policy.load-balancing :as load-balancing]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :refer :all]
            [scylla.core :refer :all]
            [scylla.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)
           (java.net InetSocketAddress)))

(defrecord MVMapClient [tbl-created? session connect-type read-cl]
  client/Client

  (open! [_ test node]
    (let [cluster (alia/cluster {:contact-points (:nodes test)
                                 :load-balancing-policy (load-balancing/whitelist-policy
                                                         (load-balancing/round-robin-policy)
                                                         (map (fn [node-name]
                                                                (InetSocketAddress. node-name 9042))
                                                              (if (= connect-type :single)
                                                                (do
                                                                  (info "load balancing only" node)
                                                                  [(name node)])
                                                                (:nodes test))))})
          session (alia/connect cluster)]
      (->MVMapClient tbl-created? session connect-type read-cl)))
  
  (setup! [_ test]
    (locking tbl-created?
      (when
       (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :map
                                            (if-exists false)
                                            (column-definitions {:key    :int
                                                                 :value    :int
                                                                 :primary-key [:key]})
                                            (with {:compaction {:class (compaction-strategy)}})))
        (try (alia/execute session (str "CREATE MATERIALIZED VIEW mvmap AS SELECT"
                                        " * FROM map WHERE value IS NOT NULL"
                                        " AND key IS NOT NULL "
                                        "PRIMARY KEY (value, key)"
                                        "WITH compaction = "
                                        "{'class' : '" (compaction-strategy)
                                        "'};"))
             (catch com.datastax.driver.core.exceptions.AlreadyExistsException e)))))
  
  (invoke! [_ _ op]
           (alia/execute session (use-keyspace :jepsen_keyspace))
           (case (:f op)
             :assoc (try
                      (alia/execute session
                                    (update :map
                                            (set-columns {:value (:v (:value op))})
                                            (where [[= :key (:k (:value op))]]))
                                    {:consistency :one
                                     :retry-policy (retry/fallthrough-retry-policy)})
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
                     (let [value (->> (alia/execute session
                                                    (select :mvmap)
                                                    {:consistency read-cl})
                                      (#(zipmap (map :key %) (map :value %))))]
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

(defn mv-map-client
  "A map implemented using MV"
  ([]
   (->MVMapClient (atom false) nil :all :all))
  ([load-balancing-policy]
   (->MVMapClient (atom false) nil load-balancing-policy :all))
  ([load-balancing-policy read-cl]
   (->MVMapClient (atom false) nil load-balancing-policy read-cl)))

(defn mv-map-test
  [name opts]
  (merge (scylla-test (str "mv contended map " name)
                         {:client (mv-map-client)
                          :generator (gen/phases
                                      (->> (gen/clients (assocs identity))
                                           (gen/delay 1)
                                           (std-gen 250))
                                      (gen/conductor :replayer
                                                     (gen/once {:type :info :f :replay}))
                                      (read-once)
                                      (->> (gen/clients (assocs -))
                                           (gen/delay 1)
                                           (std-gen 250))
                                      (gen/conductor :replayer
                                                     (gen/once {:type :info :f :replay}))
                                      (read-once))
                          :checker (checker/associative-map)})
         (merge-with merge {:conductors {:replayer (conductors/replayer)}} opts)))

(def paired-nodes
  (comp nemesis/complete-grudge (partial partition-all 2)))

(def isolate-all
  (comp nemesis/complete-grudge (partial map vector)))

;; Uncontended tests
(def bridge-test
  (mv-map-test "bridge"
               {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (mv-map-test "halves"
               {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (mv-map-test "isolate node"
               {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (mv-map-test "crash"
               {:nemesis (crash-nemesis)}))

(def clock-drift-test
  (mv-map-test "clock drift"
               {:nemesis (nemesis/clock-scrambler 10000)}))

(def flush-compact-test
  (mv-map-test "flush and compact"
               {:nemesis (conductors/flush-and-compacter)}))

;(def bridge-test-bootstrap
;  (mv-map-test "bridge bootstrap"
;               {:bootstrap (atom #{:n4 :n5})
;                :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                             :bootstrapper (conductors/bootstrapper)}}))
;
;(def halves-test-bootstrap
;  (mv-map-test "halves bootstrap"
;               {:bootstrap (atom #{:n4 :n5})
;                :conductors {:nemesis (nemesis/partition-random-halves)
;                             :bootstrapper (conductors/bootstrapper)}}))
;
;(def isolate-node-test-bootstrap
;  (mv-map-test "isolate node bootstrap"
;               {:bootstrap (atom #{:n4 :n5})
;                :conductors {:nemesis (nemesis/partition-random-node)
;                             :bootstrapper (conductors/bootstrapper)}}))
;
;(def crash-subset-test-bootstrap
;  (mv-map-test "crash bootstrap"
;               {:bootstrap (atom #{:n4 :n5})
;                :conductors {:nemesis (crash-nemesis)
;                             :bootstrapper (conductors/bootstrapper)}}))
;
;(def clock-drift-test-bootstrap
;  (mv-map-test "clock drift bootstrap"
;               {:bootstrap (atom #{:n4 :n5})
;                :conductors {:nemesis (nemesis/clock-scrambler 10000)
;                             :bootstrapper (conductors/bootstrapper)}}))
;
;(def bridge-test-decommission
;  (mv-map-test "bridge decommission"
;               {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                             :decommissioner (conductors/decommissioner)}}))
;
;(def halves-test-decommission
;  (mv-map-test "halves decommission"
;               {:conductors {:nemesis (nemesis/partition-random-halves)
;                             :decommissioner (conductors/decommissioner)}}))
;
;(def isolate-node-test-decommission
;  (mv-map-test "isolate node decommission"
;               {:conductors {:nemesis (nemesis/partition-random-node)
;                             :decommissioner (conductors/decommissioner)}}))
;
;(def crash-subset-test-decommission
;  (mv-map-test "crash decommission"
;               {:conductors {:nemesis (crash-nemesis)
;                             :decommissioner (conductors/decommissioner)}}))
;
;(def clock-drift-test-decommission
;  (mv-map-test "clock drift decommission"
;               {:conductors {:nemesis (nemesis/clock-scrambler 10000)
;                             :decommissioner (conductors/decommissioner)}}))

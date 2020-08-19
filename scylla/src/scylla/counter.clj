(ns scylla.counter
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [nemesis   :as nemesis]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :refer :all]
            [scylla [client :as c]
                    [db :as db]]))

(defrecord CQLCounterClient [tbl-created? conn writec]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [_ test]
    (let [session (:session conn)]
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
                                              (with {:compaction {:class (db/compaction-strategy)}})))
          (alia/execute session (update :counters
                                        (set-columns :count [+ 0])
                                        (where [[= :id 0]])))))))

  (invoke! [_ _ op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do (alia/execute s
                                 (update :counters
                                         (set-columns {:count [+ (:value op)]})
                                         (where [[= :id 0]]))
                                 {:consistency writec
                                  :retry-policy (retry/fallthrough-retry-policy)})
                   (assoc op :type :ok))

          :read (let [value (->> (alia/execute s
                                               (select :counters (where [[= :id 0]]))
                                               {:consistency :all
                                                :retry-policy (retry/fallthrough-retry-policy)})
                                 first
                                 :count)]
                    (assoc op :type :ok :value value))))))


  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _]))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (->CQLCounterClient (atom false) nil :one))
  ([writec] (->CQLCounterClient (atom false) nil writec)))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r {:type :invoke :f :read})

(defn workload
  "An increment-only counter workload."
  [opts]
  {:client    (cql-counter-client)
   :generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix)
   :checker   (checker/counter)})

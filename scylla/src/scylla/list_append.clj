(ns scylla.list-append
  "This test performs transactional appends and reads of various keys--each a
  distinct row containing a single CQL list value."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle.append :as append]
            [scylla [client :as c]
                    [db :as db]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(defn table-for
  "What table should we use for this key?"
  [test k]
  (str "lists"))

(defn all-tables
  "All tables for a test."
  [test]
  (mapv (partial table-for test) [0]))

(defn mop-query
  "Takes a test and an [f k v] micro-op. Generates a query for this micro-op,
  suitable for inclusion in a batch transaction. For reads, we perform an
  update which is guaranted to fail, and take advantage of the update returns
  the non-matching result."
  [test [f k v]]
  (case f
    :append (h/update (table-for test k)
                      (h/set-columns {:value [+ [v]]})
                      (h/where [[= :part 0]
                                [= :id k]])
                      ; This trivial IF always returns true.
                      (h/only-if [[= :lwt_dummy nil]]))
    :r (h/update (table-for test k)
                 (h/set-columns {:lwt_dummy nil})
                 (h/where [[= :part 0]
                           [= :id k]])
                 (h/only-if [[= :lwt_dummy nil]]))))

(defn apply-batch!
  "Takes a test, a session, and a txn. Performs the txn in a batch, batch,
  returning the resulting txn."
  [test session txn]
  (let [queries (map (partial mop-query test) txn)
        _ (info :queries queries)
        results (a/execute session (h/batch (apply h/queries queries)))]
    (assert (= (count queries) (count results))
            (str "Didn't get enough results for txn " txn ": " (pr-str results)))
    (map (fn [[f k v :as mop] res]
           (info :res [f k v] (pr-str res))
           (case f
             :r     [f k (:value res)]
             :append mop))
         txn
         results)))

(defn read
  "Takes a test, session, and a transaction. Assuming that all keys are in the
  same table, performs a single CQL select of all keys in the transaction, and
  returns the transaction with the values for those keys."
  [test session txn]
  (let [ks        (distinct (map second txn))
        _         (prn :ks ks)
        table     (table-for test (first ks))
        results   (->> (a/execute session (h/select table
                                                    (h/where [[= :part 0]
                                                              [:in :id ks]]))
                                  {:consistency :serial})
                       (map (juxt :id :value))
                       (into {}))]
    (mapv (fn [[f k _]] [f k (results k)]) txn)))

(defn write-only?
  "Is this operation a write-only txn?"
  [op]
  (every? (comp #{:append} first) (:value op)))

(defn read-only?
  "Is this operation a read-only txn?"
  [op]
  (every? (comp #{:r} first) (:value op)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [this test]
    (let [s (:session conn)]
      (c/retry-each
        (a/execute s (h/create-keyspace
                       :jepsen_keyspace
                       (h/if-exists false)
                       (h/with {:replication {:class :SimpleStrategy
                                              :replication_factor 3}})))
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (doseq [t (all-tables test)]
          (a/execute s (h/create-table
                         t
                         (h/if-exists false)
                         (h/column-definitions {:part         :int
                                                :id           :int
                                                ; We can't do LWT without SOME
                                                ; kind of IF statement (why?),
                                                ; so we leave a dummy null
                                                ; column here.
                                                :lwt_dummy    :int
                                                :value        (h/list-type :int)
                                                :primary-key  [:part :id]})
                         (h/with {:compaction {:class (db/compaction-strategy)}})))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{}
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (assoc op
               :type  :ok
               :value (apply-batch! test s (:value op))))))

  (close! [this test]
    (c/close! conn))

  (teardown! [this test]))

(defn valid-op?
  "Is this operation a read- or write-only txn?"
  [op]
  (or (write-only? op)
      (read-only? op)))

(defn workload
  "See options for jepsen.tests.append/test"
  [opts]
  (let [w (append/test opts)]
    ; CQL can't do mixed read-write queries, so we filter ourselves to
    ; write-only and read-only workloads.
    (assoc w
           :client (Client. nil))))

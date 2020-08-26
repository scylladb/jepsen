(ns scylla.wr-register
  "This test performs transactional writes and reads to a set of registers, each stored in a distinct row containing a single int value."
  (:refer-clojure :exclude [read])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle.wr :as wr]
            [scylla [client :as c]
                    [db :as db]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(defn table-for
  "What table should we use for this key?"
  [test k]
  (str "registers"))

(defn all-tables
  "All tables for a test."
  [test]
  (mapv (partial table-for test) [0]))

(defn maybe-long
  "Coerces non-null values to longs; nil values remain nil."
  [x]
  (when x (long x)))

(defn write-batch!
  "Takes a test, a session, and a write-only txn. Performs the txn in a batch,
  batch, returning the resulting txn."
  [test session txn]
  (let [queries (map (fn [[f k v]]
                       (h/update (table-for test k)
                                 (h/set-columns {:value v})
                                 (h/where [[= :part 0]
                                           [= :id k]])
                                 ; This trivial IF always returns true.
                                 (h/only-if [[= :lwt_dummy nil]])))
                     txn)
        ; _ (info :queries queries)
        results (a/execute session (h/batch (apply h/queries queries)))]
    ; Batch results make no sense so we... just ignore them. Wooo!)
    txn))

(defn read-many
  "Takes a test, a session, and a read-only txn. Performs the read as a single
  CQL select, returning the resulting txn."
  [test session txn]
  (let [ks      (distinct (map second txn))
        tables  (distinct (map (partial table-for test) ks))
        _       (assert (= 1 (count tables)))
        table   (first tables)
        results (a/execute session
                           (h/select table
                                     (h/where [[= :part 0]
                                               [:in :id ks]]))
                           {:consistency :serial})
        values  (into {} (map (juxt :id (comp maybe-long :value)) results))]
    (mapv (fn [[f k v]] [f k (get values k)]) txn)))

(defn single-read
  "Takes a test, session, and a transaction with a single read mop. performs a
  single CQL select by primary key, and returns the completed txn."
  [test session [[f k v]]]
  [[f k (->> (a/execute session
                        (h/select (table-for test k)
                                  (h/where [[= :part 0]
                                            [= :id   k]]))
                        {:consistency :serial})
             first
             :value
             maybe-long)]])

(defn single-write!
  "Takes a test, session, and a transaction with a single write mop. Performs
  the write via a CQL conditional update."
  [test session txn]
  (let [[f k v] (first txn)]
    (a/execute session
               (h/update (table-for test k)
                         (h/set-columns {:value v})
                         (h/where [[= :part 0]
                                   [= :id k]])
                         (h/only-if [[= :lwt_dummy nil]]))))
  txn)

(defn write-only?
  "Is this txn write-only?"
  [txn]
  (every? (comp #{:w} first) txn))

(defn read-only?
  "Is this txn read-only?"
  [txn]
  (every? (comp #{:r} first) txn))

(defn apply-txn!
  "Takes a test, a session, and a txn. Performs the txn, returning the
  completed txn."
  [test session txn]
  (if (= 1 (count txn))
    (cond (read-only?  txn) (single-read   test session txn)
          (write-only? txn) (single-write! test session txn)
          true              (assert false "what even is this"))
    (cond (read-only? txn)  (read-many     test session txn)
          (write-only? txn) (write-batch!  test session txn)
          true              (assert false "what even is this"))))

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
                                                :value        :int
                                                :primary-key  [:part :id]})
                         (h/with {:compaction {:class (db/compaction-strategy)}})))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{}
        (a/execute s (h/use-keyspace :jepsen_keyspace))
        (assoc op
               :type  :ok
               :value (apply-txn! test s (:value op))))))

  (close! [this test]
    (c/close! conn))

  (teardown! [this test]))

(defn workload
  "See options for jepsen.tests.append/test"
  [opts]
  {:client    (Client. nil)
   :generator (gen/filter (fn [op]
                            (let [txn (:value op)]
                              (or (read-only? txn)
                                  (write-only? txn))))
                          (wr/gen opts))
   :checker   (wr/checker (merge {:linearizable-keys? true}
                                 opts))})

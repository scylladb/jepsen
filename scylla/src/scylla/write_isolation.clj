(ns scylla.write-isolation
  "This is a variant workload designed to verify a pair of Scylla claims from
  https://docs.scylladb.com/getting-started/dml

  > All updates for an INSERT are applied atomically and in isolation.

  > In an UPDATE statement, all updates within the same partition key are
  > applied atomically and in isolation.

  To check this, we constrain our generator such that every update is a batch
  update to the same set of keys; each update picks exactly one value. Since
  updates are applied in isolation, we should never observe a mixed state."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [knossos.op :as op]
            [scylla [client :as c]
                    [wr-register :as wr-register]]
            [qbits [alia :as a]
                   [hayt :as h]]))

(def key-range-size
  "How many keys per range?"
  3)

(def num-key-ranges
  "How many key ranges total?"
  3)

(defn key-range
  "Returns a collection of keys for a read or write txn to operate on."
  []
  (let [lower (* (rand-int num-key-ranges) key-range-size)
        upper (+ lower key-range-size)]
    (shuffle (range lower upper))))

(defn same-write?
  "Are all these values part of the same write?"
  [values]
  ; Handle nils
  (or (apply = values)
      ; Or negative/positive variants
      (apply = (map #(Math/abs %) values))))

(defn writes
  "Constructs txns like [[:w 0 1] [:w 1 -1] [:w 2 -1] [:w 3 1]]"
  []
  (->> (range)
       (map (fn [v]
              {:f     :write
               :value (mapv (fn [k]
                              (let [v (* v (rand-nth [1 -1]))]
                                [:w k v]))
                            (key-range))}))))

(defn reads
  "Constructs txns like [[:r 0 nil] [:r 1 nil] ...]"
  []
  (fn [] {:f      :read
          :value  (mapv (fn [k] [:r k nil]) (key-range))}))

(defn generator
  []
  (gen/mix [(writes) (reads)]))

(defn checker
  "Looks for successful reads where not every value is from the same write."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (filter op/ok?)
                      (filter (comp #{:read} :f))
                      (keep (fn [op]
                              (when (not (same-write?
                                           (map #(nth % 2) (:value op))))
                                op))))]
        {:valid?      (empty? errs)
         :error-count (count errs)
         :errors      errs}))))

(defn workload
  [opts]
  {:client    (wr-register/->Client nil)
   :generator (generator)
   :checker   (checker)})

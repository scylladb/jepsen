(ns scylla.collections.map
  "TODO: what is this for? I think it's... a set? Backed by a CQL map?"
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

(defrecord CQLMapClient [tbl-created? conn writec]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [_ test]
    (let [s (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (alia/execute s (create-keyspace :jepsen_keyspace
                                           (if-exists false)
                                           (with {:replication {:class :SimpleStrategy
                                                                :replication_factor 3}})))
          (alia/execute s (use-keyspace :jepsen_keyspace))
          (alia/execute s (create-table :maps
                                        (if-exists false)
                                        (column-definitions {:id    :int
                                                             :elements    (map-type :int :int)
                                                             :primary-key [:id]})
                                        (with {:compaction {:class (db/compaction-strategy)}})))
          (alia/execute s (insert :maps (values [[:id 0]
                                                 [:elements {}]])))))))

  (invoke! [_ test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do (alia/execute s
                                 (update :maps
                                         (set-columns {:elements [+ {(:value op) (:value op)}]})
                                         (where [[= :id 0]]))
                                 {:consistency writec})

                   (assoc op :type :ok))
          :read (do (db/wait-for-recovery 30 s)
                    (let [value (->> (alia/execute s
                                                   (select :maps (where [[= :id 0]]))
                                                   {:consistency :all
                                                    :retry-policy c/aggressive-read})
                                     first
                                     :elements
                                     vals
                                     (into (sorted-set)))]
                    (assoc op :type :ok :value value)))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _]))

(defn cql-map-client
  "A set implemented using CQL maps"
  ([] (->CQLMapClient (atom false) nil :one))
  ([writec] (->CQLMapClient (atom false) nil writec)))

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn workload
  [opts]
  {:client          (cql-map-client)
   :generator       (adds)
   :final-generator (gen/once {:type :invoke, :f :read})
   :checker         (checker/set)})

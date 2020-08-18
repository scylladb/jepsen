(ns scylla.cas-register
  "Linearizable, single-register operations backed by lightweight transactions."
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [scylla [client :as c]
                    [db :as db]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(def ak (keyword "[applied]")) ;this is the name C* returns, define now because
                               ;it isn't really a valid keyword from reader's
                               ;perspective

(defrecord CasRegisterClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [_ test]
    (let [session (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (alia/execute session (create-keyspace :jepsen_keyspace
                                                 (if-exists false)
                                                 (with {:replication {:class :SimpleStrategy
                                                                      :replication_factor 3}})))
          (alia/execute session (use-keyspace :jepsen_keyspace))
          (alia/execute session (create-table :lwt
                                              (if-exists false)
                                              (column-definitions {:id    :int
                                                                   :value :int
                                                                   :primary-key [:id]})
                                              (with {:compaction {:class (db/compaction-strategy)}})))))))

  (invoke! [_ _ op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (use-keyspace :jepsen_keyspace))
        (case (:f op)
          :cas (let [[old new] (:value op)
                     result (alia/execute s
                                          (update :lwt
                                                  (set-columns {:value new})
                                                  (where [[= :id 0]])
                                                  (only-if [[:value old]])))]
                 (if (-> result first ak)
                   (assoc op :type :ok)
                   (assoc op :type :fail :error (-> result first :value))))

          :write (let [v (:value op)
                       result (alia/execute s (update :lwt
                                                      (set-columns {:value v})
                                                      (only-if [[:in :value (range 5)]])
                                                      (where [[= :id 0]])))]
                   (if (-> result first ak)
                     (assoc op :type :ok)
                     (let [result' (alia/execute s (insert :lwt
                                                           (values [[:id 0]
                                                                    [:value v]])
                                                           (if-exists false)))]
                       (if (-> result' first ak)
                         (assoc op :type :ok)
                         (assoc op :type :fail)))))

          :read (let [v (->> (alia/execute s
                                           (select :lwt (where [[= :id 0]]))
                                           {:consistency :serial})
                             first
                             :value)]
                  (assoc op :type :ok :value v))))))

  (close! [_ _]
          (c/close! conn))

  (teardown! [_ _]))

(defn cas-register-client
  "A CAS register implemented using LWT"
  []
  (->CasRegisterClient (atom false) nil))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn workload
  [opts]
  {:client (cas-register-client)
   :generator (gen/mix [r w cas cas])
   :checker   (checker/linearizable {:model (model/cas-register)})})

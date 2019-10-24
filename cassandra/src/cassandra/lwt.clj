(ns cassandra.lwt
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt :refer :all]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(def ak (keyword "[applied]")) ;this is the name C* returns, define now because
                               ;it isn't really a valid keyword from reader's
                               ;perspective

(defrecord CasRegisterClient [tbl-created? session]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CasRegisterClient tbl-created? session)))

  (setup! [_ test]
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
                                            (with {:compaction {:class (compaction-strategy)}}))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (case (:f op)
      :cas (try (let [[old new] (:value op)
                      result (alia/execute session
                                           (update :lwt
                                                   (set-columns {:value new})
                                                   (where [[= :id 0]])
                                                   (only-if [[:value old]])))]
                  (if (-> result first ak)
                    (assoc op :type :ok)
                    (assoc op :type :fail :error (-> result first :value))))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch ReadTimeoutException e
                  (assoc op :type :info :error :read-timed-out))
                (catch WriteTimeoutException e
                  (assoc op :type :info :error :write-timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :write (try (let [v (:value op)
                        result (alia/execute session (update :lwt
                                                             (set-columns {:value v})
                                                             (only-if [[:in :value (range 5)]])
                                                             (where [[= :id 0]])))]
                    (if (-> result first ak)
                      (assoc op :type :ok)
                      (let [result' (alia/execute session (insert :lwt
                                                                  (values [[:id 0]
                                                                           [:value v]])
                                                                  (if-exists false)))]
                        (if (-> result' first ak)
                          (assoc op :type :ok)
                          (assoc op :type :fail)))))
                  (catch UnavailableException e
                    (assoc op :type :fail :error (.getMessage e)))
                  (catch ReadTimeoutException e
                    (assoc op :type :info :error :read-timed-out))
                  (catch WriteTimeoutException e
                    (assoc op :type :info :error :write-timed-out))
                  (catch NoHostAvailableException e
                    (info "All the servers are down - waiting 2s")
                    (Thread/sleep 2000)
                    (assoc op :type :fail :error (.getMessage e))))
      :read (try (let [v (->> (alia/execute session
                                            (select :lwt (where [[= :id 0]]))
                                            {:consistency :serial})
                              first
                              :value)]
                   (assoc op :type :ok :value v))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :error (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :error :timed-out))
                 (catch NoHostAvailableException e
                   (info "All the servers are down - waiting 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))

  (close! [_ _]
    (alia/shutdown session))

  (teardown! [_ _]))

(defn cas-register-client
  "A CAS register implemented using LWT"
  []
  (->CasRegisterClient (atom false) nil))

(defn cas-register-test
  [name opts]
  (merge (cassandra-test (str "lwt register " name)
                         {:client (cas-register-client)
                          :generator (gen/phases
                                      (->> [r w cas cas cas]
                                           gen/mix
                                           (gen/stagger 1/10)
                                           (gen/delay 1.5)
                                           (gen/nemesis
                                            (gen/seq (cycle
                                                      [(gen/sleep 5)
                                                       {:type :info :f :stop}
                                                       (gen/sleep 10)
                                                       {:type :info :f :start}])))
                                           (bootstrap 2)
                                           (gen/conductor
                                            :decommissioner
                                            (gen/seq (cycle
                                                      [(gen/sleep 4)
                                                       {:type :info :f :decommission}])))
                                           (gen/time-limit 30))
                                      (->> gen/void
                                           (gen/conductor
                                            :bootstrapper
                                            (gen/once {:type :info :f :bootstrapper}))
                                           gen/barrier))
                          :checker (checker/linearizable {:model     (model/cas-register)
                                                          :algorithm :linear})})
         opts))

(def bridge-test
  (cas-register-test "bridge"
                     {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}}))

(def halves-test
  (cas-register-test "halves"
                     {:conductors {:nemesis (nemesis/partition-random-halves)}}))

(def isolate-node-test
  (cas-register-test "isolate node"
                     {:conductors {:nemesis (nemesis/partition-random-node)}}))

(def crash-subset-test
  (cas-register-test "crash"
                     {:conductors {:nemesis (crash-nemesis)}}))

(def flush-compact-test
  (cas-register-test "flush and compact"
                     {:conductors {:nemesis (conductors/flush-and-compacter)}}))

(def clock-drift-test
  (cas-register-test "clock drift"
                     {:conductors {:nemesis (nemesis/clock-scrambler 10000)}}))

(def bridge-test-bootstrap
  (cas-register-test "bridge bootstrap"
                     {:bootstrap (atom #{:n4 :n5})
                      :conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                   :bootstrapper (conductors/bootstrapper)}}))

(def halves-test-bootstrap
  (cas-register-test "halves bootstrap"
                     {:bootstrap (atom #{:n4 :n5})
                      :conductors {:nemesis (nemesis/partition-random-halves)
                                   :bootstrapper (conductors/bootstrapper)}}))

(def isolate-node-test-bootstrap
  (cas-register-test "isolate node bootstrap"
                     {:bootstrap (atom #{:n4 :n5})
                      :conductors {:nemesis (nemesis/partition-random-node)
                                   :bootstrapper (conductors/bootstrapper)}}))

(def crash-subset-test-bootstrap
  (cas-register-test "crash bootstrap"
                     {:bootstrap (atom #{:n4 :n5})
                      :conductors {:nemesis (crash-nemesis)
                                   :bootstrapper (conductors/bootstrapper)}}))

(def clock-drift-test-bootstrap
  (cas-register-test "clock drift bootstrap"
                     {:bootstrap (atom #{:n4 :n5})
                      :conductors {:nemesis (nemesis/clock-scrambler 10000)
                                   :bootstrapper (conductors/bootstrapper)}}))

(def bridge-test-decommission
  (cas-register-test "bridge decommission"
                     {:conductors {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                                   :decommissioner (conductors/decommissioner)}}))

(def halves-test-decommission
  (cas-register-test "halves decommission"
                     {:conductors {:nemesis (nemesis/partition-random-halves)
                                   :decommissioner (conductors/decommissioner)}}))

(def isolate-node-test-decommission
  (cas-register-test "isolate node decommission"
                     {:conductors {:nemesis (nemesis/partition-random-node)
                                   :decommissioner (conductors/decommissioner)}}))

(def crash-subset-test-decommission
  (cas-register-test "crash decommission"
                     {:conductors {:nemesis (crash-nemesis)
                                   :decommissioner (conductors/decommissioner)}}))

(def clock-drift-test-decommission
  (cas-register-test "clock drift decommission"
                     {:conductors {:nemesis (nemesis/clock-scrambler 10000)
                                   :decommissioner (conductors/decommissioner)}}))

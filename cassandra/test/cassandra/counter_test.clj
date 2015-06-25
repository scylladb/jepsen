(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.counter :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Regular tests without cluster composition changes

(deftest cql-counter-inc-bridge
  (run-counter-test! bridge-inc-test timestamp))

(deftest cql-counter-inc-isolate-node
  (run-counter-test! isolate-node-inc-test timestamp))

(deftest cql-counter-inc-halves
  (run-counter-test! halves-inc-test timestamp))

(deftest cql-counter-inc-crash-subset
  (run-counter-test! crash-subset-inc-test timestamp))

(deftest cql-counter-inc-dec-bridge
  (run-counter-test! bridge-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-isolate-node
  (run-counter-test! isolate-node-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-halves
  (run-counter-test! halves-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-crash-subset
  (run-counter-test! crash-subset-inc-dec-test timestamp))

(deftest constant-cluster-tests
  (cql-counter-inc-bridge)
  (cql-counter-inc-isolate-node)
  (cql-counter-inc-halves)
  (cql-counter-inc-crash-subset)
  (cql-counter-inc-dec-bridge)
  (cql-counter-inc-dec-isolate-node)
  (cql-counter-inc-dec-halves)
  (cql-counter-inc-dec-crash-subset))

;; Tests to run by default with lein test for this namespace
(defn test-ns-hook
  []
  (constant-cluster-tests))

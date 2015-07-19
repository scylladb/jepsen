(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.counter :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Regular tests without cluster composition changes
(deftest ^:counter ^:steady cql-counter-inc-bridge
  (run-test! bridge-inc-test))

(deftest ^:counter ^:steady cql-counter-inc-isolate-node
  (run-test! isolate-node-inc-test))

(deftest ^:counter ^:steady cql-counter-inc-halves
  (run-test! halves-inc-test))

(deftest ^:counter ^:steady cql-counter-inc-crash-subset
  (run-test! crash-subset-inc-test))

(deftest ^:counter ^:steady cql-counter-inc-dec-bridge
  (run-test! bridge-inc-dec-test))

(deftest ^:counter ^:steady cql-counter-inc-dec-isolate-node
  (run-test! isolate-node-inc-dec-test))

(deftest ^:counter ^:steady cql-counter-inc-dec-halves
  (run-test! halves-inc-dec-test))

(deftest ^:counter ^:steady cql-counter-inc-dec-crash-subset
  (run-test! crash-subset-inc-dec-test))

;; Bootstrapping tests
(deftest ^:counter ^:bootstrap cql-counter-inc-bridge-bootstrap
  (run-test! bridge-inc-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-isolate-node-bootstrap
  (run-test! isolate-node-inc-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-halves-bootstrap
  (run-test! halves-inc-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-crash-subset-bootstrap
  (run-test! crash-subset-inc-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-bridge-bootstrap
  (run-test! bridge-inc-dec-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-isolate-node-bootstrap
  (run-test! isolate-node-inc-dec-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-halves-bootstrap
  (run-test! halves-inc-dec-test-bootstrap))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-crash-subset-bootstrap
  (run-test! crash-subset-inc-dec-test-bootstrap))

;; Decomission tests
(deftest ^:counter ^:decommission cql-counter-inc-bridge-decommission
  (run-test! bridge-inc-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-isolate-node-decommission
  (run-test! isolate-node-inc-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-halves-decommission
  (run-test! halves-inc-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-crash-subset-decommission
  (run-test! crash-subset-inc-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-dec-bridge-decommission
  (run-test! bridge-inc-dec-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-dec-isolate-node-decommission
  (run-test! isolate-node-inc-dec-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-dec-halves-decommission
  (run-test! halves-inc-dec-test-decommission))

(deftest ^:counter ^:decommission cql-counter-inc-dec-crash-subset-decommission
  (run-test! crash-subset-inc-dec-test-decommission))

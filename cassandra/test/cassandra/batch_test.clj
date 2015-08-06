(ns cassandra.batch-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.batch :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:batch ^:steady batch-bridge
  (run-test! bridge-test))

(deftest ^:batch ^:steady batch-isolate-node
  (run-test! isolate-node-test))

(deftest ^:batch ^:steady batch-halves
  (run-test! halves-test))

(deftest ^:batch ^:steady batch-crash-subset
  (run-test! crash-subset-test))

(deftest ^:clock batch-clock-drift
  (run-test! clock-drift-test))

;; Bootstrapping tests
(deftest ^:batch ^:bootstrap batch-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:batch ^:bootstrap batch-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:batch ^:bootstrap batch-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:batch ^:bootstrap batch-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:clock batch-clock-drift-bootstrap
  (run-test! clock-drift-test-bootstrap))

;; Decommission tests
(deftest ^:batch ^:decommission batch-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:batch ^:decommission batch-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:batch ^:decommission batch-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:batch ^:decommission batch-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

(deftest ^:clock batch-clock-drift-decommission
  (run-test! clock-drift-test-decommission))

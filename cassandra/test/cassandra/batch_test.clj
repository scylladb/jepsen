(ns cassandra.batch-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.batch :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:batch-set ^:steady batch-set-bridge
  (run-test! bridge-test))

(deftest ^:batch-set ^:steady batch-set-isolate-node
  (run-test! isolate-node-test))

(deftest ^:batch-set ^:steady batch-set-halves
  (run-test! halves-test))

(deftest ^:batch-set ^:steady batch-set-crash-subset
  (run-test! crash-subset-test))

(deftest ^:clock batch-set-clock-drift
  (run-test! clock-drift-test))

;; Bootstrapping tests
(deftest ^:batch-set ^:bootstrap batch-set-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:batch-set ^:bootstrap batch-set-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:batch-set ^:bootstrap batch-set-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:batch-set ^:bootstrap batch-set-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:clock batch-set-clock-drift-bootstrap
  (run-test! clock-drift-test-bootstrap))

;; Decommission tests
(deftest ^:batch-set ^:decommission batch-set-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:batch-set ^:decommission batch-set-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:batch-set ^:decommission batch-set-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:batch-set ^:decommission batch-set-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

(deftest ^:clock batch-set-clock-drift-decommission
  (run-test! clock-drift-test-decommission))

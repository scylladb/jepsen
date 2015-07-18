(ns cassandra.batch-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.batch :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:batch-set ^:steady batch-set-bridge
  (run-set-test! bridge-test timestamp))

(deftest ^:batch-set ^:steady batch-set-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest ^:batch-set ^:steady batch-set-halves
  (run-set-test! halves-test timestamp))

(deftest ^:batch-set ^:steady batch-set-crash-subset
  (run-set-test! crash-subset-test timestamp))

(deftest ^:clock batch-set-clock-drift
  (run-set-test! clock-drift-test timestamp))

;; Bootstrapping tests
(deftest ^:batch-set ^:bootstrap batch-set-bridge-bootstrap
  (run-set-test! bridge-test-bootstrap timestamp))

(deftest ^:batch-set ^:bootstrap batch-set-isolate-node-bootstrap
  (run-set-test! isolate-node-test-bootstrap timestamp))

(deftest ^:batch-set ^:bootstrap batch-set-halves-bootstrap
  (run-set-test! halves-test-bootstrap timestamp))

(deftest ^:batch-set ^:bootstrap batch-set-crash-subset-bootstrap
  (run-set-test! crash-subset-test-bootstrap timestamp))

(deftest ^:clock batch-set-clock-drift-bootstrap
  (run-set-test! clock-drift-test-bootstrap timestamp))

;; Decommission tests
(deftest ^:batch-set ^:decommission batch-set-bridge-decommission
  (run-set-test! bridge-test-decommission timestamp))

(deftest ^:batch-set ^:decommission batch-set-isolate-node-decommission
  (run-set-test! isolate-node-test-decommission timestamp))

(deftest ^:batch-set ^:decommission batch-set-halves-decommission
  (run-set-test! halves-test-decommission timestamp))

(deftest ^:batch-set ^:decommission batch-set-crash-subset-decommission
  (run-set-test! crash-subset-test-decommission timestamp))

(deftest ^:clock batch-set-clock-drift-decommission
  (run-set-test! clock-drift-test-decommission timestamp))

(ns scylla.collections.map-test
  (:require [clojure.test :refer :all]
            [scylla.collections.map :refer :all]
            [scylla.core-test :refer :all]))

(deftest ^:map ^:steady cql-map-bridge
  (run-test! bridge-test))

(deftest ^:map ^:steady cql-map-isolate-node
  (run-test! isolate-node-test))

(deftest ^:map ^:steady cql-map-halves
  (run-test! halves-test))

(deftest ^:map ^:steady cql-map-crash-subset
  (run-test! crash-subset-test))

(deftest ^:map ^:steady cql-map-flush-compact
  (run-test! flush-compact-test))

(deftest ^:map ^:bootstrap cql-map-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:map ^:bootstrap cql-map-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:map ^:bootstrap cql-map-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:map ^:bootstrap cql-map-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:map ^:decommission cql-map-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:map ^:decommission cql-map-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:map ^:decommission cql-map-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:map ^:decommission cql-map-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

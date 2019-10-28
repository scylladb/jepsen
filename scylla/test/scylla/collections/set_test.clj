(ns scylla.collections.set-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [scylla.collections.set :refer :all]
            [scylla.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest ^:set ^:steady cql-set-bridge
  (run-test! bridge-test))

(deftest ^:set ^:steady cql-set-isolate-node
  (run-test! isolate-node-test))

(deftest ^:set ^:steady cql-set-halves
  (run-test! halves-test))

(deftest ^:set ^:steady cql-set-crash-subset
  (run-test! crash-subset-test))

(deftest ^:set ^:steady cql-set-flush-compact
  (run-test! flush-compact-test))

(deftest ^:set ^:bootstrap cql-set-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:set ^:bootstrap cql-set-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:set ^:bootstrap cql-set-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:set ^:bootstrap cql-set-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:set ^:decommission cql-set-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:set ^:decommission cql-set-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:set ^:decommission cql-set-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:set ^:decommission cql-set-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

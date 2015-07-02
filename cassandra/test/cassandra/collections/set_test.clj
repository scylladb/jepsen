(ns cassandra.collections.set-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.set :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest ^:set ^:steady cql-set-bridge
  (run-set-test! bridge-test timestamp))

(deftest ^:set ^:steady cql-set-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest ^:set ^:steady cql-set-halves
  (run-set-test! halves-test timestamp))

(deftest ^:set ^:steady cql-set-crash-subset
  (run-set-test! crash-subset-test timestamp))

(deftest ^:set ^:bootstrap cql-set-bridge-bootstrap
  (run-set-test! bridge-test-bootstrap timestamp))

(deftest ^:set ^:bootstrap cql-set-isolate-node-bootstrap
  (run-set-test! isolate-node-test-bootstrap timestamp))

(deftest ^:set ^:bootstrap cql-set-halves-bootstrap
  (run-set-test! halves-test-bootstrap timestamp))

(deftest ^:set ^:bootstrap cql-set-crash-subset-bootstrap
  (run-set-test! crash-subset-test-bootstrap timestamp))

(deftest ^:set ^:decommission cql-set-bridge-decommission
  (run-set-test! bridge-test-decommission timestamp))

(deftest ^:set ^:decommission cql-set-isolate-node-decommission
  (run-set-test! isolate-node-test-decommission timestamp))

(deftest ^:set ^:decommission cql-set-halves-decommission
  (run-set-test! halves-test-decommission timestamp))

(deftest ^:set ^:decommission cql-set-crash-subset-decommission
  (run-set-test! crash-subset-test-decommission timestamp))

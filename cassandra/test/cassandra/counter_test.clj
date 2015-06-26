(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.counter :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Regular tests without cluster composition changes
(deftest ^:counter ^:steady cql-counter-inc-bridge
  (run-counter-test! bridge-inc-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-isolate-node
  (run-counter-test! isolate-node-inc-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-halves
  (run-counter-test! halves-inc-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-crash-subset
  (run-counter-test! crash-subset-inc-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-dec-bridge
  (run-counter-test! bridge-inc-dec-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-dec-isolate-node
  (run-counter-test! isolate-node-inc-dec-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-dec-halves
  (run-counter-test! halves-inc-dec-test timestamp))

(deftest ^:counter ^:steady cql-counter-inc-dec-crash-subset
  (run-counter-test! crash-subset-inc-dec-test timestamp))

;; Bootstrapping tests
(deftest ^:counter ^:bootstrap cql-counter-inc-bridge-bootstrap
  (run-counter-test! bridge-inc-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-isolate-node-bootstrap
  (run-counter-test! isolate-node-inc-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-halves-bootstrap
  (run-counter-test! halves-inc-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-crash-subset-bootstrap
  (run-counter-test! crash-subset-inc-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-bridge-bootstrap
  (run-counter-test! bridge-inc-dec-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-isolate-node-bootstrap
  (run-counter-test! isolate-node-inc-dec-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-halves-bootstrap
  (run-counter-test! halves-inc-dec-test-bootstrap timestamp))

(deftest ^:counter ^:bootstrap cql-counter-inc-dec-crash-subset-bootstrap
  (run-counter-test! crash-subset-inc-dec-test-bootstrap timestamp))

(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.counter :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest cql-counter-inc-test-bridge
  (run-counter-test! bridge-inc-test timestamp))

(deftest cql-counter-inc-test-isolate-node
  (run-counter-test! isolate-node-inc-test timestamp))

(deftest cql-counter-inc-test-halves
  (run-counter-test! halves-inc-test timestamp))

(deftest cql-counter-inc-test-crash-subset
  (run-counter-test! crash-subset-inc-test timestamp))

(deftest cql-counter-inc-dec-test-bridge
  (run-counter-test! bridge-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-test-isolate-node
  (run-counter-test! isolate-node-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-test-halves
  (run-counter-test! halves-inc-dec-test timestamp))

(deftest cql-counter-inc-dec-test-crash-subset
  (run-counter-test! crash-subset-inc-dec-test timestamp))

(ns cassandra.collections.set-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.set :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest cql-set-bridge
  (run-set-test! bridge-test timestamp))

(deftest cql-set-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest cql-set-halves
  (run-set-test! halves-test timestamp))

(deftest cql-set-crash-subset
  (run-set-test! crash-subset-test timestamp))

(deftest constant-cluster-tests
  (cql-set-bridge)
  (cql-set-isolate-node)
  (cql-set-halves)
  (cql-set-crash-subset))

;; Tests to run by default with lein test for this namespace
(defn test-ns-hook
  []
  (constant-cluster-tests))

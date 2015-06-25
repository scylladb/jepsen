(ns cassandra.collections.set-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.set :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(comment (deftest cql-set-no-nemesis
           (run-set-test! (cql-set-test "no nemesis" {}) timestamp)))

(deftest cql-set-bridge
  (run-set-test! bridge-test timestamp))

(deftest cql-set-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest cql-set-halves
  (run-set-test! halves-test timestamp))

(deftest cql-set-crash-subset
  (run-set-test! crash-subset-test timestamp))

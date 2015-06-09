(ns cassandra.collections.map-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.map :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest cql-map-test-bridge
  (run-set-test! bridge-test))

(deftest cql-map-test-isolate-node
  (run-set-test! isolate-node-test))

(deftest cql-map-test-halves
  (run-set-test! halves-test))

(deftest cql-map-test-crash-subset
  (run-set-test! crash-subset-test))

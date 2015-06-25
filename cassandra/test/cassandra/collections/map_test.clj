(ns cassandra.collections.map-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.map :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest cql-map-bridge
  (run-set-test! bridge-test timestamp))

(deftest cql-map-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest cql-map-halves
  (run-set-test! halves-test timestamp))

(deftest cql-map-crash-subset
  (run-set-test! crash-subset-test timestamp))

(ns cassandra.collections.map-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.map :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest ^:map ^:steady cql-map-bridge
  (run-set-test! bridge-test timestamp))

(deftest ^:map ^:steady cql-map-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest ^:map ^:steady cql-map-halves
  (run-set-test! halves-test timestamp))

(deftest ^:map ^:steady cql-map-crash-subset
  (run-set-test! crash-subset-test timestamp))

(deftest ^:map ^:bootstrap cql-map-bridge-bootstrap
  (run-set-test! bridge-test-bootstrap timestamp))

(deftest ^:map ^:bootstrap cql-map-isolate-node-bootstrap
  (run-set-test! isolate-node-test-bootstrap timestamp))

(deftest ^:map ^:bootstrap cql-map-halves-bootstrap
  (run-set-test! halves-test-bootstrap timestamp))

(deftest ^:map ^:bootstrap cql-map-crash-subset-bootstrap
  (run-set-test! crash-subset-test-bootstrap timestamp))

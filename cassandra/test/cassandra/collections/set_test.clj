(ns cassandra.collections.set-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.collections.set :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;(deftest cql-set-test-no-nemesis
;  (run-set-test! (cql-set-test "no nemesis" {})))

(deftest cql-set-test-bridge
  (run-set-test! bridge-test))

(deftest cql-set-test-isolate-node
  (run-set-test! isolate-node-test))

(deftest cql-set-test-halves
  (run-set-test! halves-test))

(deftest cql-set-test-crash-subset
  (run-set-test! crash-subset-test))

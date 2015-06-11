(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.counter :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest cql-counter-test-bridge
  (run-counter-test! bridge-test))

(deftest cql-counter-test-isolate-node
  (run-counter-test! isolate-node-test))

(deftest cql-counter-test-halves
  (run-counter-test! halves-test))

(deftest cql-counter-test-crash-subset
  (run-counter-test! crash-subset-test))

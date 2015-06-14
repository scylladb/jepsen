(ns cassandra.lwt-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.lwt :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(deftest lwt-test-bridge
  (run-cas-register-test! bridge-test))

(deftest lwt-test-isolate-node
  (run-cas-register-test! isolate-node-test))

(deftest lwt-test-halves
  (run-cas-register-test! halves-test))

(deftest lwt-test-crash-subset
  (run-cas-register-test! crash-subset-test))

(ns cassandra.mv-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.mv :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:mv ^:steady mv-bridge
  (run-test! bridge-test))

(deftest ^:mv ^:steady mv-isolate-node
  (run-test! isolate-node-test))

(deftest ^:mv ^:steady mv-halves
  (run-test! halves-test))

(deftest ^:mv ^:steady mv-crash-subset
  (run-test! crash-subset-test))

(deftest ^:mv ^:steady mv-flush-compact
  (run-test! flush-compact-test))

(deftest ^:clock mv-clock-drift
  (run-test! clock-drift-test))

;; Bootstrapping tests
(deftest ^:mv ^:bootstrap mv-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:mv ^:bootstrap mv-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:mv ^:bootstrap mv-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:mv ^:bootstrap mv-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:clock mv-clock-drift-bootstrap
  (run-test! clock-drift-test-bootstrap))

;; Decommission tests
(deftest ^:mv ^:decommission mv-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:mv ^:decommission mv-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:mv ^:decommission mv-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:mv ^:decommission mv-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

(deftest ^:clock mv-clock-drift-decommission
  (run-test! clock-drift-test-decommission))

;; Consistency delay test
(deftest consistency-delay
  (run-test! delay-test))

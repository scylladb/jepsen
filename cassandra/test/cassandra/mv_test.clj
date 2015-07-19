(ns cassandra.mv-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.mv :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:mv-set ^:steady mv-set-bridge
  (run-test! bridge-test))

(deftest ^:mv-set ^:steady mv-set-isolate-node
  (run-test! isolate-node-test))

(deftest ^:mv-set ^:steady mv-set-halves
  (run-test! halves-test))

(deftest ^:mv-set ^:steady mv-set-crash-subset
  (run-test! crash-subset-test))

(deftest ^:clock mv-set-clock-drift
  (run-test! clock-drift-test))

;; Bootstrapping tests
(deftest ^:mv-set ^:bootstrap mv-set-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:mv-set ^:bootstrap mv-set-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:mv-set ^:bootstrap mv-set-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:mv-set ^:bootstrap mv-set-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

(deftest ^:clock mv-set-clock-drift-bootstrap
  (run-test! clock-drift-test-bootstrap))

;; Decommission tests
(deftest ^:mv-set ^:decommission mv-set-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:mv-set ^:decommission mv-set-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:mv-set ^:decommission mv-set-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:mv-set ^:decommission mv-set-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

(deftest ^:clock mv-set-clock-drift-decommission
  (run-test! clock-drift-test-decommission))

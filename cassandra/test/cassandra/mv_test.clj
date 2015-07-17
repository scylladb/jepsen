(ns cassandra.mv-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.mv :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

;; Steady state cluster tests
(deftest ^:mv-set ^:steady mv-set-bridge
  (run-set-test! bridge-test timestamp))

(deftest ^:mv-set ^:steady mv-set-isolate-node
  (run-set-test! isolate-node-test timestamp))

(deftest ^:mv-set ^:steady mv-set-halves
  (run-set-test! halves-test timestamp))

(deftest ^:mv-set ^:steady mv-set-crash-subset
  (run-set-test! crash-subset-test timestamp))

(deftest ^:clock mv-set-clock-drift
  (run-set-test! clock-drift-test timestamp))

;; Bootstrapping tests
(deftest ^:mv-set ^:bootstrap mv-set-bridge-bootstrap
  (run-set-test! bridge-test-bootstrap timestamp))

(deftest ^:mv-set ^:bootstrap mv-set-isolate-node-bootstrap
  (run-set-test! isolate-node-test-bootstrap timestamp))

(deftest ^:mv-set ^:bootstrap mv-set-halves-bootstrap
  (run-set-test! halves-test-bootstrap timestamp))

(deftest ^:mv-set ^:bootstrap mv-set-crash-subset-bootstrap
  (run-set-test! crash-subset-test-bootstrap timestamp))

(deftest ^:clock mv-set-clock-drift-bootstrap
  (run-set-test! clock-drift-test-bootstrap timestamp))

;; Decommission tests
(deftest ^:mv-set ^:decommission mv-set-bridge-decommission
  (run-set-test! bridge-test-decommission timestamp))

(deftest ^:mv-set ^:decommission mv-set-isolate-node-decommission
  (run-set-test! isolate-node-test-decommission timestamp))

(deftest ^:mv-set ^:decommission mv-set-halves-decommission
  (run-set-test! halves-test-decommission timestamp))

(deftest ^:mv-set ^:decommission mv-set-crash-subset-decommission
  (run-set-test! crash-subset-test-decommission timestamp))

(deftest ^:clock mv-set-clock-drift-decommission
  (run-set-test! clock-drift-test-decommission timestamp))

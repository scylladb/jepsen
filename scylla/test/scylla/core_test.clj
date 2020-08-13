(ns scylla.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [scylla.core :refer :all]
            [jepsen [core :as jepsen]]))

(defn run-test!
  [test]
  (flush) ; Make sure nothing buffered
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))))

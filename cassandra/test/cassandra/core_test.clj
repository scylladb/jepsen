(ns cassandra.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.core :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(defn run-set-test!
  "Runs a set test"
  [test]
  (let [test (jepsen/run! test)]
    (or (is (:valid? (:results test)))
        (println (:error (:results test))))
    (report/to (str "report/" (:name test) "/history.edn")
               (pprint (:history test)))
    (report/to (str "report/" (:name test) "/set.edn")
               (pprint (:set (:results test))))))

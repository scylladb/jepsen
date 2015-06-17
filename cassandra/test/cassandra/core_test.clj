(ns cassandra.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.core :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

(defn date-tag
  "Date tagging for the report directory"
  []
  (->> (java.util.Date.)
       (.format (java.text.SimpleDateFormat. "yyyy-MM-dd-HH-mm-ss"))))

; Timestamp for this session of testing - either lein test or repl
(def timestamp (date-tag))

(defn run-set-test!
  "Runs a set test"
  [test ts]
  (let [test (jepsen/run! test)]
    (or (is (:valid? (:results test)))
        (println (:error (:results test))))
    (report/to (str "report-" ts "/" (:name test) "/history.edn")
               (pprint (:history test)))
    (report/to (str "report-" ts "/" (:name test) "/set.edn")
               (pprint (:set (:results test))))))

(defn run-counter-test!
  "Runs a counter test"
  [test ts]
  (let [test (jepsen/run! test)]
    (or (is (:valid? (:results test)))
        (println (:error (:results test))))
    (report/to (str "report-" ts "/" (:name test) "/history.edn")
               (pprint (:history test)))
    (report/to (str "report-" ts "/" (:name test) "/counter.edn")
               (pprint (:counter (:results test))))))

(defn run-cas-register-test!
  "Runs a cas register test"
  [test ts]
  (let [test (jepsen/run! test)]
    (or (is (:valid? (:results test)))
        (println (:error (:results test))))
    (report/to (str "report-" ts "/" (:name test) "/history.edn")
               (pprint (:history test)))
    (report/to (str "report-" ts "/" (:name test) "/linearizability.txt")
               (-> test :results :linear report/linearizability))))

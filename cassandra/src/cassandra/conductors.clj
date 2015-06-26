(ns cassandra.conductors
  (:require [clojure.tools.logging :refer :all]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [cassandra.core :as cassandra]))

(defn bootstrapper
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [bootstrap (:bootstrap test)]
        (if-let [node (first @bootstrap)]
          (do (info node "bootstrapping")
              (swap! bootstrap rest)
              (c/on node (cassandra/start! node test))
              (assoc op :value (str node " bootstrapped")))
          (assoc op :value "no nodes left to bootstrap"))))
    (teardown! [this test] this)))

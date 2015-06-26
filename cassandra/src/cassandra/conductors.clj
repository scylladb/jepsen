(ns cassandra.conductors
  (:require [clojure.tools.logging :refer :all]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [cassandra.core :as cassandra]))

(defn bootstrapper
  []
  (let [bootstrap (atom [])]
    (reify client/Client
      (setup! [this test node]
        (reset! bootstrap (:bootstrap test))
        this)
      (invoke! [this test op]
        (if-let [node (first @bootstrap)]
          (do (info node "bootstrapping")
              (c/on node (cassandra/start! node test))
              (swap! bootstrap rest)
              (assoc op :value (str node " bootstrapped")))
          (assoc op :value "no nodes left to bootstrap")))
      (teardown! [this test] this))))

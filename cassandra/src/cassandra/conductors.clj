(ns cassandra.conductors
  (:require [cassandra.core :as cassandra]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
             [control :as c]
             [util :as util :refer [meh]]]))

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

(defn decommissioner
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [decommission (:decommission test)]
        (if-let [node (some-> test cassandra/live-nodes (set/difference @decommission)
                              shuffle (get 3))] ; keep at least RF nodes
          (do (info node "decommissioning")
              (info @decommission "already decommissioned")
              (swap! decommission conj node)
              (cassandra/nodetool node "decommission")
              (assoc op :value (str node " decommissioned")))
          (assoc op :value "no nodes eligible for decommission"))))
    (teardown! [this test] this)))

(defn replayer
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [live-nodes (cassandra/live-nodes test)]
        (doseq [node live-nodes]
          (cassandra/nodetool node "replaybatchlog"))
        (assoc op :value (str live-nodes " batch logs replayed"))))
    (teardown! [this test] this)))

(defn flush-and-compacter
  "Flushes to sstables and forces a major compaction on all nodes"
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do (doseq [node (:nodes test)]
                     (cassandra/nodetool node "flush")
                     (cassandra/nodetool node "compact"))
                   (assoc op :value (str (:nodes test) " nodes flushed and compacted")))
        :stop (assoc op :value "stop is a no-op with this nemesis")))
    (teardown! [this test] this)))

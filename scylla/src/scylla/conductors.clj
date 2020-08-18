(ns scylla.conductors
  "A holding tank for some code which I think is meant to be integrated into
  nemeses. None of this actually *worked* with Scylla, but it might be useful
  to look at and adapt into scylla.nemesis later. TODO: drop this namespace,
  once that's done"
  (:require [scylla [db :as db]]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen
             [client :as client]
             [control :as c]]))

(defn bootstrapper
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [bootstrap (:bootstrap test)]
        (if-let [node (first @bootstrap)]
          (do (info node "starting bootstrapping")
              (swap! bootstrap rest)
              (c/on node (db/start! node test))
              (while (some #{db/dns-resolve (name node)}
                           (db/joining-nodes test))
                (info node "still joining")
                (Thread/sleep 1000))
              (assoc op :value (str node " bootstrapped")))
          (assoc op :value "no nodes left to bootstrap"))))
    (teardown! [this test] this)))

(defn decommissioner
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [decommission (:decommission test)]
        (if-let [node (some-> test db/live-nodes (set/difference @decommission)
                              shuffle (get 3))] ; keep at least RF nodes
          (do (info node "decommissioning")
              (info @decommission "already decommissioned")
              (swap! decommission conj node)
              (db/nodetool node "decommission")
              (assoc op :value (str node " decommissioned")))
          (assoc op :value "no nodes eligible for decommission"))))
    (teardown! [this test] this)))

(defn replayer
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [live-nodes (db/live-nodes test)]
        (doseq [_ live-nodes]
	  ; we do not support it yet
          ;(scylla/nodetool node "replaybatchlog")
	  )
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
                     (db/nodetool node "flush")
                     (db/nodetool node "compact"))
                   (assoc op :value (str (:nodes test) " nodes flushed and compacted")))
        :stop (assoc op :value "stop is a no-op with this nemesis")))
    (teardown! [this test] this)))

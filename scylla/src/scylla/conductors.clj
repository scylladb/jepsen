(ns scylla.conductors
  (:require [scylla.core :as scylla]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [util :as util :refer [meh]]]))

(defn bootstrapper
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [bootstrap (:bootstrap test)]
        (if-let [node (first @bootstrap)]
          (do (info node "starting bootstrapping")
              (swap! bootstrap rest)
              (c/on node (scylla/start! node test))
              (while (some #{scylla/dns-resolve (name node)}
                           (scylla/joining-nodes test))
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
        (if-let [node (some-> test scylla/live-nodes (set/difference @decommission)
                              shuffle (get 3))] ; keep at least RF nodes
          (do (info node "decommissioning")
              (info @decommission "already decommissioned")
              (swap! decommission conj node)
              (scylla/nodetool node "decommission")
              (assoc op :value (str node " decommissioned")))
          (assoc op :value "no nodes eligible for decommission"))))
    (teardown! [this test] this)))

(defn replayer
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [live-nodes (scylla/live-nodes test)]
        (doseq [node live-nodes]
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
                     (scylla/nodetool node "flush")
                     (scylla/nodetool node "compact"))
                   (assoc op :value (str (:nodes test) " nodes flushed and compacted")))
        :stop (assoc op :value "stop is a no-op with this nemesis")))
    (teardown! [this test] this)))
;
;(defn flexible-partitioner
;  "Responds to a :start operation by cutting network links as defined by
;  (grudge nodes), and responds to :stop by healing the network. Uses
;  :grudge entry in op to determine grudge function used."
;  [grudge-map]
;  (reify client/Client
;    (setup! [this test _]
;      (net/heal! (:net test) test)
;      this)
;
;    (invoke! [this test op]
;      (case (:f op)
;        :start (let [grudge ((-> op :grudge grudge-map) (:nodes test))]
;                 (nemesis/partition! test grudge)
;                 (assoc op :value (str "Cut off " (pr-str grudge))))
;        :stop  (do (net/heal! (:net test) test)
;                   (assoc op :value "fully connected"))))
;
;    (teardown! [this test]
;      (net/heal! (:net test) test))))

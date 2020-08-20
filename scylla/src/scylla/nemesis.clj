(ns scylla.nemesis
  "All kinds of failure modes for Scylla!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [scylla [client :as client]
                    [db :as sdb]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [util :as util]]
            [jepsen.nemesis [time :as nt]
                            [combined :as nc]]))

(defn ordered-soonest-op-map
  "Takes a pair of maps wrapping operations. Each map has the following
  structure:

    :op       An operation
    :weight   An optional integer weighting.

  Returns whichever map has an operation which occurs sooner. If one map is
  nil, the other happens sooner. If one map's op is :pending, the other happens
  sooner. If one op has a lower :time, it happens sooner. If the two ops have
  equal :times, prefers the first op."
  [m1 m2]
  (condp = nil
    m1 m2
    m2 m1
    (let [op1 (:op m1)
          op2 (:op m2)]
      (condp = :pending
        op1 m2
        op2 m1
        (let [t1 (:time op1)
              t2 (:time op2)]
          (if (< t2 t1)
            m2
            m1))))))

(defrecord OrderedAny [gens]
  gen/Generator
  (op [this test ctx]
    (when-let [{:keys [op gen' i]}
               (->> gens
                    (map-indexed
                      (fn [i gen]
                        (when-let [[op gen'] (gen/op gen test ctx)]
                          {:op    op
                           :gen'  gen'
                           :i     i})))
                    (reduce ordered-soonest-op-map nil))]
      [op (OrderedAny. (assoc gens i gen'))]))

  (update [this test ctx event]
    (OrderedAny. (mapv (fn updater [gen]
                         (gen/update gen test ctx event))
                       gens))))

(defn ordered-any
  "Takes multiple generators and binds them together. Operations are taken from
  any generator, preferring earlier over later. Updates are propagated to all
  generators."
  [& gens]
  (condp = (count gens)
    0 nil
    1 (first gens)
      (OrderedAny. (vec gens))))

(defn after-time
  "Adjusts all operations from gen so that they execute no sooner than time t."
  [gen t]
  (gen/map (fn [op] (update op :time max t)) gen))

(defn after-times
  "All ops from gen at dt seconds, then 2dt seconds, then 3dt seconds, etc."
  [dt gen]
  (->> (iterate (partial + dt) dt)
       (map util/secs->nanos)
       (map (partial after-time gen))))

(defn periodically-recover
  "Takes a package and modifies its generator to periodically evaluate
  final-generator."
  [pkg]
  (let [g  (:generator pkg)
        fg (:final-generator pkg)]
    (assoc pkg :generator
           (ordered-any
             [(after-times 60 [(gen/log "Recovering...")
                               fg
                               (gen/sleep 66666600)
                               (gen/log "Recovery done, back to mischief")])]
             g))))

(defn package
  "Constructs a {:nemesis, :generator, :final-generator} map for the test.
  Options:

      :interval How long to wait between faults
      :db       The database we're going to manipulate.
      :faults   A set of faults, e.g. #{:kill, :pause, :partition}
      :targets  A map of options for each type of fault, e.g.
                {:partition {:targets [:majorities-ring ...]}}"
  [opts]
  (-> (nc/nemesis-package opts)
      periodically-recover))

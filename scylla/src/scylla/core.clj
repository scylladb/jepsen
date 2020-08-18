(ns scylla.core
  "Combines dbs, nemeses, and workloads to build Jepsen tests. Also includes a
  CLI runner."
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.java.jmx :as jmx]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [cli       :as cli]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [generator :as gen]
             [tests     :as tests]]
            [jepsen.control [net :as net]]
            [jepsen.os.debian :as debian]
            [scylla [batch :as batch]
                    [client     :as sc]
                    [counter    :as counter]
                    [db         :as db]
                    [generator  :as sgen]
                    [mv         :as mv]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core Session)
           (com.datastax.driver.core Cluster)
           (com.datastax.driver.core Metadata)
           (com.datastax.driver.core Host)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:batch-set       batch/set-workload
   :counter         counter/workload
   :counter-inc-dec counter/inc-dec-workload
   :mv              mv/workload})

(def standard-workloads
  "The workload names we run for test-all by default."
  (keys workloads))

(def nemeses
  "Types of faults a nemesis can create."
   #{:pause :kill :partition :clock})

(def standard-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause]
   [:kill]
   [:partition]
   [:pause :kill :partition :clock]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none      []
   :standard  [:pause :kill :partition :clock]
   :all       [:pause :kill :partition :clock]})


(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn scaled
  "Applies a scaling factor to a number - used for durations
  throughout testing to easily scale the run time of the whole
  test suite. Accepts doubles."
  [v]
  (let [factor (or (some-> (System/getenv "JEPSEN_SCALE") (Double/parseDouble))
                   1)]
    (Math/ceil (* v factor))))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
   (gen/phases
    (gen/once {:type :info, :f :stop})
    (gen/sleep 10))))

(defn bootstrap
  "A generator that bootstraps nodes into the cluster with the given pause
  and routes other :op's onward."
  [pause src-gen]
  (sgen/conductor :bootstrapper
                  (gen/seq (cycle [(gen/sleep pause)
                                   {:type :info :f :bootstrap}]))
                  src-gen))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  ([gen] (std-gen 400 gen))
  ([duration gen]
   (gen/phases
    (->> gen
         (gen/nemesis
          (gen/seq (cycle [(gen/sleep (scaled 20))
                           {:type :info :f :start}
                           (gen/sleep (scaled 60))
                           {:type :info :f :stop}])))
         (bootstrap 120)
         (sgen/conductor :decommissioner
                        (gen/seq (cycle [(gen/sleep (scaled 100))
                                         {:type :info :f :decommission}])))
         (gen/time-limit (scaled duration)))
    (recover)
    (gen/clients
     (->> gen
          (gen/time-limit (scaled 40)))))))

; TODO: pull these generators out into workloads
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn assocs
  "Generator that emits :assoc operations for sequential integers,
  mapping x to (f x)"
  [f]
  (->> (range)
       (map (fn [x] {:type :invoke :f :assoc :value {:k x
                                                     :v (f x)}}))
       gen/seq))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
   (gen/once {:type :invoke, :f :read})))

(defn safe-mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.

  TODO: Also does DNS lookups? This is... not at all what it claims to be!
  --aphyr

      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs test]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))
      set
      (set/difference @(:bootstrap test))
      (#(map (comp db/dns-resolve name) %))
      set
      (set/difference @(:decommission test))
      shuffle))

(defn test-aware-node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify client/Client
      (setup! [this test _] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (if-let [ns (-> test :nodes (targeter test) util/coll)]
                            (if (compare-and-set! nodes nil ns)
                              (c/on-many ns (start! test (keyword c/*host*)))
                              (str "nemesis already disrupting " @nodes))
                            :no-target)
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test (keyword c/*host*)))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))

; TODO: move this to scylla.nemesis
(defn crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (test-aware-node-start-stopper
   safe-mostly-small-nonempty-subset
   (fn start [_ node]
     (meh (c/su (c/exec :killall :-9 :scylla-jmx :scylla))) [:killed node])
   (fn stop  [test node]
     (meh (db/guarded-start! node test)) [:restarted node])))

; TODO: some tests intersperse
; (sgen/conductor :replayer (gen/once {:type :info :f :replay}))
; with their generators. As far as I can tell, this code doesn't actually
; *work*, but we should figure out what replayer does and maybe make a nemesis
; for it.

(defn scylla-test
  "Takes test options from the CLI, all-tests, etc, and constructs a Jepsen
  test map."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        nemesis  nil
        generator (->> (:generator workload)
                       (gen/stagger (/ (:rate opts)))
                       (gen/nemesis (:generator nemesis))
                       (gen/time-limit (:time-limit opts)))
        generator (if-let [fg (:final-generator workload)]
                    (gen/phases generator
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for cluster to recover")
                                (gen/sleep 20)
                                (gen/clients fg))
                    generator)]
    (merge tests/noop-test
           opts
           (dissoc workload :generator :final-generator) ; These we handle
           {:name         (str "scylla " (name (:workload opts)))
            :os           debian/os
            :db           (db/db "4.2")
            :logging      {:overrides
                           {"com.datastax.driver.core.Connection"   :error
                            "com.datastax.driver.core.ClockFactory" :error
                            "com.datastax.driver.core.Session"      :error
                            ;"com.datastax.driver.core.ControlConnection" :off
                            }}
            :bootstrap    (atom #{}) ; TODO: remove me
            :decommission (atom #{}) ; TODO: remove me
            :nonserializable-keys [:conductors] ; TODO: remove me
            ; TODO: recovery and :final-generator, if applicable
            :generator    generator})))

(def cli-opts
  "Options for test runners."
  [[nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str "Faults must be one of " nemeses " or "
                    (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  3
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   ["-r" "--rate HZ" "Approximate number of requests per second per thread"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   ["-v" "--version VERSION" "What version of Scylla should we test?"
    :default "4.2"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of test options, by
  combining all workloads and nemeses."
  [opts]
  (let [nemeses     (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads   (if-let [w (:workload opts)] [w] standard-workloads)
        counts      (range (:test-count opts))]
    (->> (for [i counts, n nemeses, w workloads]
           (assoc opts :nemesis n :workload w))
         (map scylla-test))))

(defn -main
  "Handles CLI args."
  [& args]
  (cli/run! (merge (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/single-test-cmd {:test-fn  scylla-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

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
             [checker   :as checker]
             [cli       :as cli]
             [util      :as util :refer [meh timeout parse-long]]
             [control   :as c :refer [| lit]]
             [generator :as gen]
             [tests     :as tests]]
            [jepsen.control [net :as net]]
            [jepsen.os.debian :as debian]
            [scylla [batch          :as batch]
                    [batch-return   :as batch-return]
                    [cas-register   :as cas-register]
                    [client         :as sc]
                    [counter        :as counter]
                    [db             :as db]
                    [list-append    :as list-append]
                    [mv             :as mv]
                    [nemesis        :as nemesis]
                    [wr-register    :as wr-register]]
            [scylla.collections [map :as cmap]
                                [set :as cset]]
            [qbits.commons.enum])
  (:import (com.datastax.driver.core ConsistencyLevel)))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:batch-set       batch/set-workload
   :batch-return    batch-return/workload
   :cas-register    cas-register/workload
   :counter         counter/workload
   :cmap            cmap/workload
   :list-append     list-append/workload
   :mv              mv/workload
   :cset            cset/workload
   :wr-register     wr-register/workload})

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
        db       (db/db (:version opts))
        nemesis  (nemesis/package
                   {:db         db
                    :faults     (set (:nemesis opts))
                    :partition  {:targets [:majority]}
                    :interval  (:nemesis-interval opts)})
        generator (->> (:generator workload)
                       (gen/stagger (/ (:rate opts)))
                       (gen/nemesis (:generator nemesis))
                       (gen/time-limit (:time-limit opts)))
        generator (if-let [fg (:final-generator workload)]
                    (gen/phases generator
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for cluster to recover")
                                (gen/sleep 10)
                                (gen/clients fg))
                    generator)
        checker (checker/compose
                  {:perf        (checker/perf {:nemeses (:perf nemesis)})
                   :clock       (checker/clock-plot)
                   :stats       (checker/stats)
                   :exceptions  (checker/unhandled-exceptions)
                   :workload    (:checker workload)})]
    (merge tests/noop-test
           opts
           (dissoc workload :generator :final-generator) ; These we handle
           {:checker      checker
            :name         (str (name (:workload opts))
                               " " (str/join "," (map name (:nemesis opts))))
            :os           debian/os
            :db           db
            :nemesis      (:nemesis nemesis)
            :logging      {:overrides
                           {"com.datastax.driver.core.Connection"   :error
                            "com.datastax.driver.core.ClockFactory" :error
                            "com.datastax.driver.core.Session"      :error
                            "com.datastax.driver.core.ControlConnection" :off
                            "com.datastax.driver.core.Cluster"      :warn
                            }}
            :bootstrap    (atom #{}) ; TODO: remove me
            :decommission (atom #{}) ; TODO: remove me
            :nonserializable-keys [:conductors] ; TODO: remove me
            :generator    generator
            :pure-generators true})))

(def consistency-levels
  "A set of keyword consistency levels the C* driver supports."
  (set (keys (qbits.commons.enum/enum->map ConsistencyLevel))))

(def cli-opts
  "Options for test runners."
  [[nil "--compaction-strategy CLASS" "What compaction strategy should we use for tables?"
    :default "SizeTieredCompactionStrategy"]

   [nil "--[no-]hinted-handoff" "Enable or disable hinted handoff."
    :default true]

   [nil "--key-count INT" "For the append test, how many keys should we test at once?"
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--local-scylla-bin FILE" (str "If provided, uploads the local file to each DB node, replacing " db/scylla-bin ". Helpful for testing development builds.")]

   [nil "--[no-]lwt" "Enables or disables LWT for some workloads."
    :default true]

   [nil "--max-txn-length INT" "What's the most operations we can execute per transaction?"
    :default  5
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--max-writes-per-key INT" "How many writes can we perform to any single key, for append tests?"
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str "Faults must be one of " nemeses " or "
                    (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--[no-]noisy-timestamps" "If set, randomly fuzz timestamps to simulate behavior in a cluster without perfect clocks."
    :default  true]

   [nil "--partition-count NUM" "How many partitions should we spread operations across?"
    :default 1
    :parse-fn parse-long
    :validate? [pos? "must be positive"]]

   [nil "--phi-level LEVEL" "What value should we use for the phi-accrual failure detector? Higher numbers slow down transitions during partitions."
    :default 2]

   ["-r" "--rate HZ" "Approximate number of requests per second per thread"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--read-consistency LEVEL" "What consistency level should we set for reads?"
    :parse-fn keyword
    :validate [consistency-levels (cli/one-of consistency-levels)]]

   [nil "--read-serial-consistency LEVEL"
    "What *serial* consistency level should we set for reads? The C* Java driver docs say this is ignored, but it might actually be used in Scylla?"
    :parse-fn keyword
    :validate [consistency-levels (cli/one-of consistency-levels)]]

   [nil "--table-count NUM" "How many tables should we spread operations across?"
    :default 1
    :parse-fn parse-long
    :validate? [pos? "must be positive"]]

   ["-v" "--version VERSION" "What version of Scylla should we test?"
    :default "4.2"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--write-consistency LEVEL"
    "What consistency level should we set for writes?"
    :parse-fn keyword
    :validate [consistency-levels (cli/one-of consistency-levels)]]

   [nil "--write-serial-consistency LEVEL"
    "What *serial* consistency level should we set for writes?"
    :parse-fn keyword
    :validate [consistency-levels (cli/one-of consistency-levels)]]])

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

(ns scylla.core
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.java.jmx :as jmx]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [generator :as gen]
             [tests     :as tests]]
            [jepsen.control [net :as net]]
            [jepsen.os.debian :as debian]
            [scylla.generator :as sgen])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core Session)
           (com.datastax.driver.core Cluster)
           (com.datastax.driver.core Metadata)
           (com.datastax.driver.core Host)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)))

(defn scaled
  "Applies a scaling factor to a number - used for durations
  throughout testing to easily scale the run time of the whole
  test suite. Accepts doubles."
  [v]
  (let [factor (or (some-> (System/getenv "JEPSEN_SCALE") (Double/parseDouble))
                   1)]
    (Math/ceil (* v factor))))

(defn compaction-strategy
  "Returns the compaction strategy to use"
  []
  (or (System/getenv "JEPSEN_COMPACTION_STRATEGY")
      "SizeTieredCompactionStrategy"))

(defn compressed-commitlog?
  "Returns whether to use commitlog compression"
  []
  (= (some-> (System/getenv "JEPSEN_COMMITLOG_COMPRESSION") (str/lower-case))
     "false"))

(defn coordinator-batchlog-disabled?
  "Returns whether to disable the coordinator batchlog for MV"
  []
  (boolean (System/getenv "JEPSEN_DISABLE_COORDINATOR_BATCHLOG")))

(defn phi-level
  "Returns the value to use for phi in the failure detector"
  []
  (or (System/getenv "JEPSEN_PHI_VALUE")
      8))

(defn disable-hints?
  "Returns true if Jepsen tests should run without hints"
  []
  (not (System/getenv "JEPSEN_DISABLE_HINTS")))

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                   (str "Driver didn't report all nodes were up in "
                        timeout-secs "s - failing")))
           (while (->> conn
                       .getCluster
                       .getMetadata
                       .getAllHosts
                       (map #(.isUp %))
                       and
                       not)
             (Thread/sleep 500))))

(defn dns-resolve
  "Gets the address of a hostname"
  [hostname]
  (.getHostAddress (InetAddress/getByName (name hostname))))

(defn live-nodes
  "Get the list of live nodes from a random node in the cluster"
  [test]
  (set (some (fn [node]
               (try (jmx/with-connection {:host (name node) :port 7199}
                      (jmx/read "org.apache.cassandra.db:type=StorageService"
                                :LiveNodes))
                    (catch Exception _
                      (info "Couldn't get status from node" node))))
             (-> test :nodes set (set/difference @(:bootstrap test))
                 (#(map (comp dns-resolve name) %)) set (set/difference @(:decommission test))
                 shuffle))))

(defn joining-nodes
  "Get the list of joining nodes from a random node in the cluster"
  [test]
  (set (mapcat (fn [node]
                 (try (jmx/with-connection {:host (name node) :port 7199}
                        (jmx/read "org.apache.cassandra.db:type=StorageService"
                                  :JoiningNodes))
                      (catch Exception _
                        (info "Couldn't get status from node" node))))
               (-> test :nodes set (set/difference @(:bootstrap test))
                   (#(map (comp dns-resolve name) %)) set (set/difference @(:decommission test))
                   shuffle))))

(defn nodetool
  "Run a nodetool command"
  [node & args]
  (c/on node (apply c/exec (lit "nodetool") args)))

; This policy should only be used for final reads! It tries to
; aggressively get an answer from an unstable cluster after
; stabilization
(def aggressive-read
  (proxy [RetryPolicy] []
    (onReadTimeout [statement cl requiredResponses
                    receivedResponses dataRetrieved nbRetry]
      (if (> nbRetry 100)
        (RetryPolicy$RetryDecision/rethrow)
        (RetryPolicy$RetryDecision/retry cl)))

    (onWriteTimeout [statement cl writeType requiredAcks
                     receivedAcks nbRetry]
      (RetryPolicy$RetryDecision/rethrow))

    (onUnavailable [statement cl requiredReplica aliveReplica nbRetry]
      (info "Caught UnavailableException in driver - sleeping 2s")
      (Thread/sleep 2000)
      (if (> nbRetry 100)
        (RetryPolicy$RetryDecision/rethrow)
        (RetryPolicy$RetryDecision/retry cl)))))

(defn install!
  "Installs ScyllaDB on the given node."
  [node version]
  (c/su
    (c/cd "/tmp"
          (info "installing ScyllaDB")
          (debian/add-repo!
            "scylla"
            (str "deb  [arch=amd64] http://downloads.scylladb.com/downloads/"
                 "scylla/deb/debian/scylladb-" version " buster non-free")
            "hkp://keyserver.ubuntu.com:80"
            "5e08fbd8b5d6ec9c")
          ; Scylla wants to install SNTP/NTP, which is going to break in
          ; containers--we skip the install here.
          (debian/install [:scylla :scylla-jmx :scylla-tools :ntp-])

          (info "configuring scylla logging")
          (c/exec :mkdir :-p (lit "/var/log/scylla"))
          (c/exec :install :-o :root :-g :adm :-m :0640 "/dev/null"
                  "/var/log/scylla/scylla.log")
          (c/exec :echo
                  ":syslogtag, startswith, \"scylla\" /var/log/scylla/scylla.log\n& ~" :> "/etc/rsyslog.d/10-scylla.conf")
          (c/exec :service :rsyslog :restart)

          ; We don't presently use this, but it might come in handy if we have
          ; to test binaries later.
          (info "copy scylla start script to node")
          (c/su
            (c/exec :echo (slurp (io/resource "start-scylla.sh"))
                    :> "/start-scylla.sh")
            (c/exec :chmod :+x "/start-scylla.sh")))))

(defn configure!
  "Uploads configuration files to the given node."
  [node _]
  (info node "configuring ScyllaDB")
  (c/su
    (c/exec :echo (slurp (io/resource "default/scylla-server"))
            :> "/etc/default/scylla-server")
   (doseq [rep (into ["\"s/.*cluster_name: .*/cluster_name: 'jepsen'/g\""
                      "\"s/row_cache_size_in_mb: .*/row_cache_size_in_mb: 20/g\""
                      (str "\"s/seeds: .*/seeds: '" (dns-resolve :n1) "," (dns-resolve :n2) "'/g\"")
                      (str "\"s/listen_address: .*/listen_address: " (dns-resolve node)
                           "/g\"")
                      (str "\"s/rpc_address: .*/rpc_address: " (dns-resolve node) "/g\"")
                      (str "\"s/broadcast_rpc_address: .*/broadcast_rpc_address: "
                           (net/local-ip) "/g\"")
                      "\"s/internode_compression: .*/internode_compression: none/g\""
                      (str "\"s/hinted_handoff_enabled:.*/hinted_handoff_enabled: "
                           (disable-hints?) "/g\"")
                      "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                      (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                           "commitlog_sync_batch_window_in_ms: 1/g\"" )
                      "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                      (str "\"s/# phi_convict_threshold: .*/phi_convict_threshold: " (phi-level)
                           "/g\"")
                      "\"/auto_bootstrap: .*/d\""]
                     (when (compressed-commitlog?)
                       ["\"s/#commitlog_compression.*/commitlog_compression:/g\""
                        (str "\"s/#   - class_name: LZ4Compressor/"
                             "    - class_name: LZ4Compressor/g\"")]))]
     (c/exec :sed :-i (lit rep) "/etc/scylla/scylla.yaml"))
   (c/exec :echo (str "auto_bootstrap: "  true)
           :>> "/etc/scylla/scylla.yaml")))

(defn syclla-setup!
  "This runs a one-time benchmark to tune system settings. This actually looks like it does... more than just that--there's stuff in here about mucking with NTP. Maybe enable this later?"
  [node]
  (c/su
    (c/exec :scylla_setup)))

(defn start!
  "Starts ScyllaDB"
  [node _]
  (info node "starting ScyllaDB")
  (c/su
    (c/exec :service :scylla-server :start)
   ; TODO: Poll Scylla for startup, rather than waiting 2 minutes (!)
   (Thread/sleep 120000))
  (info node "started ScyllaDB"))

(defn guarded-start!
  "Guarded start that only starts nodes that have joined the cluster already
  through initial DB lifecycle or a bootstrap. It will not start decommissioned
  nodes."
  [node test]
  (let [bootstrap     (:bootstrap test)
        decommission  (:decommission test)]
    (when-not (or (contains? @bootstrap node)
                  (->> node name dns-resolve (get decommission)))
      (start! node test))))

(defn stop!
  "Stops ScyllaDB"
  [node]
  (info node "stopping ScyllaDB")
  (c/su
   (meh (c/exec :killall :scylla-jmx))
   (while (str/includes? (c/exec :ps :-ef) "scylla-jmx")
     (Thread/sleep 100))
   (meh (c/exec :killall :scylla))
   (while (str/includes? (c/exec :ps :-ef) "scylla")
     (Thread/sleep 100)))
  (info node "has stopped ScyllaDB"))

(defn wipe!
  "Shuts down Scylla and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
    ; TODO: wipe log files?
    (meh (c/exec :rm :-rf (lit "/var/lib/scylla/data/*")))))

(defn db
  "New ScyllaDB run"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! test)
        ; Not sure this is safe to run yet--it looks to mess with other system
        ; settings.
        ; (scylla-setup!)
        (guarded-start! test)))

    (teardown! [_ test node]
      (when-not (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
        (wipe! node)))

    db/LogFiles
    (log-files [db test node]
      ["/var/log/scylla/scylla.log"])))

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

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r {:type :invoke :f :read})
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
   (gen/once r)))

(defn safe-mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.
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
      (#(map (comp dns-resolve name) %))
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

(defn crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (test-aware-node-start-stopper
   safe-mostly-small-nonempty-subset
   (fn start [_ node]
     (meh (c/su (c/exec :killall :-9 :scylla-jmx :scylla))) [:killed node])
   (fn stop  [test node]
     (meh (guarded-start! node test)) [:restarted node])))

(defn scylla-test
  [name opts]
  (-> tests/noop-test
      (merge {:name    (str "scylla " name)
              :os      debian/os
              :db      (db "4.2")
              :bootstrap (atom #{})
              :decommission (atom #{})
              :nonserializable-keys [:conductors]})
      ; TODO: Scylla originally set this up by making hardcoded changes to
      ; jepsen.control; I've pulled it out to this point so that we don't need
      ; a custom version of Jepsen to run the test. I've also commented this
      ; out, because it breaks the test for non-root users--I think it's
      ; intended for Docker only. Make sure this is configurable when we switch
      ; over to using jepsen.cli--I think we've got CLI flags for this already
      ; but double-check.
      ;(update :ssh assoc :private-key-path "/root/.ssh/id_rsa"
      ;                   :strict-host-key-checking :no)
      ; Hardcoding until we have a CLI runner--this is for EC2 specifically.
      ;(update :ssh assoc :username "admin")
      (merge opts)
      ))

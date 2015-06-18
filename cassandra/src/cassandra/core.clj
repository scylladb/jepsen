(ns cassandra.core
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [checker   :as checker]
             [model     :as model]
             [generator :as gen]
             [nemesis   :as nemesis]
             [store     :as store]
             [report    :as report]
             [tests     :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control [net :as net]
             [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)))

(defn scaled
  "Applies a scaling factor to a number - used for durations
  throughout testing to easicaly scale the run time of the whole
  test suite. Accepts doubles."
  [v]
  (let [factor (or (some-> (System/getenv "JEPSEN_SCALE") (Double/parseDouble))
                   1)]
    (Math/ceil (* v factor))))

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                   (str "Driver didn't report all nodes were up in "
                        timeout-secs "s - failing")))
           (while (->> (cassandra/get-hosts conn)
                       (map :is-up) and not)
             (Thread/sleep 500))))

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

(def setup-lock (Object.))

(defn cached-install?
  [src]
  (try (c/exec :grep :-s :-F :-x (lit src) (lit ".download"))
       true
       (catch RuntimeException _ false)))

(defn install!
  "Installs Cassandra on the given node."
  [node version]
  (c/su
   (c/cd
    "/tmp"
    (let [tpath (System/getenv "CASSANDRA_TARBALL_PATH")
          url (or tpath
                  (System/getenv "CASSANDRA_TARBALL_URL")
                  (str "http://www.us.apache.org/dist/cassandra/" version
                       "/apache-cassandra-" version "-bin.tar.gz"))]
      (info node "installing Cassandra from" url)
      (if (cached-install? url)
        (info "Used cached install on node" node)
        (do (if tpath
              (c/scp* tpath "/tmp/cassandra.tar.gz")
              (c/exec :wget :-O "cassandra.tar.gz" url (lit ";")))
            (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~")
            (c/exec :rm :-r :-f (lit "~/cassandra"))
            (c/exec :mv (lit "~/apache* ~/cassandra"))
            (c/exec :echo url :> (lit ".download")))))
    (c/exec
     :echo
     "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     :>"/etc/apt/sources.list.d/webupd8team-java.list")
    (c/exec
     :echo
     "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     :>> "/etc/apt/sources.list.d/webupd8team-java.list")
    (try (c/exec :apt-key :adv :--keyserver "hkp://keyserver.ubuntu.com:80"
                :--recv-keys "EEA14886")
         (debian/update!)
         (catch RuntimeException e
           (info "Error updating caused by" e)))
    (c/exec :echo
            "debconf shared/accepted-oracle-license-v1-1 select true"
            | :debconf-set-selections)
    (debian/install [:oracle-java8-installer]))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra")
  (c/su
   (doseq [rep ["\"s/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE='512M'/g\""
                "\"s/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE='128M'/g\""]]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra-env.sh"))
   (doseq [rep ["\"s/cluster_name: .*/cluster_name: 'jepsen'/g\""
                "\"s/row_cache_size_in_mb: .*/row_cache_size_in_mb: 20/g\""
                "\"s/seeds: .*/seeds: 'n1,n2'/g\""
                (str "\"s/listen_address: .*/listen_address: " (net/local-ip)
                     "/g\"")
                (str "\"s/rpc_address: .*/rpc_address: " (net/local-ip) "/g\"")
                (str "\"s/broadcast_rpc_address: .*/broadcast_rpc_address: "
                     (net/local-ip) "/g\"")
                "\"s/internode_compression: .*/internode_compression: none/g\""
                "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                     "commitlog_sync_batch_window_in_ms: 1.0/g\"")
                "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                "\"/auto_bootstrap: .*/d\""]]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra.yaml"))
   (c/exec :echo "auto_bootstrap: false" :>> "~/cassandra/conf/cassandra.yaml")))

(defn start!
  "Starts Cassandra."
  [node test]
  (info node "starting Cassandra")
  (c/su
   (c/exec (lit "~/cassandra/bin/cassandra"))))

(defn stop!
  "Stops Cassandra."
  [node]
  (info node "stopping Cassandra")
  (c/su
   (meh (c/exec :killall :java))))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :rm :-r "~/cassandra/data/data"))
   (meh (c/exec :rm :-r "~/cassandra/data/commitlog"))
   (meh (c/exec :rm :-r "~/cassandra/data/saved_caches"))))

(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! test)
        (start! test)))

    (teardown! [_ test node]
      (wipe! node))))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
   (gen/phases
    (gen/once {:type :info, :f :stop})
    (gen/sleep 10))))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
   (->> gen
        (gen/nemesis
         (gen/seq (cycle [(gen/sleep (scaled 20))
                          {:type :info :f :start}
                          (gen/sleep (scaled 60))
                          {:type :info :f :stop}])))
        (gen/time-limit (scaled 600)))
   (recover)
   (gen/clients
    (->> gen
         (gen/time-limit (scaled 60))))))

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

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
   (gen/once r)))

(defn mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.
      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(def crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  (nemesis/node-start-stopper
   mostly-small-nonempty-subset
   (fn start [test node] (c/su (c/exec :killall :-9 :java)) [:killed node])
   (fn stop  [test node] (start! node test) [:restarted node])))

(defn cassandra-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "cassandra " name)
          :os      debian/os
          :db      (db "2.1.6")}
         opts))

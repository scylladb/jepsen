(ns scylla.db
  "Database setup and teardown."
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
             [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [scylla [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core Session)
           (com.datastax.driver.core Cluster)
           (com.datastax.driver.core Metadata)
           (com.datastax.driver.core Host)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)))

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

(defn install!
  "Installs ScyllaDB on the given node."
  [node version]
  (c/su
    (c/cd "/tmp"
          ; Scylla has a mandatory dep on jdk8
          (info "installing JDK8")
          ; LIVE DANGEROUSLY
          (c/exec :wget :-qO :- "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" | :apt-key :add :-)
          (debian/add-repo! "adoptopenjdk" "deb  [arch=amd64] https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main")
          (debian/install [:adoptopenjdk-8-hotspot])

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

(defn seeds
  "Returns a comma-separated string of seed nodes to join to."
  [test]
  (->> (:nodes test)
       (map dns-resolve)
       (str/join ",")))

(defn configure!
  "Uploads configuration files to the current node."
  [node test]
  (info "configuring ScyllaDB")
  (c/su
    (c/exec :echo (slurp (io/resource "default/scylla-server"))
            :> "/etc/default/scylla-server")
    (c/exec :echo
            (-> (io/resource "scylla.yaml")
                slurp
                (str/replace "$SEEDS"           (seeds test))
                (str/replace "$LISTEN_ADDRESS"  (dns-resolve node))
                (str/replace "$RPC_ADDRESS"     (dns-resolve node))
                (str/replace "$HINTED_HANDOFF"  (str (boolean (:hinted-handoff test))))
                (str/replace "$PHI_LEVEL"       (str (:phi-level test))))
            :> "/etc/scylla/scylla.yaml")))

(defn guarded-start!
  "Guarded start that only starts nodes that have joined the cluster already
  through initial DB lifecycle or a bootstrap. It will not start decommissioned
  nodes."
  [node test db]
  (let [bootstrap     (:bootstrap test)
        decommission  (:decommission test)]
    (when-not (or (contains? @bootstrap node)
                  (->> node name dns-resolve (get decommission)))
      (db/start! db test node))))

(defn db
  "Sets up and tears down ScyllaDB"
  [version]
  (let [tcpdump (db/tcpdump {:ports         [9042]
                             :clients-only? true})]
    (reify db/DB
      (setup! [db test node]
        (db/setup! tcpdump test node)
        (doto node
          (install! version)
          (configure! test))
        (let [t1 (util/linear-time-nanos)]
          (guarded-start! node test db)
          (sc/close! (sc/await-open test node))
          (info "Scylla startup complete in"
                (float (util/nanos->secs (- (util/linear-time-nanos) t1)))
                "seconds")))

      (teardown! [db test node]
        (db/kill! db test node)
        (c/su
          (info "deleting data files")
          (meh (c/exec :rm :-rf
                       ; We leave directories in place; Scylla gets confused
                       ; without them.
                       (lit "/var/lib/scylla/data/*")
                       (lit "/var/lib/scylla/commitlog/*")
                       (lit "/var/lib/scylla/hints/*")
                       (lit "/var/lib/scylla/view_hints/*")
                       "/var/log/scylla/scylla.log")))
        (db/teardown! tcpdump test node))

      db/LogFiles
      (log-files [db test node]
        (concat (db/log-files tcpdump test node)
                ["/var/log/scylla/scylla.log"]))

      db/Process
      (start! [_ test node]
        (info "starting ScyllaDB")
        (c/su
          (c/exec :service :scylla-server :start)
          (info "started ScyllaDB")))

      (kill! [_ test node]
        (info node "stopping ScyllaDB")
        (c/su
          (cu/grepkill! "scylla-jmx")
          (cu/grepkill! "scylla")
          (try+ (c/exec :service :scylla-server :stop)
                ; Not installed yet?
                (catch [:exit 1] e)
                (catch [:exit 5] e)))
        (info node "has stopped ScyllaDB"))

      db/Pause
      (pause! [_ test node]
        (c/su (cu/grepkill! :stop "/usr/bin/scylla")))

      (resume! [_ test node]
        (c/su (cu/grepkill! :cont :scylla))))))

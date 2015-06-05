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
           (com.datastax.driver.core ConsistencyLevel)))

(def setup-lock (Object.))

(defn install!
  "Installs Cassandra on the given node."
  [node version]
  (c/su
   (info node "installing Cassandra" version "from binary tarball")
   (c/cd
    "/tmp"
    (c/exec
     :echo
     "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     :>"/etc/apt/sources.list.d/webupd8team-java.list")
    (c/exec
     :echo
     "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     :>> "/etc/apt/sources.list.d/webupd8team-java.list")
    (c/exec :apt-key :adv :--keyserver "hkp://keyserver.ubuntu.com:80"
            :--recv-keys "EEA14886")
    (debian/update!)
    (c/exec :echo
            "debconf shared/accepted-oracle-license-v1-1 select true"
            | :debconf-set-selections)
    (debian/install [:oracle-java8-installer])
    (meh (c/exec :wget :-c :-nc :-O "cassandra.tar.gz"
                 (str "http://www.us.apache.org/dist/cassandra/" version
                      "/apache-cassandra-" version "-bin.tar.gz")))
    (meh (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~"))
    (meh (c/exec :mv (str  "~/apache-cassandra-" version) "~/cassandra")))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra")
  (c/su
   (c/exec :echo
           (-> "cassandra-env.sh"
               io/resource
               slurp)
           :> "~/cassandra/conf/cassandra-env.sh")
   (c/exec :echo
           (-> "cassandra.yaml"
               io/resource
               slurp
               (str/replace "$NODE_IP" (net/local-ip))
               (str/replace "$NODES" (->> test :nodes (map name)
                                          (str/join ","))))
           :> "~/cassandra/conf/cassandra.yaml")))

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
   (meh (c/exec :pkill :-f (lit "'java.*cassandra.*'")))))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (c/exec :rm :-r "~/cassandra/data/data")
   (c/exec :rm :-r "~/cassandra/data/commitlog")
   (c/exec :rm :-r "~/cassandra/data/saved_caches")))

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
   (gen/once {:type :invoke :f :read})))

(defn cassandra-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "cassandra " name)
          :os      debian/os
          :db      (db "2.1.5")}
         opts))

(ns scylla.nemesis
  "All kinds of failure modes for Scylla!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [scylla [client :as client]
                    [db :as sdb]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [nemesis :as n]]
            [jepsen.nemesis [time :as nt]
                            [combined :as nc]]))

(defn package
  "Constructs a {:nemesis, :generator, :final-generator} map for the test.
  Options:

      :interval How long to wait between faults
      :db       The database we're going to manipulate.
      :faults   A set of faults, e.g. #{:kill, :pause, :partition}
      :targets  A map of options for each type of fault, e.g.
                {:partition {:targets [:majorities-ring ...]}}"
  [opts]
  (nc/nemesis-package opts))

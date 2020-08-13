(ns scylla.generator
  "Additional generators for Scylla"
  (:require [jepsen.generator :as gen]))

(defn conductor
  "Combines a generator of normal operations and a generator for a conductor.
  Takes a process name for the conductor process (e.g. :nemesis), the generator
  which should produces conductor operations, and optionally, a generator for
  other processes."
  ([conductor conductor-gen]
   (gen/on #{conductor} conductor-gen))
  ([conductor conductor-gen src-gen]
   (gen/concat (gen/on #{conductor} conductor-gen)
               (gen/on (complement #{conductor}) src-gen))))

(ns scylla.client
  "Basic Scylla client operations."
  (:require [qbits.alia :as alia]
            [qbits.hayt :as hayt]
            [dom-top.core :as dt]
            [clojure.tools.logging :refer [info warn]])
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defn open
  "Returns an map of :cluster :session bound to the given node."
  [node]
  ; TODO: don't do a cluster config--we want to bind specifically to a single
  ; node.
  (let [cluster (alia/cluster
                  {:contact-points [node]
                   ; We want to force all requests to go to this particular
                   ; node, to make sure that every node actually tries to
                   ; execute requests--if we allow the smart client to route
                   ; requests to other nodes, we might fail to observe behavior
                   ; on isolated nodes during a partition.
                   :load-balancing-policy {:whitelist [{:hostname node
                                                        :port 9042}]}})]
    (try (let [session (alia/connect cluster)]
           {:cluster cluster
            :session session})
         (catch Throwable t
           (alia/shutdown cluster)
           (throw t)))))

(defn close!
  "Closes a connection map--both cluster and session."
  [conn]
  (alia/shutdown (:session conn))
  (alia/shutdown (:cluster conn)))

(def await-open-interval
  "How long to sleep between connection attempts, in ms"
  5000)

(defn await-open
  "Blocks until a connection is available, then returns that connection."
  [node]
  (dt/with-retry [tries 32]
    (let [c (open node)]
      (info :session (:session c))
      (info :desc-cluster
            (alia/execute (:session c)
                          (hayt/->raw (hayt/select :system.peers))))

      c)
    (catch NoHostAvailableException e
      (when (pos? tries)
        (info node "not yet available, retrying")
        (Thread/sleep await-open-interval)
        (retry (dec tries))))))

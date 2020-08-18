(ns scylla.client
  "Basic Scylla client operations."
  (:require [qbits.alia :as alia]
            [qbits.hayt :as hayt]
            [dom-top.core :as dt]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)
           (com.datastax.driver.core Session
                                     Cluster
                                     Metadata
                                     Host)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)))

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
      (when (zero? tries)
        (throw+ {:type :await-open-timeout
                 :node node}))
      (info node "not yet available, retrying")
      (Thread/sleep await-open-interval)
      (retry (dec tries)))))

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



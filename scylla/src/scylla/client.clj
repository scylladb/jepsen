(ns scylla.client
  "Basic Scylla client operations."
  (:require [qbits.alia :as alia]
            [qbits.alia.policy [load-balancing :as load-balancing]]
            [qbits.hayt :as hayt]
            [dom-top.core :as dt]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.datastax.driver.core.exceptions NoHostAvailableException
                                                OperationTimedOutException
                                                ReadFailureException
                                                ReadTimeoutException
                                                TransportException
                                                UnavailableException
                                                WriteFailureException
                                                WriteTimeoutException)
           (com.datastax.driver.core Session
                                     Cluster
                                     Metadata
                                     Host
                                     TimestampGenerator)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetSocketAddress)))


(defn naive-timestamps
  "This timestamp generator uses System/currentTimeMillis as its source."
  []
  (reify TimestampGenerator
    (next [x]
      (-> (System/currentTimeMillis)
          (* 1000)
          long))))

(defn noisy-timestamps
  "This timestamp generator returns randomly distributed values around now, but
  with 100 seconds of uncertainty."
  []
  (reify TimestampGenerator
    (next [x]
      (let [uncertainty-s   100
            uncertainty-ms  (* 1000 uncertainty-s)]
        (-> (System/currentTimeMillis)
            (+ (rand-int uncertainty-ms))
            (- (/ uncertainty-ms 2))
            (* 1000)
            long)))))

(defn open
  "Returns an map of :cluster :session bound to the given node."
  [test node]
  (let [opts (cond->
               {:contact-points [node]
                ; We want to force all requests to go to this particular node,
                ; to make sure that every node actually tries to execute
                ; requests--if we allow the smart client to route requests to
                ; other nodes, we might fail to observe behavior on isolated
                ; nodes during a partition. The docs suggest this works, but it
                ; looks like it doesn't actually in practice:
                ;:load-balancing-policy {:whitelist [{:hostname node
                ;                                     :port 9042}]}
                ; This *mostly* works. It looks like table creation and some
                ; other queries still get routed to other nodes, but at least
                ; DML goes to only the specified node?
                :load-balancing-policy
                (load-balancing/whitelist-policy
                  (load-balancing/round-robin-policy)
                  [(InetSocketAddress. node 9042)])
                ; By default the client has an exponential backoff on reconnect
                ; attempts, which can keep us from detecting when a node has
                ; come back
                :reconnection-policy {:type              :constant
                                      :constant-delay-ms 1000}}

               (:noisy-timestamps test) (assoc :timestamp-generator
                                               (noisy-timestamps)))
        cluster (alia/cluster opts)]
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
  [test node]
  (dt/with-retry [tries 60]
    (let [c (open test node)]
      (alia/execute (:session c)
                    (hayt/->raw (hayt/select :system.peers)))
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

(defn read-opts
  "Returns an options map, suitable for passing to alia/execute!, for a read.
  Uses the `test` to decide what :consistency and :serial-consistency to use.
  Will not include keys when a test's values are `nil`, which means you can
  write

    (merge {:consistency :quorum} (read-options test))

  to provide default options suitable for your workload."
  [test]
  (let [c (:read-consistency test)]
    (cond-> {}
      c (assoc :consistency c))))

(defn write-opts
  "Returns an options map, suitable for passing to alia/execute!, for a write.
  Uses the `test` to decide what :consistency and :serial-consistency to use.
  Will not include keys when a test's values are `nil`, which means you can
  write

    (merge {:consistency :quorum} (write-options test))

  to provide default options suitable for your workload."
  [test]
  (let [c  (:write-consistency test)
        sc (:write-serial-consistency test)]
    (cond-> {}
      c  (assoc :consistency c)
      sc (assoc :serial-consistency sc))))

(def applied-kw
  "This is the special field Scylla uses to indicate a row update was applied."
  (keyword "[applied]"))

(defn applied?
  "Takes a collection of rows from alia/execute, and returns true if all rows
  were applied."
  [rows]
  (every? applied-kw rows))

(defn assert-applied
  "Takes a collection of rows from (alia/execute) and asserts that all of them
  have `[applied] true`; if not, throws. Returns rows."
  [rows]
  (cond (applied? rows)
        rows

        (some applied-kw rows)
        (throw+ {:type      :partially-applied
                 :message   "Some changes, but not others, were applied!"
                 :definite? false
                 :rows      rows})

        true
        (throw+ {:type      :not-applied
                 :message   "No  changes were applied."
                 :definite? true
                 :rows      rows})))

(defmacro remap-errors-helper
  "Basic error remapping. See remap-errors."
  [& body]
  `(try+ ~@body
         (catch NoHostAvailableException e#
           (throw+ {:type       :no-host-available
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch OperationTimedOutException e#
           (throw+ {:type       :operation-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch ReadFailureException e#
           (throw+ {:type       :read-failure
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch ReadTimeoutException e#
           (throw+ {:type       :read-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch TransportException e#
           (throw+ {:type       :transport
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch UnavailableException e#
           (throw+ {:type       :unavailable
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch WriteFailureException e#
           (throw+ {:type       :write-failure
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch WriteTimeoutException e#
           (throw+ {:type       :write-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))))

(defmacro remap-errors
  "Evaluates body, catching known client errors and remapping them to Slingshot
  exceptions for ease of processing."
  [& body]
  `(try+ (remap-errors-helper ~@body)
         ; Sometimes, but not always, Alia wraps exceptions in its own ex-info,
         ; which *would* be helpful if we didn't already have to catch the
         ; Cassandra driver exceptions on our own. We extract the cause of the
         ; ex-info in this case, and try remapping it.
         (catch [:type :qbits.alia/execute] e#
           (remap-errors-helper (throw (:cause ~'&throw-context))))))

(defmacro slow-no-host-available
  "Introduces artificial latency for NoHostAvailableExceptions, which
  prevents us from performing a million no-op requests a second when the client
  thinks every node is down."
  [& body]
  `(try ~@body
        (catch NoHostAvailableException e#
          (Thread/sleep 2000)
          (throw e#))))

(defn known-error?
  "For use in try+ catch expressions: is this thrown object one we generated?"
  [ex]
  (and (map? ex) (contains? ex :definite?)))

(defmacro with-errors
  "Takes an operation, a set of :f's which are idempotent, and a body to
  evaluate. Evaluates body, slowing no-host-available errors, remapping errors
  to friendly ones. When a known error is caught, returns op with :type :fail
  or :info, depending on whether or not it is a definite error, and whether the
  operation is idempotent."
  [op idempotent? & body]
  `(try+ (remap-errors (slow-no-host-available ~@body))
         (catch known-error? e#
           (assoc ~op
                  :type (if (or (~idempotent? (:f ~op))
                                (:definite? e#))
                          :fail
                          :info)
                  :error e#))))

(def retry-delay
  "Roughly how long should we wait between auto-retry attempts, in millis?"
  1000)

(defn wrap-retry
  "Takes a form and returns a form which is wrapped in remap-errors and a
  transparent retry."
  [form]
  `(dt/with-retry [tries# 10]
     (try+ (remap-errors ~form)
           (catch known-error? e#
             (when (zero? tries#)
               (throw+ e#))
             (info (pr-str (quote ~form)) "threw" e#
                   "; automatically retrying")
             (Thread/sleep (rand-int retry-delay))
             (~'retry (dec tries#))))))

(defmacro retry-each
  "Schema creation and initial inserts of values tend to fail pretty often.
  This macro takes a series of forms and evaluates them in order, retrying each
  form if it throws a known Scylla error. Since these forms will be retried,
  they need to be idempotent!"
  [& forms]
  (cons 'do (map wrap-retry forms)))

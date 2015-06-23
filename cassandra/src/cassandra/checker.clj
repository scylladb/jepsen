(ns cassandra.checker
  (require [clojure.core.reducers :as r]
           [clojure.tools.logging :refer [debug info warn]]
           [jepsen.model :as model]
           [knossos.core :as knossos])
  (:import jepsen.checker.Checker))

(defn complete-rewrite-fold-op
  "Temporarily copied from knossos.history. Rewrites failed CAS to :read."
  [[history index] op]
  (condp = (:type op)
    ; An invocation; remember where it is
    :invoke
    (do
      ; Enforce the singlethreaded constraint.
      (when-let [prior (get index (:process op))]
        (throw (RuntimeException.
                 (str "Process " (:process op) " already running "
                      (pr-str (get history prior))
                      ", yet attempted to invoke "
                      (pr-str op) " concurrently"))))

      [(conj! history op)
       (assoc! index (:process op) (dec (count history)))])

    ; A completion; fill in the completed value.
    :ok
    (let [i           (get index (:process op))
          _           (assert i)
          invocation  (nth history i)
          value       (or (:value invocation) (:value op))
          invocation' (assoc invocation :value value)]
      [(-> history
           (assoc! i invocation')
           (conj! op))
       (dissoc! index (:process op))])

    ; A failure; fill in either value.
    :fail
    (let [i           (get index (:process op))
          _           (assert i)          
          invocation  (nth history i)
          value       (or (:value invocation) (:value op))
          [invocation' op'] (if (and (= :cas (:f op)) (number? (:value op)))
                                  [(assoc invocation :f :read :value (:value op))
                                   (assoc op :f :read :type :ok)]
                                  [(assoc invocation :value value) (assoc op :value value)])]
      [(-> history
           (assoc! i invocation')
           (conj! op'))
       (dissoc! index (:process op))])

    ; No change for info messages
    :info
    [(conj! history op) index]))

(defn complete-and-rewrite
  "Temporarily copied from knossos.history until we figure out if rewriting history
  is a good idea."
  [history]
  (->> history
       (reduce complete-rewrite-fold-op
               [(transient []) (transient {})])
       first
       persistent!))

(defn enhanced-analysis
  "An enhanced version of analysis in knossos.core which rewrites failed CAS to
  :read"
  [model history]
  (let [history+            (complete-and-rewrite history)
        [lin-prefix worlds] (knossos/linearizable-prefix-and-worlds model history+)
        valid?              (= (count history+) (count lin-prefix))
        evil-op             (when-not valid?
                              (nth history+ (count lin-prefix)))

        ; Remove worlds with equivalent states
        worlds              (->> worlds
                                 ; Wait, is this backwards? Should degenerate-
                                 ; world-key be the key in this map?
                                 (r/map (juxt knossos/degenerate-world-key identity))
                                 (into {})
                                 vals)]
    (if valid?
      {:valid?              true
       :linearizable-prefix lin-prefix
       :worlds              worlds}
      {:valid?                   false
       :linearizable-prefix      lin-prefix
       :last-consistent-worlds   worlds
       :inconsistent-op          evil-op
       :inconsistent-transitions (map (fn [w]
                                      [(:model w)
                                       (-> w :model (knossos/step evil-op) :msg)])
                                      worlds)})))

(def enhanced-linearizable
  "A linearizability checker using Knossos that rewrites failed CAS operations
  to :read, since these are returned in Cassandra from failed LWT CAS"
  (reify Checker
    (check [this test model history]
      (enhanced-analysis model history))))

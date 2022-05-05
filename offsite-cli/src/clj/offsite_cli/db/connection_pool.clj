(ns offsite-cli.db.connection-pool
  ^{:author       "Derek Ealy <dealy663@gmail.com>"
    :organization "http://grandprixsw.com"
    :project      "offsite"
    :date         "5/1/22"
    :doc          ""
    :no-doc       true}
  (:require [mount.core :as mount]
            [offsite-cli.config :refer [env]]
            [manifold.stream :as ms]
            [xtdb.api :as xt]
            [clojure.tools.logging :as log]
            [offsite-cli.system-utils :as su]
            [manifold.deferred :as md]))

(def node-count (volatile! 0))
;(def empty-connection-pool [])
(def empty-cp {:max-connections (-> env :connection-pool-cfg :max-connections)
               :node-count            0
               :ready-connections     nil
               :busy-connections      nil
               :stream                nil})
(def cp (ref empty-cp))

(declare init-cp stop-cp connection-pool*)
(mount/defstate! connection-pool*
  :start (init-cp)
  :stop  (stop-cp))

(defn get-node
  ""
  []

  (let [s (:stream @cp)]
    (assert (not (nil? s)) "The node pool has not been initialized")
    (assert (not (ms/closed? s)) "The node pool is closed")

    (if-let [n @(ms/try-take! s 0)]
      (do
        (su/dbg "got node: " n)
        n)
      (dosync
        (when (< (:node-count @cp) (:max-connections @cp))
          (su/dbg "adding new node")
          (let [d (ms/try-put! s (xt/start-node (:xtdb-config env)) 100)]
            (if-not @d
              (log/error "Unable to add new db node to pool queue")
              (do
                (alter cp update :node-count inc)
                (get-node)))))))))

(defn release-node
  ""
  [node]

  (su/dbg "releasing node: " node)
  (when (xt/status node)
    (ms/put! (:stream @cp) node)))

(defn close-node
  ""
  [node]

  (.close node)
  (dosync
    (if (> (:node-count @cp) 0)
      (alter cp update :node-count dec)
      (log/warn ":node-count is out of sync (below zero) in node-pool"))))

(defmacro with-cp-node
  "bindings => [name (get-node) ...]

  Evaluates body in a try expression with names bound to the XTDB node values,
  and a finally clause that calls (release-node name) on each
  name in reverse order."
  {:added "1.0"}
  [bindings & body]
  {:pre [(vector? bindings)                                 ;"a vector for its binding"
         (even? (count bindings))                           ;"an even number of forms in binding vector"
         ]}
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-cp-node ~(subvec bindings 2) ~@body)
                                (finally
                                  (release-node ~(bindings 0)))))
    :else (throw (IllegalArgumentException.
                   "with-cp-node only allows Symbols in bindings"))))

(defn init-cp
  "Initialize the connection pool with a single connection"
  ([]

   (init-cp (-> env :connection-pool-cfg :max-connections)))

  ([max-connections]
   (dosync
     (ref-set cp empty-cp)
     (alter cp assoc :max-connections max-connections       ;:connections empty-connection-pool
            :stream (ms/stream max-connections)))))

(defn stop-cp
  "Close all nodes"
  []

  (dosync
    (when-let [s (:stream @cp)]
      (ms/consume #(close-node %) s)
      (ms/close! s))))
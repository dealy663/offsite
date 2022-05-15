(ns offsite-cli.block-processor.bp-core
  ^{:author       "Derek Ealy <dealy663@gmail.com>"
    :organization "http://grandprixsw.com"
    :date         "1/15/2022"
    :doc          "Offsite-Client block processing "
    :no-doc       true}
  (:require [clojure.core.async :as a]
            [clj-commons.digest :refer :all]
            [clojure.java.io :as io]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.channels :refer :all]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.system-utils :as su]
            [offsite-cli.remote.os-service :as svc]
            [offsite-cli.remote.os-node :as node]
            [clojure.tools.logging :as log]
            [offsite-cli.channels :as ch]
            [mount.core :as mount]
            [offsite-cli.init :as init])
  (:import (java.io ByteArrayOutputStream)
           (org.apache.commons.lang3 NotImplementedException)))

(def max-block-chunk -1)                                    ;; A negative value means to read the whole file
(def stop-key :stop-bp)
(def bp-counters-empty {:catalog-block-count 0
                        :onsite-block-count  0})
(def bp-counters (atom bp-counters-empty))
(def bp-state-empty {:started                false
                     :halt                   false
                     :onsite-path-processor  []
                     :catalog-update         nil
                     :offsite-nodes          []               ;; a sequence of node connections and response statistics
                     :onsite-block-q         []})
(def bp-state (ref bp-state-empty))
(swap! init/client-state update :service-keys conj :onsite :offsite)

(declare start stop split encrypt broadcast bp-reset!)
;(mount/defstate ^{:on-reload :noop} bp-state
;                :start (start)
;                :stop (stop))

(defn get-checksum
  "Creates a checksum on one of the following types of messages:
    * String
    * byte Array
    * File
    * InputStream
    * A sequence of byte Arrays

    You can use one of several possible digests: sha3-384 sha-256 sha3-256 sha-384 sha3-512 sha-1 sha-224 sha1 sha-512
    md2 sha sha3-224 md5

    Params:
    message      The data to be checksummed
    digest       (optional - default clj-commons.digest/md5) digest function to use"
  ([message]
   (get-checksum message clj-commons.digest/md5))

  ([message digest-fn]
   (digest-fn message)))

(defn make-block-info
  "Create a block-info map for storing details in DB

   Params:
   ons-file-block     A file or directory ready to have its state catalogued in the DB"
  [ons-file-block]

  (let [file (io/file (:orig-path ons-file-block))]
    {:xt/id        (.hashCode file)
     :backup-id    (:backup-id ons-file-block)
     :path         (.getCanonicalPath file)
     :checksum     (get-checksum file)
     :block-type   (if (.isDirectory file) :dir :file)
     :data-type    :offsite-block
     :version      nil
     :last-modified (.lastModified file)
     :hidden?      (.isHidden file)}))

(defn create-ofs-block-state
  "Creates a file-state map in preparation for writing to DB

   Params:
   ofs-blk-info      A ofs-blk-info map or vector of maps representing a file or dir prepared for backup

   Returns a vector of block state maps"
  [ofs-blk-info]

  (when (some? ofs-blk-info)
    (let [block-info-vec (if (vector? ofs-blk-info)
                           ofs-blk-info
                           [ofs-blk-info])]
      ;(su/dbg "create-ofs-block-state: block-info-vec - " block-info-vec)
      (map #(assoc % :version 0) block-info-vec))))



(defn get-block-state!
  "Retrieves existing offsite block state from the DB or creates on if this is a new block

   Params:
   onsite-block     A block to be prepared for broadcast

   Returns a block-state map from the DB"
  [onsite-block]

  (let [offsite-block-info (make-block-info onsite-block)]
    (or (db/get-ofs-block-state! offsite-block-info)
        (let [state (create-ofs-block-state offsite-block-info)]
          (db/easy-ingest! state)
          state))))

(defn event-handler-fns
  "Handles Manifold bus events

  Params:
  event-fns     A sequence of functions which take a single event parameter and execute custom logic"
  [& event-fns]

  (fn [event]
    (mapv #(% event) event-fns)))

(defn bp-reset!
  "Reset the state bindings for the block-processor

  Params:
  state-init-map     (optional - default nil) Map of associations to initialize the bp-state ref with"
  ([]

   (bp-reset! nil))

  ([state-init-map]

   (reset! bp-counters bp-counters-empty)
   (dosync (ref-set bp-state (into bp-state-empty state-init-map)))
   bp-state))


(defn start
  "Start the block processor"
  []

  (let [bps (bp-reset! {:started true :onsite-path-processor nil})]
    ;; create a monitor for catalog-state events
    (ch/m-sub-monitor :catalog-add-block (fn [_] (swap! bp-counters update :catalog-block-count inc)))
    bps))

(defn stop
  []

  ;; no action yet
  )
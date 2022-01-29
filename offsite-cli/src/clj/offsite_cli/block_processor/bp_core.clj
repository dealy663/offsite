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
            [mount.core :as mount]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.system-utils :as su]
            [offsite-cli.remote.os-service :as svc]
            [offsite-cli.remote.os-node :as node]
            [clojure.tools.logging :as log])
  (:import (java.io ByteArrayOutputStream)
           (org.apache.commons.lang3 NotImplementedException)))

(def max-block-chunk -1)                                    ;; A negative value means to read the whole file
(def stop-key :stop-bp)
(def bp-state (ref {:started        false
                    :halt           false
                    :offsite-nodes  []                      ;; a sequence of node connections and response statistics
                    :onsite-block-q []}))

(declare start stop split encrypt broadcast)

;(defn add-block [state event]
;  "Adds a block to the processing channel
;
;   Params:
;   state     The state of the bp-fsm
;   event     The event that triggered this fn call"
;
;  (println "handling unprocessed block: " state " event: " event)
;  ;  (a/>!! chan (:block event))
;  ;(dosync
;  ;  (alter bp-data update-in [:queue] conj block))
;  )

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

  (let [file (:file-dir ons-file-block)]
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
      (su/dbg "create-ofs-block-state: block-info-vec - " block-info-vec)
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



;(defn add-block [backup-block]
;  "Add a backup block to the block-data queue
;
;  params:
;  backup-block     A block of data for backup"
;  (alter bp-data update-in :queue conj backup-block))


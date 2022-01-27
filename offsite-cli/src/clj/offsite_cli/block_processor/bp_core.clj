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
            [java-time :as t]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.system-utils :as su]
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
(mount/defstate block-processor-chans
                :start   (start)
                :stop    (stop))

(defn add-block [state event]
  "Adds a block to the processing channel

   Params:
   state     The state of the bp-fsm
   event     The event that triggered this fn call"

  (println "handling unprocessed block: " state " event: " event)
  ;  (a/>!! chan (:block event))
  ;(dosync
  ;  (alter bp-data update-in [:queue] conj block))
  )

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
   file     A file or directory ready to have its state catalogued in the DB"
  [file]

  {:xt/id          (.hashCode file)
   :path           (.getCanonicalPath file)
   :checksum       (get-checksum file)
   :type           (if (.isDirectory file) :dir :file)
   :version        nil
   :last-modified   (.lastModified file)
   :hidden?        (.isHidden file)})

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

(defn process-file
  "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
   and broadcast to nodes for offsite backup.

   Params:
   ons-file-block     The offsite block representing a file

   Returns file-state map if the object is new or has changed and is to be backed up"
  [ons-file-block]

  (let [file-info  (make-block-info (:file-dir ons-file-block))
        _ (su/dbg "Got block info: " file-info)
        file-state (or (db/get-ofs-block-state! (:xt/id file-info))
                      (let [state (create-ofs-block-state file-info)]
                        (su/dbg "created-ofs-block-state: " (:first state))
                        (su/dbg "easy-ingest! output: " (db/easy-ingest! state))
                        (:first state)))]
    (su/dbg "process-file: fetched block-state - " file-state)
    (when (or (nil? (:version file-state))
              (> 1 (:version file-state))
              (not (= (:checksum file-info) (:checksum file-state))))
      file-state
      #_(split file-state file-block))))

(defn process-dir
  "Processes an onsite block which represents a directory. The directory will have all of its
   children enqueued for block processing and finally the directory itself will be entered
   into the onsite DB and broadcast to nodes for offsite backup.

   Params:
   ons-dir-block     The onsite block representing a directory

   Returns an offsite block for a directory to be prepped for backup"
  [ons-dir-block]

  (doseq [file (.listFiles (:file-dir ons-dir-block))]
    (throw (.NotImplementedException "process-dir not ready"))
    ;; add files to pathDB
    #_(dosync (alter bp-state update-in [:queue] conj (col/create-block {:file-dir file})))))

(defn process-block
  "Handles an onsite block. If it refers to a file then it is read, disintegrated, encrypted and broadcast.
   If it is a directory then a new block for the directory is created and entered into the block queue

   Params:
   block        The onsite block to be processed

   Returns an empty offsite block ready to be prepped for backup"
  [block]

  ;; If the block is a file
  (if (.isDirectory (:file-dir block))
    (process-dir block)
    (process-file block))
  ;; after block has been processed remove it from block-data
  #_(dosync (alter bp-state update-in [:onsite-block-q] rest)))

(defn get-client-info
  "Returns the client info map"
  []

  {:client-id   42
   :public-key  "invalid key"
   :quality     100})

;; Maybe we will leave it to the nodes to figure out the distribution of copies
;; to other nodes
(defn request-nodes
  "Reach out to the offsite-svc to get a group of nodes that it thinks will be good
   candidates for the offsite-blocks to be sent to.

   Params:
   client-info      A map of details about this client :client-id :uptime :speed :quality etc"
  [client-info]

  ;; will need to make rest call against offsite-svc
  ;; for now this returns placeholder node-info maps for testing
  (let [nodes [{:node-id       1
                :quality       100
                :last-response -1}
               {:node-id       2
                :quality       50
                :last-response -1}
               {:node-id       3
                :quality       25
                :last-response -1}]]
    (dosync (alter bp-state assoc-in [:offsite-nodes] nodes))))

(defn send-block!
  "Sends an offsite block to a remote node for storage

   Params:
   node            An Offsite-node connection
   offsite-block   A block representing a file or sub-block ready for storage on a remote Offsite-node

   Returns a response indicating the success of the remote operation"
  [node offsite-block]

  (let [start-time (t/instant)]
    (println "Stored block: " (:xt/id offsite-block) " to node: " (:node-id node))
    {:status   200
     :node     node
     :duration (- (t/instant) start-time)}))

(defn update-ofs-block-state
  "Updates block state information after a block has been successfully stored offsite. This function
   is intended to feed directly into e.g. (easy-ingest! (update-ofs-block-state bfs node))

   Params:
   block-state      State map of a block that was successfully backed up to an offsite node
   response-node    The response of the best node that received the block

   Returns the updated block-state"
  [block-state response-node]

  (-> block-state
      (update :ver inc)
      (dissoc :file-dir)
      (assoc  :last-node (:node response-node))))


(defn broadcast!
  "Send the split, encrypted offsite block out to offsite-nodes for remote backup.

   Params:
   prepped-block      A block fully prepared to be broadcast into the Offsite universe

   Returns block-state with updated details on the broadcast"
  [prepped-block]

  ;; this needs to be updated to send blocks to the offsite nodes in parallel
  (let [{:keys [block-state offsite-block chunk prep-state]} prepped-block
        offsite-nodes (:offsite-nodes @bp-state)]
    (when offsite-nodes
      (loop [nodes         offsite-nodes
             best-response nil]
        (if-not nodes
          (update-ofs-block-state block-state best-response)
          (let [node      (first nodes)
                resp      (send-block! node offsite-block)]
            (recur (rest nodes)
                   (if (<= (:duration resp) (:duration (or best-response resp)))
                     resp
                     best-response))))))))

(declare encrypt-chunk)

(defn encrypt
  "Encrypt an offsite-block with the user's key. Then pass on to broadcast

   Params:
   prepped-block      A block partially prepared for broadcast

   Returns a partially prepared block that has encrypted/signed the chunk"
  [prepped-block]

  (let [{:keys [offsite-block block-state chunk prep-state]} prepped-block]
    (assoc prepped-block :chunk      (encrypt-chunk chunk)
                         :prep-state :encrypted)))

(defn split
  "Splits an offsite file block into pieces no larger than max-block-size

   Params:
   prepped-block      A block partially prepared for broadcast

   Returns a partially prepared block with the file being split and the next chunk to be prepped"
  [prepped-block]

  (let [{:keys [offsite-block block-state input-stream]} prepped-block]
    ;; open file
    (with-open [output-stream   (ByteArrayOutputStream.)]
      ;; read max-chunk bytes into buffer
      (io/copy input-stream output-stream :buf-size max-block-chunk)
      (assoc prepped-block :chunk      (.toByteArray output-stream)
                           :prep-state :chunked))               ;; I think this copies the whole file
    ;; add open file to partial-block
))

(defn prepare-offsite-block!
  "Define the process for prepping an offsite-block for backup and broadcasting the prepped
   offsite-block to the nodes provided by offsite-svc

   Params:
   prepped-block      A block partially prepared for broadcast

   Returns map for next phase of prepping block for broadcast"
  [prepped-block]

  (let [{:keys [offsite-block block-state]} prepped-block]
    (assoc prepped-block :input-stream (io/input-stream (:file-dir offsite-block))
                         :prep-state   :opened)))

(defn- onsite-block-handler
  "Overridable logic for handling onsite blocks to prep for cataloguing and backup

   Params:
   block          The file-dir block that is ready for processing"
  [block]

  (when-not (= stop-key block)
    (su/dbg "\n received block: " block)
    (->> block
        (process-block)
        (put! :offsite-block-chan))))

(defn onsite-block-listener
  "Starts a thread which listens to the ::onsite-block-chan for new blocks which need
   to be processed

   Params:
   block-handler      (optional, default is (onsite-block-handler)), this is intended to be overridden in testing"
  ([block-handler]

   (su/dbg "Dispatching OnBL thread")
   (a/go
     (su/dbg "OnBL: in loop thread")

     (loop [started (:started @bp-state)
            break false]
       (when-not (or break (not started))
         (su/dbg "OnBL: waiting for block")
         (let [block (a/<! (get-ch :onsite-block-chan))]
           (if (nil? block)
             (log/error "Retrieved nil from closed :onsite-block-channel, started: " started ", break: " break)
             (do
               (su/dbg "Took block off :onsite-block-chan, val: " block)
               (block-handler block)
               (recur (:started @bp-state) (= stop-key block))))))))
   (su/dbg "OnBL: Closing ::onsite-block-chan.")
   (close! :onsite-block-chan)
   (su/dbg "OnBL: block-processor stopped"))

  ([]
   (onsite-block-listener onsite-block-handler)))

(defn get-block-state!
  "Retrieves existing offsite block state from the DB or creates on if this is a new block

   Params:
   offsite-block     A block to be prepared for broadcast

   Returns a block-state map from the DB"
  [offsite-block]

  (let [offsite-block-info (make-block-info (:file-dir offsite-block))]
    (or (db/get-ofs-block-state! offsite-block-info)
        (let [state (create-ofs-block-state offsite-block-info)]
          (db/easy-ingest! state)
          state))))

(defn- offsite-block-listener-default
  ""
  []
  (a/go
    (println "OfBL: started loop thread for offsite-blocks")
    (while (:started @bp-state)
      (su/dbg "OfBL: waiting for next offsite block")
      (let [offsite-block (a/<! (get-ch :offsite-block-chan))]
        (when-not (= stop-key offsite-block)
          (su/dbg (str "OfBL: received offsite block: " (:root-dir offsite-block)))
          (-> offsite-block
              ;(get-block-state!)

;; remember that you must indicate the previous block in the backup chain, sub-node, parent dir, previous file etc
              (prepare-offsite-block!)
              (split)                                       ;; This is where a payload is added
              (encrypt)
              (broadcast!)
              (db/easy-ingest!)))))))

(defn offsite-block-listener
  "Starts a thread which listens to the offsite-block channel for blocks that are
   ready for offsite broadcast"
  ([offsite-block-listener-impl]

   (su/dbg "dispatching OfBL thread")
   (offsite-block-listener-impl))

  ([]
   (offsite-block-listener offsite-block-listener-default)))

;(defn add-block [backup-block]
;  "Add a backup block to the block-data queue
;
;  params:
;  backup-block     A block of data for backup"
;  (alter bp-data update-in :queue conj backup-block))

(defn- start-default
  "Standard implementation of start, can be overridden in test by passing customized body"
  []

  (new-channel! :onsite-block-chan stop-key)
  (new-channel! :offsite-block-chan stop-key)
  (onsite-block-listener)
  (offsite-block-listener))

(defn start
  "Start processing block-data from the queue

   Params:
   start-impl        (optional - default is (start-default) can be overridden in tests"
  ([start-impl]

   (when-not (:started @bp-state)
     (su/dbg "Starting bp-core with impl fn: " start-impl)
     (dosync (alter bp-state assoc-in [:started] true))
     (start-impl)))

  ([]
   (start start-default)))

(defn- stop-default
  "Standard implementation of stop, can be overridden int est by passing customized body"
  []

  (a/go
    (a/>! (get-ch :onsite-block-chan) stop-key)
    (a/>! (get-ch :offsite-block-chan) stop-key)))

(defn stop
  "Stops the block-processor, will wait for all blocks in queue to be finished.

   Params:
   stop-impl          (optional - default is (stop-default) can be overridden in tests"
  ([stop-impl]

   (when (:started @bp-state)
     (dosync (alter bp-state assoc :started false))
     (stop-impl)))

  ([]

   (stop stop-default)))
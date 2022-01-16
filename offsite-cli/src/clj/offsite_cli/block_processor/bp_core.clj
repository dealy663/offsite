(ns offsite-cli.block-processor.bp-core
  ^{:author "Derek Ealy <dealy663@gmail.com>"
    :organization "http://grandprixsw.com"
    :date         "1/15/2022"
    :doc    "Offsite-Client block processing "
    :no-doc true}
  (:require [clojure.core.async :as a]
            [clj-commons.digest :refer :all]
            [clojure.java.io :as io]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.channels :refer :all]
            [mount.core :as mount]
            [java-time :as t]
            [offsite-cli.db.db-core :as db]))

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
   (get-checksum message digest-fn)))

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

(defn process-file [file-block]
  "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
   and broadcast to nodes for offsite backup.

   Params:
   file-block     The offsite block representing a file"
  [file-block]

  (let [file-info  (make-block-info (:file-dir file-block))
        file-state (or (db/get-ofs-block-state (:xt/id file-info))
                      (db/create-ofs-block-state file-info))]
    (if-not (= (:checksum file-info) (:checksum file-state))
      (split file-state file-block))))

(defn process-dir [dir-block]
  "Processes an onsite block which represents a directory. The directory will have all of its
   children enqueued for block processing and finally the directory itself will be entered
   into the onsite DB and broadcast to nodes for offsite backup.

   Params:
   dir-block     The onsite block representing a directory"

  (doseq [file (.listFiles (:file-dir dir-block))]
    (dosync (alter bp-state update-in [:queue] conj (col/create-block {:file-dir file})))))

(defn process-block [block]
  "Handles an onsite block. If it refers to a file then it is read, disintegrated, encrypted and broadcast.
   If it is a directory then a new block for the directory is created and entered into the block queue

   Params:
   block        The onsite block to be processed"

  ;; If the block is a file
  (if (.isDirectory (:file-dir block))
    (process-dir block)
    (process-file block))
  ;; after block has been processed remove it from block-data
  (dosync (alter bp-state update-in [:onsite-block-q] rest)))

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

(defn send-block
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

(defn broadcast
  "Send the split, encrypted offsite block out to offsite-nodes for remote backup.

   Params:
   block-state        DB state info for the block
   offsite-block      A block representing a file (or sub-block) ready for offsite backup"
  [block-state offsite-block]

  ;; this needs to be updated to send blocks to the offsite nodes in parallel
  (let [offsite-nodes (:offsite-nodes @bp-state)]
    (when offsite-nodes
      (loop [nodes         offsite-nodes
             best-response nil]
        (if-not nodes
          (db/update-ofs-block-state block-state best-response)
          (let [node      (first nodes)
                resp      (send-block node offsite-block)]
            (recur (rest nodes)
                   (if (<= (:duration resp) (:duration (or best-response resp)))
                     resp
                     best-response))))))))

(defn encrypt
  "Encrypt an offsite-block with the user's key. Then pass on to broadcast

   Params:
   block-state        DB state info for the block
   offsite-block      A block representing a file (or sub-block) ready for encryption"
  [block-state offsite-block]

  ;; add logic to encrypt the file block
  (broadcast block-state offsite-block))

(defn split
  "Splits an offsite file block into pieces no larger than max-block-size

   Params:
   block-state        DB state info for block
   offsite-block      A block representing a file ready for offsite backup"

  [block-state offsite-block]

  ;; add logic for breaking up the file and passing it off to encrypt
  (encrypt block-state offsite-block))

(defn backup-offsite
  "Define the process for prepping an offsite-block for backup and broadcasting the prepped
   offsite-block to the nodes provided by offsite-svc

   Params:
   offsite-block    The block representing a file or directory ready for offsite backup"
  [offsite-block]

  (let [offsite-block-info (make-block-info (:file-dir offsite-block))
        block-state        (or (db/get-ofs-block-state offsite-block-info)
                               (db/create-ofs-block-state offsite-block-info))]
    (split block-state offsite-block)))

(defn onsite-block-listener
  "Starts a thread which listens to the ::onsite-block-chan for new blocks which need
   to be processed"
  []

  (a/go
    (println "OnBL: in loop thread")

    (while (:started @bp-state)
      (println "OnBL: waiting for block" )
      (let [block (take! :onsite-block-chan)]
        (when-not (= stop-key block)
          (println (str "\n received block: " block))
          (process-block block)
          #_(fsm/send fsm-svc {:type :enqueued :block block}))))
    (println "OnBL: Closing ::onsite-block-chan.")
    (close! :onsite-block-chan)
    (println "OnBL: block-processor stopped")))

(defn offsite-block-listener
  "Starts a thread which listens to the offsite-block channel for blocks that are
   ready for offsite broadcast"
  []

  (a/go
    (println "OfBL: started loop thread for offsite-blocks")
    (while (:started @bp-state)
      (println "OfBL: waiting for next offsite block")
      (let [offsite-block (take! :offsite-block-chan)]
        (when-not (= stop-key offsite-block)
          (println (str "OfBL: received offsite block: " (:root-dir offsite-block)))
          (backup-offsite offsite-block))))))
;(defn add-block [backup-block]
;  "Add a backup block to the block-data queue
;
;  params:
;  backup-block     A block of data for backup"
;  (alter bp-data update-in :queue conj backup-block))



(defn start []
  "Start processing block-data from the queue"

  (when-not (:started @bp-state)
    (dosync (alter bp-state assoc-in [:started] true))
    (new-channel! :onsite-block-chan stop-key)
    (new-channel! :offsite-block-chan stop-key)
    (onsite-block-listener)
    (offsite-block-listener)))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @bp-state)
    (dosync (alter bp-state assoc :started false))
    (put! :onsite-block-chan stop-key)
    (put! :offsite-block-chan stop-key)))
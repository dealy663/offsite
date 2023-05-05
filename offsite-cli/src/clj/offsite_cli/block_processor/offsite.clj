(ns offsite-cli.block-processor.offsite
   ^{:author       "Derek Ealy <dealy663@gmail.com>"
     :date         "1/29/22"
     :organization "http://grandprixsw.com"
     :doc          "Processor for offsite blocks"
     :no-doc       true
     :project      "offsite"}
  (:require [offsite-cli.system-utils :as su]
            [offsite-cli.remote.os-service :as svc]
            [offsite-cli.block-processor.bp-core :as bpc]
            [offsite-cli.remote.os-node :as node]
            [clojure.java.io :as io]
            [offsite-cli.db.db-core :as db]
            [clojure.core.async :as a]
            [offsite-cli.channels :as ch]
            [byte-streams :as bs]
            [babashka.fs :as fs]
            [offsite-cli.init :as init])
   (:import (java.io ByteArrayOutputStream)))

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
   (let [offsite-nodes  (svc/request-nodes (su/get-client-info))
         {:keys [block-state offsite-block chunk prep-state]} prepped-block]
      (dosync (alter bpc/bp-state assoc :offsite-nodes offsite-nodes))
      (when offsite-nodes
         (loop [nodes         offsite-nodes
                best-response nil]
            (if-not nodes
               (update-ofs-block-state block-state best-response)
               (let [node      (first nodes)
                     resp      (node/send-block! node offsite-block)]
                  (recur (rest nodes)
                         (if (<= (:duration resp) (:duration (or best-response resp)))
                            resp
                            best-response))))))))

(declare encrypt-chunk)

(defn encryption-none
  "A simple pass-through function for early development. This does no encryption

  Params:
  prepped-block     A block prepped for encryption

  Returns the same block with no encryption applied"
  [prepped-block]

  (assoc prepped-block :encryption :none))

(defn encryption-aes
  "Encryption using AES

  Params:
  options           Encryption options
  prepped-block     A block prepped for encryption

  Returns the block of data after encryption"
  [options prepped-block]

  (let [{:keys [chunk]} prepped-block
        encrypted-chunk "NOT ENCRYPTED"]

    (assoc prepped-block :chunk encrypted-chunk :encryption :aes-options)))

(defn encryption-tink
  "Encryption using the Google Tink library

  Params:
  options          Tink configuration parameters
  prepped-block    A block prepped for encryption

  Returns the block of data after encryption"

  [options prepped-block]

  (let [{:keys [chunk]} prepped-block
        encrypted-chunk "NOT ENCRYPTED"]
    ;; tink commands
    (assoc prepped-block :chunk encrypt-chunk :encryption :tink-algo)))

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
    process-fn         Function to process and broadcast each chunk of split file
    algo-fn            (optional - default nil) Override default split logic

    Returns a partially prepared block with the file being split and the next chunk to be prepped"
   [prepped-block process-fn & algo-fn]

   (let [{:keys [offsite-block block-state input-stream]} prepped-block
         file (fs/file (:file-dir prepped-block))
         byte-buffers (bs/to-byte-buffers file {:chunk-size (:max-block-size init/client-state)})]
      ;; open file
      (with-open [output-stream   (ByteArrayOutputStream.)]
         ;; read max-chunk bytes into buffer
         (io/copy input-stream output-stream :buf-size bpc/max-block-chunk)
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
     (when-let [file-dir (:file-dir offsite-block)]
       (assoc prepped-block :input-stream (io/input-stream file-dir)
                            :prep-state   :opened))))

(defn process-block
  "Processing logic for sending a file's post split blocks offsite

  Params:
  sub-block     A block to finish prep for sending offsite"
  [block]

  (comp))

(defn onsite-block-handler
  "Converts an onsite block to an offsite block. The block is then broadcast to supporting nodes
  and the DB is updated.

  Params:
  onsite-block     An onsite block that has been retrieved from the backup catalog

  Returns the result of the DB update after the block has been broadcast"
  [onsite-block]

  (let [publish-msg (ch/gen-publisher :offsite-msg :onsite-block-handler {:log-level :debug})]
    (publish-msg (:root-path onsite-block))
    (when-let [file-dir (:file-dir onsite-block)]
      ;; probably don't need this iostream
      (with-open [in-stream (io/input-stream file-dir)]
        (let [offsite-block (assoc onsite-block :input-stream in-stream
                                                :prep-state :opened)
              process-block (comp db/easy-ingest!
                                  ;; probably need to create XTDB doc for the block before easy-ingest
                                  ;; also some bookkeeping statistics
                                  broadcast!
                                  #_(partial encryption-tink :aes)
                                  encryption-none)
              onsite-file    (fs/file file-dir)]
          (loop [split-bufs        (bs/to-byte-buffers onsite-file {:chunk-size (:max-block-size init/client-state)})
                 block-id          0
                 previous-block-id nil]
            (when-let [chunk (first split-bufs)]
              (let [block {:file-id           (:xt/id onsite-block)
                           :block-id          block-id
                           :previous-block-id previous-block-id
                           :chunk             chunk}]
                (process-block block)
                (recur (rest split-bufs) (inc block-id) block-id))))
          #_(-> offsite-block
              ;(get-block-state!)

              ;; remember that you must indicate the previous block in the backup chain, sub-node, parent dir, previous file etc
              ;;(prepare-offsite-block!)
              (split process-block)                                       ;; This is where a payload is added
              ;(encrypt)
              ;(broadcast!)
              #_(db/easy-ingest!))
          (publish-msg (str "exiting:\n"
                            {:message "Created offsite block"
                             :data    (:orig-path offsite-block)}) #{:exit}))))))

(defn offsite-block-listener
   "Starts a thread which listens to the offsite-block channel for blocks that are
    ready for offsite broadcast"
   ([offsite-block-handler-impl]

    (let [publish-msg (ch/gen-publisher :offsite-msg :offsite-block-listener {:log-level :debug})]
      (publish-msg "dispatching OfBL thread")
      (a/go
        (publish-msg "started loop thread for offsite-blocks")
        (while (:started @bpc/bp-state)
          (publish-msg "OfBL: waiting for next offsite block")
          #_(when-let [offsite-block (a/<! (ch/get-ch :offsite-block-chan))]
              (when-not (= bpc/stop-key offsite-block)
                (offsite-block-handler-impl offsite-block)))))))

   ([]
    (offsite-block-listener onsite-block-handler)))
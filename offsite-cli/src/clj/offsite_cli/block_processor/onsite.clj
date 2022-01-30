(ns offsite-cli.block-processor.onsite
   ^{:author       "Derek Ealy <dealy663@gmail.com>"
     :date         "1/29/22"
     :organization "http://grandprixsw.com"
     :doc          "block processor onsite functions"
     :no-doc       true
     :project      "offsite"}
   (:require [offsite-cli.system-utils :as su]
             [clojure.core.async :as a]
             [clojure.tools.logging :as log]
             [offsite-cli.channels :as ch]
             [offsite-cli.block-processor.bp-core :as bpc]
             [offsite-cli.block-processor.offsite :as bpof]
             [offsite-cli.collector.col-core :as col]
             [offsite-cli.db.db-core :as db]
             [mount.core :as mount]))

(declare start stop)
(mount/defstate block-processor-chans
                :start   (start)
                :stop    (stop))

(declare process-file)

(defn create-child-dir
   "Given a list of files and directories, filter for the directories and add them as paths to the DB for
    processing by the collector.

    Params:
    file-dir     A file object for a given directory"
   [file-dir ons-dir-parent]

   {:xt/id         (.hashCode file-dir)
    :path          (.getCanonicalPath file-dir)
    :backup-id     (:backup-id (col/get-backup-info))
    :data-type     :backup-path
    :ons-parent-id (:xt/id ons-dir-parent)})

(defn process-child-file
   "Creates on offsite block for a file and adds it to the queue for processing.

    Params:
    file             A Java.io.File to be backed up
    ons-dir-parent  ons-file-block for parent directory"
   [file ons-dir-parent]

   (let [ons-file-block (col/create-block {:path (.getCanonicalPath file)} ons-dir-parent)]
      (process-file ons-file-block)))

(defn process-dir
   "Processes an onsite block which represents a directory. The directory will have all of its
    children enqueued for block processing and finally the directory itself will be entered
    into the onsite DB and broadcast to nodes for offsite backup.

    Params:
    ons-dir-block     The onsite block representing a directory

    Returns an offsite block for a directory to be prepped for backup"
   [ons-dir-block]

   (let [file-dirs (.listFiles (:file-dir ons-dir-block))]
      (->> file-dirs
           (filter #(col/included? %))
           (mapv #(if (.isDirectory %)
                     (->> ons-dir-block
                          (create-child-dir %)
                          (db/easy-ingest!))
                     (->> ons-dir-block
                          (process-child-file %)
                          (fn [file]
                             (when-some [ofs file]
                                (ch/put! :offsite-block-chan ofs)))))))))

(defn process-file
   "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
    and broadcast to nodes for offsite backup.

    Params:
    ons-file-block     The offsite block representing a file

    Returns file-state map if the object is new or has changed and is to be backed up"
   [ons-file-block]

   (let [file-info  (bpc/make-block-info ons-file-block)
         _ (su/dbg "Got block info: " file-info)
         file-state (or (db/get-ofs-block-state! (:xt/id file-info))
                        (let [state (bpc/create-ofs-block-state file-info)]
                           (su/dbg "created-ofs-block-state: " (:first state))
                           (su/dbg "easy-ingest! output: " (db/easy-ingest! state))
                           (:first state)))]
      (su/dbg "process-file: fetched block-state - " file-state)
      (when (or (nil? (:version file-state))
                (> 1 (:version file-state))
                (not (= (:checksum file-info) (:checksum file-state))))
         file-state)))

(defn process-block
   "Handles an onsite block. If it refers to a file then it is read, disintegrated, encrypted and broadcast.
    If it is a directory then a new block for the directory is created and entered into the block queue

    Params:
    block        The onsite block to be processed

    Returns an empty offsite block ready to be prepped for backup"
   [block]

   (if (.isDirectory (:file-dir block))
      (process-dir block)
      (process-file block)))

(defn- onsite-block-handler
   "Overridable logic for handling onsite blocks to prep for cataloguing and backup

    Params:
    block          The file-dir block that is ready for processing"
   [block]

   (when-not (= bpc/stop-key block)
      (su/dbg "received block: " block)
      (->> block
           (process-block)
           (ch/put! :offsite-block-chan))))

(defn- path-block-handler
  "Overrideable logic for processing path-blocks that have been added to the DB for the current
   backup."
  [_]

  (with-open [path-seq-itr (db/get-path-blocks-lazy (:backup-id col/get-backup-info))]
    (doseq [path-block (iterator-seq path-seq-itr)]
      (su/dbg "received block: " path-block)
      (->> path-block
           (process-block)
           (ch/put! :offsite-block-chan)))))

(defn onsite-block-listener
   "Starts a thread which listens to the ::onsite-block-chan for new blocks which need
    to be processed

    Params:
    block-handler      (optional, default is (onsite-block-handler)), this is intended to be overridden in testing"
   ([block-handler]

    (a/go
       (su/dbg "OnBL: in loop thread")

       (loop [started (:started @bpc/bp-state)
              break false]
          (when-not (or break (not started))
             (su/dbg "OnBL: waiting for block")
             (let [block (a/<! (ch/get-ch :onsite-block-chan))]
                (if (nil? block)
                   (log/error "Retrieved nil from closed :onsite-block-channel, started: " started ", break: " break)
                   (do
                      (su/dbg "Took block off :onsite-block-chan, val: " block)
                      (block-handler block)
                      (recur (:started @bpc/bp-state) (= bpc/stop-key block))))))))
    (su/dbg "OnBL: Closing ::onsite-block-chan.")
    (ch/close! :onsite-block-chan)
    (su/dbg "OnBL: block-processor stopped"))

   ([]
    ;;(onsite-block-listener onsite-block-handler)
    (onsite-block-listener path-block-handler)))

(defn- start-default
   "Standard implementation of start, can be overridden in test by passing customized body"
   []

   (ch/new-channel! :onsite-block-chan bpc/stop-key)
   (ch/new-channel! :offsite-block-chan bpc/stop-key)
   (onsite-block-listener)
   (bpof/offsite-block-listener))

(defn start
   "Start processing block-data from the queue

    Params:
    start-impl        (optional - default is (start-default) can be overridden in tests"
   ([start-impl]

    (when-not (:started @bpc/bp-state)
       (su/dbg "Starting bp-core with impl fn: " start-impl)
       (dosync (alter bpc/bp-state assoc-in [:started] true))
       (start-impl)))

   ([]
    (start start-default)))

(defn- stop-default
   "Standard implementation of stop, can be overridden int est by passing customized body"
   []

   (a/go
      (a/>! (ch/get-ch :onsite-block-chan) bpc/stop-key)
      (a/>! (ch/get-ch :offsite-block-chan) bpc/stop-key)))

(defn stop
   "Stops the block-processor, will wait for all blocks in queue to be finished.

    Params:
    stop-impl          (optional - default is (stop-default) can be overridden in tests"
   ([stop-impl]

    (when (:started @bpc/bp-state)
       (dosync (alter bpc/bp-state assoc :started false))
       (stop-impl)))

   ([]

    (stop stop-default)))
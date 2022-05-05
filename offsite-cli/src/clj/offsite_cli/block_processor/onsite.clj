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
            [manifold.bus :as mb]
            [manifold.stream :as ms]
            [manifold.deferred :as md]
            [mount.core :as mount]
            [offsite-cli.db.catalog :as dbc]
            [manifold.go-off :as mg]
            [babashka.fs :as fs]))

(declare start stop)
(mount/defstate onsite-bp-state
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

   (let [ons-file-block (col/create-path-block {:path (.getCanonicalPath file)} ons-dir-parent)]
      (process-file ons-file-block)))

(defn process-dir
   "Processes an onsite block which represents a directory. The directory will have all of its
    children enqueued for block processing and finally the directory itself will be entered
    into the onsite DB and broadcast to nodes for offsite backup.

    Params:
    ons-dir-block     The onsite block representing a directory

    Returns an offsite block for a directory to be prepped for backup"
   [ons-dir-block]

   (su/dbg "process-dir: " (:orig-path ons-dir-block))
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
                                #_(ch/put! :offsite-block-chan ofs)))))))))

(defn process-file
   "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
    and broadcast to nodes for offsite backup.

    Params:
    ons-file-block     The offsite block representing a file

    Returns file-state map if the object is new or has changed and is to be backed up"
   [ons-file-block]

   (su/dbg "process-file: " (:orig-path ons-file-block))
   (let [file-info  (bpc/make-block-info ons-file-block)
         ;_ (su/dbg "Got block info: " file-info)
         file-state (or (db/get-ofs-block-state! (:xt/id file-info))
                        (let [state (bpc/create-ofs-block-state file-info)]
                           ;(su/dbg "created-ofs-block-state: " (:first state))
                           ;(su/dbg "easy-ingest! output: " (db/easy-ingest! state))
                           (:first state)))]
     ;(su/dbg "process-file: fetched block-state - " file-state)
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

  (let [block     (assoc block :state :onsite)
        ons-block (assoc block :file-dir (fs/file (:orig-path block)))]
    (try
      ;(su/dbg "process-block: handing block off to offsite processor " block)
      ;(su/dbg "process-block: easy-ingest! -> " (db/easy-ingest! [block]))
      (db/easy-ingest! [block])
      (swap! bpc/bp-counters update :onsite-block-count inc)
      (bpof/onsite-block-handler ons-block)
      (catch Throwable e (str "Caught exception while processing onsite block: " (.getMessage e))))
    #_(if (.isDirectory (:file-dir ons-block))
      (process-dir ons-block)
      (process-file ons-block))))

(defn- path-block-handler
  "Overrideable logic for processing path-blocks that have been added to the DB for the current
   backup. This function will process all path-blocks currently in the catalog and then will
   check to see if any more have been added while it was working. If there are more to process
   then the function calls itself again."
  ;; for now just making this callable with an ignored parameter or nothing at all
  ([]
   (path-block-handler nil))

  ([deferred]
   (su/dbg "path-block-handler: starting")
   (a/go-loop [opp (-> @bpc/bp-state :onsite-path-processor first)]
     (when opp
      (dosync (alter bpc/bp-state update :onsite-path-processor rest))
      (with-open [path-seq-itr (dbc/get-path-blocks-lazy (:backup-id (col/get-backup-info)) {:state :catalog})]
        (doseq [path-block (iterator-seq path-seq-itr)]
          (su/dbg "path-block-handler: " (first path-block))
          (->> path-block
               first
               (process-block))))
      (recur (-> @bpc/bp-state :onsite-path-processor first))))
   (md/success! deferred true)
   #_(dosync
     (when (:catalog-update @bpc/bp-state)
       (alter bpc/bp-state assoc
              :catalog-update nil
              :onsite-path-processor (md/future (path-block-handler)))))
   (su/dbg "path-block-handler: exiting")))

(defn- path-block-handler2
  [buf-stream]

  (mg/go-off
    (let [msg (mg/<!? buf-stream)]
      (with-open [path-seq-itr (dbc/get-path-blocks-lazy (:backup-id (col/get-backup-info)) {:state :catalog})]
        (doseq [path-block (iterator-seq path-seq-itr)]
          (su/dbg "path-block-handler: " (first path-block))
          (->> path-block
               first
               (process-block)))))))

(defn catalog-block-handler
  "Monitor function for onsite-block messages from the collector"
  [cat-stream buf-stream]

  (mg/go-off
    (loop [msg (mg/<!? cat-stream)]
      (su/dbg "cbh: got cat msg: " msg)
      (when msg
        (let [p (ms/try-put! buf-stream msg 0 ::timedout)]
          (su/dbg "cbh: put realized? - " (md/realized? p) ", val: " @p)
          (if @p
            (su/dbg "cbh: put msg on buf-stream: " msg)
            (su/dbg "cbh: dropped msg buf-stream full. " msg))            ;; only put onto buf-stream if it isn't full
        (recur (mg/<!? cat-stream)))))
  buf-stream
  #_(dosync
    (let [path-processor-futures (:onsite-path-processor @bpc/bp-state)]
      (if (< (count path-processor-futures) 2)
        (alter bpc/bp-state update :onsite-path-processor (conj (md/deferred (path-block-handler))))
        nil)))))

(defn root-catalog-block-handler
  ""
  [msg]

  (let [{:keys [path-block dir-info-atom]} msg]
    (su/dbg "root-catalog-block-handler: " path-block)
    (dosync (alter bpc/bp-state assoc :catalog-update path-block))))

#_(defn root-path-event-handler
  "Handles notifications for new root-paths that are ready for processing.

   Params:
   stream     A manifold stream that has subscribed to the event-bus for :root-path messages
   handler-fn (optional - default onsite-block-monitor) Overridable function to handle the root-path msg"
  ([stream]
   (root-path-event-handler stream catalog-block-handler))

  ([stream handler-fn]

   (mg/go-off
     (su/dbg "setting root-path-handler loop")
     (loop []
       (when-let [deferred-root (mg/<!? stream)]
         (su/dbg "got deferred-root: " deferred-root)
         (handler-fn deferred-root)
         (recur))))))

(defn- start-default
   "Standard implementation of start, can be overridden in test by passing customized body"
   []

  ;  (ch/m-subscribe :root-path)
  (ch/m-sub-monitor :root-path root-catalog-block-handler)
  (let [cat-stream (ch/m-subscribe :catalog-add-block)]
    (->> (ms/stream 2)
         (catalog-block-handler cat-stream)
         (path-block-handler2)))
  #_(-> (:bus @ch/channels)
      (mb/subscribe :root-path)
      (root-path-event-handler)))

(defn start
  "Start processing block-data from the queue

   Params:
   start-impl        (optional - default is (start-default) can be overridden in tests"
  ([]
   (start start-default))

  ([start-impl]

   (when-not (:started @bpc/bp-state)
     (su/dbg "Starting bp-onsite with impl fn: " start-impl)
     ;     (bpc/bp-reset! {:started true :onsite-path-processor nil})
     (bpc/start)
     ;     (dosync (alter bpc/bp-state assoc :started true :onsite-path-processor nil))
     (start-impl))))

(defn- stop-default
   "Standard implementation of stop, can be overridden int est by passing customized body"
   []

   #_(a/go
      (a/>! (ch/get-ch :onsite-block-chan) bpc/stop-key)
      (a/>! (ch/get-ch :offsite-block-chan) bpc/stop-key)))

(defn stop
   "Stops the block-processor, will wait for all blocks in queue to be finished.

    Params:
    stop-impl          (optional - default is (stop-default) can be overridden in tests"
  ([]

   (stop stop-default))

  ([stop-impl]

    (when (:started @bpc/bp-state)
       (dosync (alter bpc/bp-state assoc :started false))
       (stop-impl))))
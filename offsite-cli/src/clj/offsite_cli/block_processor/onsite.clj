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
            [babashka.fs :as fs]
            [offsite-cli.init :as init]))

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

   (su/debug "process-dir: " (:orig-path ons-dir-block))
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

   (su/debug "process-file: " (:orig-path ons-file-block))
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

#_(defn- path-block-handler
  "Overrideable logic for processing path-blocks that have been added to the DB for the current
   backup. This function will process all path-blocks currently in the catalog and then will
   check to see if any more have been added while it was working. If there are more to process
   then the function calls itself again."
  ;; for now just making this callable with an ignored parameter or nothing at all
  ([]
   (path-block-handler nil))

  ([deferred]
   (su/debug "path-block-handler: starting")
   (a/go-loop [opp (-> @bpc/bp-state :onsite-path-processor first)]
     (when opp
      (dosync (alter bpc/bp-state update :onsite-path-processor rest))
      (with-open [path-seq-itr (dbc/get-path-blocks-lazy (:backup-id (col/get-backup-info)) {:state :catalog})]
        (doseq [path-block (iterator-seq path-seq-itr)]
          (su/debug "path-block-handler: " (first path-block))
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
   (su/debug "path-block-handler: exiting")))

(defn- path-block-handler2
  [buf-stream]

  (let [publish-msg (ch/gen-publisher :onsite-msg :path-block-handler2 {:log-level :debug})]
    (mg/go-off
      (publish-msg "start")
      (loop [msg (mg/<!? buf-stream)
             blocks-handled 0]
        (if msg
          (do
            (with-open [path-seq-itr (dbc/get-path-blocks-lazy (:backup-id (col/get-backup-info)) {:state :catalog})]
              (doseq [path-block (iterator-seq path-seq-itr)]
                (publish-msg (-> path-block first :orig-path))
                (ch/m-publish :msg {:event-type :path-block-handler2
                                    :payload    (-> path-block first :orig-path)})
                (->> path-block
                     first
                     (process-block))))
            (recur (mg/<!? buf-stream) (inc blocks-handled)))
          (publish-msg (str "exit, blocks-handled " blocks-handled)))))))

(defn catalog-block-handler
  "Monitor function for onsite-block messages from the collector, transfers the message to
  buf-stream as long as buf-stream isn't full, otherwise drops message.

  Params:
  cat-stream     Input stream of catalog messages
  buf-stream     Output stream buffer which should have a buf size of 2

  Returns the output stream"
  [cat-stream buf-stream]

  (let [publish-msg (ch/gen-publisher :onsite-msg :catalog-block-handler)]
    (mg/go-off
      (loop [msg (mg/<!? cat-stream)]
        ;(su/dbg "cbh: got cat msg: " msg)
        (if msg
          (let [p (ms/try-put! buf-stream msg 0 ::timedout)]
            ;          (su/dbg "cbh: put realized? - " (md/realized? p) ", val: " @p)
            (if @p
              (publish-msg (str "put msg on buf-stream: " msg))
              (publish-msg (str "dropped msg buf-stream full. " msg))) ;; only put onto buf-stream if it isn't full
            (recur (mg/<!? cat-stream)))
          (do
            (ms/close! buf-stream)
            (log/info "catalog-block-handler: exit"))))))
  buf-stream)

(defn root-catalog-block-handler
  ""
  [msg]

  (let [publish-msg (ch/gen-publisher :onsite-msg :root-catalog-block-handler {:log-level :info})
        payload (su/payload-gen :root-catalog-block-handler)
        {:keys [path-block dir-info-atom]} msg]
    (publish-msg path-block)
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
     (su/debug "setting root-path-handler loop")
     (loop []
       (when-let [deferred-root (mg/<!? stream)]
         (su/debug "got deferred-root: " deferred-root)
         (handler-fn deferred-root)
         (recur))))))

(defn- start-default
   "Standard implementation of start, can be overridden in test by passing customized body"
   []

  (ch/m-sub-monitor :root-path root-catalog-block-handler)
  (let [cat-stream (ch/m-subscribe :catalog-add-block)
        out-stream (ms/stream 2)]
    (catalog-block-handler cat-stream out-stream)
    (path-block-handler2 out-stream)))

(defn start
  "Start processing block-data from the queue

   Params:
   start-impl        (optional - default is (start-default) can be overridden in tests"
  ([]
   (start start-default))

  ([start-impl]

   (let [publish-msg (ch/gen-publisher :onsite-msg :start)]
     (when-not (:started @bpc/bp-state)
       (publish-msg (str "Starting bp-onsite with impl fn: " start-impl))
       (bpc/start)
       (start-impl)))))

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

   (dosync
     (when (:started @bpc/bp-state)
       (alter bpc/bp-state assoc :started false)
       (stop-impl)))))
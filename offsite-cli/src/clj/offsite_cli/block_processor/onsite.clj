(ns offsite-cli.block-processor.onsite
   ^{:author       "Derek Ealy <dealy663@gmail.com>"
     :date         "1/29/22"
     :organization "http://grandprixsw.com"
     :doc          "block processor onsite functions"
     :no-doc       true
     :project      "offsite"}
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [offsite-cli.channels :as ch]
            [offsite-cli.block-processor.bp-core :as bpc]
            [offsite-cli.block-processor.offsite :as bpof]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.db.db-core :as db]
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

;5/15/22
;(defn process-child-file
;   "Creates on offsite block for a file and adds it to the queue for processing.
;
;    Params:
;    file             A Java.io.File to be backed up
;    ons-dir-parent  ons-file-block for parent directory"
;   [file ons-dir-parent]
;
;   (let [ons-file-block (col/create-path-block {:path (.getCanonicalPath file)} ons-dir-parent)]
;      (process-file ons-file-block)))

;5/15/22
;(defn process-dir
;   "Processes an onsite block which represents a directory. The directory will have all of its
;    children enqueued for block processing and finally the directory itself will be entered
;    into the onsite DB and broadcast to nodes for offsite backup.
;
;    Params:
;    ons-dir-block     The onsite block representing a directory
;
;    Returns an offsite block for a directory to be prepped for backup"
;   [ons-dir-block]
;
;   (su/debug "process-dir: " (:orig-path ons-dir-block))
;   (let [file-dirs (.listFiles (:file-dir ons-dir-block))]
;      (->> file-dirs
;           (filter #(col/included? %))
;           (mapv #(if (.isDirectory %)
;                     (->> ons-dir-block
;                          (create-child-dir %)
;                          (db/easy-ingest!))
;                     (->> ons-dir-block
;                          (process-child-file %)
;                          (fn [file]
;                             (when-some [ofs file]
;                                #_(ch/put! :offsite-block-chan ofs)))))))))

(defn process-file
   "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
    and broadcast to nodes for offsite backup.

    Params:
    ons-file-block     The onsite block representing a file

    Returns file-state map if the object is new or has changed and is to be backed up"
   [ons-file-block]

  (let [publish-msg  (ch/gen-publisher :onsite-msg :process-file)
        file-info     (bpc/make-block-info ons-file-block)
        _            (publish-msg (str "process-file: " (:orig-path ons-file-block)))
        file-state    (or (db/get-ofs-block-state! (:xt/id file-info))
                        (let [state (bpc/create-ofs-block-state file-info)]
                           (publish-msg (str "created-ofs-block-state: " (:first state)))
                           (:first state)))]
    (when (or (nil? (:version file-state))
              (> 1 (:version file-state))
              (not (= (:checksum file-info) (:checksum file-state))))
      file-state)))

(defn process-block
  "Handles an onsite block and passes it off to the offsite processor. If it refers to a file then it is read,
  disintegrated, encrypted and broadcast.
  If it is a directory then a new block for the directory is created and entered into the block queue

   Params:
   block        The onsite block to be processed

   Returns an empty offsite block ready to be prepped for backup"
  [block]

  (let [publish-msg (ch/gen-publisher :onsite-msg :process-block)
        block       (assoc block :state :onsite)
        ons-block   (assoc block :file-dir (fs/file (:orig-path block)))]
    (try
      (publish-msg (str "process-block: handing block off to offsite processor " block))
      (db/easy-ingest! [block])
      (swap! bpc/bp-counters update :onsite-block-count inc)
      (bpof/onsite-block-handler ons-block)
      (catch Throwable e (str "Caught exception while processing onsite block: " (.getMessage e))))))

(defn- path-block-handler2
  "Start a thread for processing all the catalog path blocks currently in the DB belonging to the
  current backup.

  Returns a deferred which will have the value set to a map with the count of the blocks processed"
  [buf-stream]

  (let [publish-msg (ch/gen-publisher :onsite-msg :path-block-handler2 {:log-level :debug})
        result-d    (md/deferred)]
    (mg/go-off
      (publish-msg "start")
      (loop [msg (mg/<!? buf-stream)
             blocks-handled 0]
        (if-not msg
          (do
            (md/success! result-d {:catalog-blocks-handled blocks-handled})
            (publish-msg (str "exit, blocks-handled " blocks-handled)))
          (do
            (with-open [path-seq-itr (dbc/get-path-blocks-lazy (:backup-id (col/get-backup-info)) {:state :catalog})]
              (doseq [path-block (iterator-seq path-seq-itr)]
                (publish-msg (-> path-block first :orig-path))
                ;; this second publish is probably not necessary
                (ch/m-publish :msg {:event-type :path-block-handler2
                                    :payload    (-> path-block first :orig-path)})
                (->> path-block
                     first
                     (process-block))))
            (recur (mg/<!? buf-stream) (inc blocks-handled))))))))

(defn catalog-block-handler
  "Monitor function for onsite-block messages from the collector, transfers the message to
  buf-stream as long as buf-stream isn't full, otherwise drops message. This action just queues up
  another notification to process more catalog blocks from DB, but there is no need to queue more than one
  since the catalog block query will pull all at the next startup.

  Params:
  cat-stream     Input stream of catalog messages
  buf-stream     Output stream buffer which should have a buf size of 2

  Returns the output stream"
  [cat-stream buf-stream]

  (let [publish-msg (ch/gen-publisher :onsite-msg :catalog-block-handler)]
    (mg/go-off
      (loop [msg (mg/<!? cat-stream)]
        (if-not msg
          (do
            (ms/close! buf-stream)
            (log/info "catalog-block-handler: exit"))
          (let [p (ms/try-put! buf-stream msg 0 ::timedout)]
            (if @p
              (publish-msg (str "put msg on buf-stream: " msg))
              (publish-msg (str "dropped msg buf-stream full. " msg))) ;; only put onto buf-stream if it isn't full
            (recur (mg/<!? cat-stream)))))))
  buf-stream)

; 5/15/22
;(defn root-catalog-block-handler
;  "Handles root paths from the cataloging operation of the collector."
;  [msg]
;
;  (let [publish-msg (ch/gen-publisher :onsite-msg :root-catalog-block-handler {:log-level :debug})
;        {:keys [path-block dir-info-atom]} msg]
;    (publish-msg path-block)
;    (dosync (alter bpc/bp-state assoc :catalog-update path-block))))

(defn- start-default
   "Standard implementation of start, can be overridden in test by passing customized body. This function
   will create a buffer between incoming root catalog path block events and a processor, only queueing the
   event indicating to launch another handler process if one isn't already in the queue waiting to launch."
   []

  ; 5/15/22
  ;  (ch/m-sub-monitor :root-path root-catalog-block-handler)
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
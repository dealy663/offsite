(ns offsite-cli.collector.col-core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [offsite-cli.channels :refer :all]
            [mount.core :as mount]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.db.catalog :as dbc]
            [offsite-cli.channels :as ch]
            [clojure.tools.logging :as log]
            [offsite-cli.system-utils :as su]
            [clojure.string :as str]
            [babashka.fs :as fs]
            [manifold.bus :as mb]
            [manifold.stream :as ms]
            [manifold.go-off :as mg]
            [offsite-cli.init :as init]))

(def stop-key :stop-collector)

;; collector-state A ref representing the current state of the collector
(def collector-state
  "The current state of the Collector"
  (ref {:files            []
                           :started         false
                           :backup-count    0
                           :push-count      0
                           :total-bytes     0
                           :backup-paths    []
                           :catalog-state   nil
                           :push-bytes      0}))

(def events #{:col-progress :col-complete})
(declare start stop get-backup-info)
(swap! init/client-state update :service-keys conj :collector)

(mount/defstate collector-chans
  :start (do
           (su/debug "starting col-core"))
  :stop (stop))

(defn create-path-block
  "Create an onsite backup block

  params:
  file-dir          A backup path
  parent-block-id  (optional - default nil) The ID of the ons parent block

  returns:     A backup block"
  ([file-dir]
   (create-path-block file-dir nil))

  ([file-dir parent-block-id]
   (let [file-dir-path (if (string? file-dir) (io/file file-dir) file-dir)]
     {:xt/id     (.hashCode file-dir-path)
      :root-path (.getCanonicalPath file-dir-path)
      :orig-path (.getPath file-dir-path)
      :data-type :path-block
      :state     :catalog
      :backup-id (:backup-id (get-backup-info))
      ;:file-dir
      :parent-id (if (some? parent-block-id) parent-block-id)
      :size      (if (.isDirectory file-dir-path) 0 (.length file-dir-path))})))

;(defn start [backup-paths]
;  "Start processing the files in the backup paths, cataloguing current state, changed files
;   and passing them off to the file-processor for backup
;
;   params:
;   backup-paths    A sequence of paths to recurse through containing the dirs to backup"
;  (doseq [path-def backup-paths]
;    (create-block path-def)))

(defn path-processor
  "Processes paths from the :path-chan in its own thread"
  []

  #_(a/go-loop []
    (when-some [path (ch/take! :path-chan)]
      )))


(defn included?
  "Returns true if the file/dir path string is not in an exclusion list. This function expects that
   the file/dir path is fully qualified from the root of the filesystem.

   Params:
   file-dir       The directory to test for inclusion in backup
   exclusions    (optional - default @su/paths-config) These will be created by the init

   Returns true if the file/dir path is not in the exclusion list"
  ([file-dir]
   (included? file-dir (:exclusions @su/paths-config)))

  ([file-dir exclusions]

   (let [file-dir (if (and (string? file-dir) (empty? file-dir)) nil file-dir)]
     (if file-dir
       (let [excl-match (some #(% file-dir) exclusions)]
         (not excl-match))
       false))))

(defn- pre-visit-dir-fn
  "Creates a function that babashka/fs will use when walking a file path. This function will be called
  before visiting a directory. The returned function takes two parameters and returns a keyword indicating
  whether to :continue or :skip-subtree if processing this path is to stop.

  Params:
  dir-info-atom    A data structure within an atom that has details about the path being processed

  Returns (fn [dir attrs] ...)"
  [dir-info-atom]

  (fn
    [dir attrs]
    (let [publish-msg (ch/gen-publisher :collector-msg :pre-visit-dir-fn)
          dir-file     (.toFile dir)
          parent-id   (-> @dir-info-atom :parent-ids first)]
      (if (included? dir)
        (let [path-block (create-path-block dir-file parent-id)]
          (dbc/add-path-block! path-block)
          (swap! dir-info-atom update :parent-ids conj (:xt/id path-block))
          (swap! dir-info-atom update :dir-count inc)
          (when (nil? parent-id)
            (publish-msg (str "pre-visit-dir-fn: publishing root-path: " path-block))
            (ch/m-publish :root-path {:path-block path-block :dir-info-atom dir-info-atom}))
          (ch/m-publish :catalog-add-block (:orig-path path-block))
          (ch/m-publish :col-progress (dissoc @dir-info-atom :parent-ids))
          :continue)
        (do
          (ch/m-publish :walk-tree [:excluded-dir dir])
          :skip-subtree)))))

(defn- post-visit-dir-fn
  "Creates a function that babashka/fs will use when walking a dir path. This function will be called
  after visiting a directory. The returned function takes the dir as the first param and an exception as the
  second, the exception will be rethrown if it is not nil.

  Params:
  dir-info-atom    A data structure within an atom that has details about the path being processed

  Returns (fn [_ exp] ...)"
  [dir-info-atom]

  (fn [_ exp]
    (if (nil? exp)
      (do
        (swap! dir-info-atom update :parent-ids rest)
        :continue)
      (throw exp))))

(defn- visit-file-fn
  "Creates a function that babshka/fs will use when walking a file path. The function will be called
  with the file that is being visited by babashka/fs. The returned function takes the dir as the first
  param and attributes as the second, it will return a keyword indicating whether babashka/fs is to :continue
  or :skip-subtree if processing should go no further down this path.

  Params:
  dir-info-atom     A data structure within an atom that has details about the path being processed.

  Returns (fn [path attrs] ...)"
  [dir-info-atom]

  (fn [path attrs]
    (let [publish-msg  (ch/gen-publisher :collector-msg :visit-file-fn)
          file         (.toFile path)
          file-path    (.getCanonicalPath file)]
      (publish-msg (str "visit-file got path: " path))
      (if (included? path)
        (let [parent-id  (-> @dir-info-atom :parent-ids first)
              path-block (create-path-block file parent-id)]
          (dbc/add-path-block! path-block)
          (swap! dir-info-atom update :byte-count + (fs/size file))
          (swap! dir-info-atom update :file-count inc)

          (when (nil? parent-id)
            (publish-msg (str "visit-file-fn: publishing root-path: " path-block))
            (ch/m-publish :root-path {:path-block path-block :dir-info-atom dir-info-atom}))
          (publish-msg "visit-file-fn: publishing :catalog-add-block")
          (ch/m-publish :catalog-add-block (:orig-path path-block))
          :continue)
        (do
          (ch/m-publish :walk-tree [:excluded-file path])
          :skip-subtree)))))

(defn walk-paths
  "Go through all the paths defined as part of this backup and build a catalog in the DB, while making sure to
  obey the exclusion rules that have been defined in the backup config EDN file.

  Params:
  root-dir         The root path to directory or maybe just a file to be backed up.

  Returns a map of backup info with the number of files, directories an bytes that will make up the backup."
  [root-dir]

  (let [root-file-dir  (if (string? root-dir) (fs/file root-dir) root-dir)
        path-info-atom (atom {:parent-ids '() :file-count 0 :dir-count 0 :byte-count 0})
        pre-visit-dir  (pre-visit-dir-fn path-info-atom)
        post-visit-dir (post-visit-dir-fn path-info-atom)
        visit-file     (visit-file-fn path-info-atom)]
    (fs/walk-file-tree root-file-dir {:pre-visit-dir pre-visit-dir :post-visit-dir post-visit-dir :visit-file visit-file})
    (ch/m-publish :walk-tree :complete)

    @path-info-atom))

(defn get-backup-info
  "Returns map of details regarding this backup"
  []

  (:backup-info @collector-state))


(defn event-monitor
  "Handles collector events.

   Params:
   stream     A manifold stream that has subscribed to the event-bus for :root-path messages
   handler-fn Function to handle the root-path msg"
  [stream handler-fn]

  (mg/go-off
    (loop []
      (when-let [deferred-event (mg/<!? stream)]
        (handler-fn @deferred-event)
        (recur)))))

(defn- close-collector-channels
  ""
  []

  (let [bus (:bus @ch/channels)]
    (mapv ms/close! (mb/downstream bus :root-path))
    (mapv ms/close! (mb/downstream bus :catalog-add-block))
    (mapv ms/close! (mb/downstream bus :col-msg))))

(defn start
  "Start process to wait for new paths from which to create onsite blocks.

   Params:
   backup-root-paths     A sequence of backup path definitions (maps) and exclusions
   progress-callback     (optional - default is nil) A function to call for progress updates during backup"
  ([backup-root-paths]
   (start backup-root-paths nil))

  ([backup-root-paths progress-callback]
   (let [publish-msg (ch/gen-publisher :col-msg :start)]
     (when-not (:started @collector-state)
       (publish-msg (str "Starting collector, root paths: " backup-root-paths))
       (swap! init/client-state assoc :catalog-state :started)
       (when-not (nil? (:backup-paths @collector-state))
         (dosync (alter collector-state assoc :backup-paths nil :total-bytes 0)))

       (dosync (alter collector-state update-in [:backup-count] inc))
       (try
         (let [backup-info (db/start-backup! backup-root-paths :adhoc)]
           (dosync (alter collector-state assoc :started true :backup-info backup-info))
           (loop [root-paths backup-root-paths
                  acc {:dir-count  0
                       :file-count 0
                       :byte-count 0}]
             (if-let [path-def (first root-paths)]
               (do
                 (let [path-block (create-path-block (:path path-def))
                       root-path (:root-path path-block)
                       {:keys [dir-count file-count byte-count :as result]} (walk-paths root-path)]
                   (dosync
                     (alter collector-state update :backup-paths conj path-def)
                     (alter collector-state update :total-bytes + byte-count))
                   (publish-msg (str "path: " root-path " dir-count: " dir-count " file-count: " file-count
                                     " byte-count: " byte-count))
                   (recur (rest root-paths)
                          (assoc acc :dir-count (+ dir-count (:dir-count acc))
                                     :file-count (+ file-count (:file-count acc))
                                     :byte-count (+ byte-count (:byte-count acc))))))
               (ch/m-publish :col-finished acc)))
           (close-collector-channels)
           (dbc/catalog-complete! (:backup-id backup-info)))
         (catch Exception e
           (log/error "Collector stopped with exception: " (.getMessage e))
           (.printStackTrace e)))))))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @collector-state)
    (println "Collector: stopping")

    (dosync (alter collector-state assoc :started false))
    (db/stop-backup! "Stopped from collector.")))

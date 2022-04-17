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
            [manifold.stream :as ms]))

(def stop-key :stop-collector)

(def collector-state (ref {:files         []
                           :started      false
                           :backup-count 0
                           :push-count   0
                           :total-bytes  0
                           :backup-paths []
                           :push-bytes   0}))

(def events #{:col-progress :col-complete})
(declare start stop get-backup-info)

(mount/defstate collector-chans
  :start (do
           (su/dbg "starting col-core")
           ;(new-channel! :path-chan stop-key)
           #_(start))
  :stop (stop))

(defn create-path-block
  "Create a backup block

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
  [dir-info-atom]

  (fn [dir attrs]
    ;(su/dbg "pre-visit-dir got dir: " dir)
    (let [dir-file   (.toFile dir)
          ;dir-path  (-> dir .toFile .getCanonicalPath)
          parent-id (-> @dir-info-atom :parent-ids first)]
      (if (included? dir)
        (let [path-block (create-path-block dir-file parent-id)]
          (dbc/add-path-block! path-block)
          (swap! dir-info-atom update :parent-ids conj (:xt/id path-block))
          (swap! dir-info-atom update :dir-count inc)
          (when (nil? parent-id)
            (ch/m-publish :root-path path-block))
          (ch/m-publish :col-progress (dissoc @dir-info-atom :parent-ids))
          :continue)
        (do
          (ch/m-publish :walk-tree [:excluded-dir dir])
          :skip-subtree)))))

(defn- post-visit-dir-fn
  [dir-info-atom]

  (fn [_ exp]
    (if (nil? exp)
      (do
        (swap! dir-info-atom update :parent-ids rest)
        :continue)
      (throw exp))))

(defn- visit-file-fn
  [dir-info-atom]

  (fn [path attrs]
    ;(su/dbg "vist-file got path: " path)
    (let [file (.toFile path)
          file-path (.getCanonicalPath file)]
      (if (included? path)
        (let [path-block (create-path-block file (-> @dir-info-atom :parent-ids first))]
          (dbc/add-path-block! path-block)
          (swap! dir-info-atom update :byte-count + (fs/size file))
          (swap! dir-info-atom update :file-count inc)
          :continue)
        (do
          (ch/m-publish :walk-tree [:excluded-file path])
          :skip-subtree)))))

(defn walk-paths
  "Go through all of the paths defined as part of this backup and build a catalog in the DB, while making sure to
  obey the exclusion rules that have been defined in the backup config EDN file.

  Params:
  root-dir         The root path to directory or maybe just a file to be backed up.

  Returns a map of backup info with the number of files, directories an bytes that will make up the backup."
  [root-dir]

  (let [root-file-dir   (if (string? root-dir) (fs/file root-dir) root-dir)
        path-info-atom (atom {:parent-ids '() :file-count 0 :dir-count 0 :byte-count 0})
        pre-visit-dir  (pre-visit-dir-fn path-info-atom)
        post-visit-dir (post-visit-dir-fn path-info-atom)
        visit-file      (visit-file-fn path-info-atom)]
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
  ;([stream]
  ; (col-event-monitor stream onsite-block-monitor))

  [stream handler-fn]

  (a/go-loop []
    (when-let [deferred-event (ms/take! stream)]
      (handler-fn @deferred-event)
      (recur))))

(defn start
  "Start process to wait for new paths from which to create onsite blocks.

   Params:
   backup-root-paths     A sequence of backup path definitions (maps) and exclusions
   progress-callback     (optional - default is nil) A function to call for progress updates during backup"
  ([backup-root-paths]
   (start backup-root-paths nil))

  ([backup-root-paths progress-callback]
   #_(su/dbg "Starting Collector started: " (:started @collector-state))
   (when-not (:started @collector-state)
     (ch/m-publish :col-msg (str "Starting collector, root paths: " backup-root-paths))
     (when-not (nil? (:backup-paths @collector-state))
       (dosync (alter collector-state assoc :backup-paths nil :total-bytes 0)))

     (dosync (alter collector-state update-in [:backup-count] inc))
     (try
       (let [backup-info (db/start-backup! backup-root-paths :adhoc)]
         (dosync (alter collector-state assoc :started true :backup-info backup-info))
         (loop [root-paths backup-root-paths
                acc {:dir-count   0
                     :file-count   0
                     :byte-count  0}]
           (if-let [path-def (first root-paths)]
             (do
               ;               (dosync (alter collector-state update-in [:backup-paths] conj path-def))
               (let [path-block (create-path-block (:path path-def))
                     root-path (:root-path path-block)
                     {:keys [dir-count file-count byte-count :as result]} (walk-paths root-path)]
                 (dosync
                   (alter collector-state update :backup-paths conj path-def)
                   (alter collector-state update :total-bytes + byte-count))
                 ;(su/dbg "path: " root-path " dir-count: " dir-count " file-count: " file-count " byte-count: " byte-count)
                 (recur (rest root-paths)
                        (assoc acc :dir-count  (+ dir-count  (:dir-count acc))
                                   :file-count  (+ file-count  (:file-count acc))
                                   :byte-count (+ byte-count (:byte-count acc))))))
             (ch/m-publish :col-finished acc)))
         (dbc/catalog-complete! (:backup-id backup-info)))
       (catch Exception e
         (log/error "Collector stopped with exception: " (.getMessage e))
         (.printStackTrace e))))))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @collector-state)
    (println "Collector: stopping")

    (dosync (alter collector-state assoc :started false))
    #_(put! :path-chan stop-key)
    (db/stop-backup! "Stopped from collector.")))

(ns offsite-cli.collector.col-core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [offsite-cli.channels :refer :all]
            [mount.core :as mount]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.channels :as ch]
            [clojure.tools.logging :as log]
            [offsite-cli.system-utils :as su]
            [clojure.string :as str]
            [babashka.fs :as fs]))

(def stop-key :stop-collector)

(def collector-state (ref {:files         []
                           :started      false
                           :backup-count 0
                           :push-count   0
                           :total-bytes  0
                           :backup-paths []
                           :push-bytes   0}))

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
  file-dir       A backup path
  parent-block  (optional - default nil) The ID of the ons parent block

  returns:     A backup block"
  ([file-dir]
   (create-path-block file-dir nil))

  ([file-dir parent-block]
   {:xt/id     (.hashCode file-dir)
    :root-path (.getCanonicalPath file-dir)
    :orig-path (.getPath file-dir)
    :data-type :path-block
    :backup-id (:backup-id (get-backup-info))
    ;:file-dir
    :parent-id (if (some? parent-block) (:xt/id parent-block))
    :size      (if (.isDirectory file-dir) 0 (.length file-dir))}))

(defn create-root-path-block
  "Create a backup block

  params:
  path-defs     A backup path, with possible exclusions
  parent-block  (optional - default nil) The ID of the ons parent block

  returns:     A backup block"
  ([{:keys [path exclusions] :as path-defs} parent-block]

   ;; I'm still of mixed opinions about whether to store an actual file object vs a path string
   ;; here. It looks like a valid file object can be written to XTDB, staying with path string for now
   (let [file-dir (io/file path)]
     (create-path-block file-dir parent-block)))

  ([path-defs]
   (create-root-path-block path-defs nil)))

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
   file-dir-str

   Returns true if the file/dir path is not in the exclusion list"
  [file-dir-str]

  ;; logic will need to be smart enough to figure out relative exclusion paths and
  ;; wildcard designators
  ;;
  ;; babashka/fs has some globbing facilities that will need to be explored
  ;;
  ;; always returns true for now

  (when-not (str/blank? file-dir-str)
    (let [exclusions (:exclusions @su/paths-config)
          excl-match (or
                       (some #{file-dir-str} exclusions)
                       (some #(do
                                ;(su/dbg "comparing excl: " % " with file-dir-path: " file-dir-path)
                                (str/starts-with? file-dir-str %)) exclusions))]
      (not excl-match))))

;; this function should be refactored into something smaller I think
;; Probably should really look at re-writing this with babashka/fs walk-file-tree functions
  (defn recurse-paths!
    "Walks a file system storing all directories that aren't excluded. Each directory that is found will
     be written to the DB as a path-block for later processing.

     Params:
     root-dir            A directory (string or a file) to recurse through
     progress-callback   (optional - default nil) A function to call back with progress updates it should expect
                         a map with progress details {:dir-count
                                                      :file-count
                                                      :byte-count}

     Returns the number of directories processed and bytes expected to be backed up"
    ([root-dir]
     (recurse-paths! root-dir nil))

    ([root-dir progress-callback]
     (let [root-file-dir (if (string? root-dir) (io/file root-dir) root-dir)]
       (loop [dirs [{:parent-id nil
                     :file-dir  root-file-dir}]
              dir-count 0
              file-count 0
              byte-count 0]
         ;(su/dbg "got dirs: " dirs)
         (if-some [current-dir (first dirs)]
           (let [{:keys [parent-id file-dir]} current-dir
                 path-block (create-path-block file-dir parent-id)
                 ;_ (su/dbg "created path block: " path-block)
                 tx-info (db/add-path-block! path-block)
                 children (vec (.listFiles file-dir))
                 ;_ (su/dbg "got children: " children)
                 child-dirs (->> children
                                 (filter #(and (.isDirectory %)
                                              (included? (str (fs/canonicalize %)))))
                                 (map (fn [dir] {:parent-id (:xt/id path-block) :file-dir dir}))) ;; too lazy, these to filters should be in a single
                 child-files (filter #(not (.isDirectory %)) children) ;; function
                 sum-files (->> child-files
                                (map #(.length %))
                                (reduce +))]
             (when progress-callback
               (progress-callback {:cwd        file-dir
                                   :dir-count  dir-count
                                   :file-count  file-count
                                   :byte-count byte-count}))
             (recur (concat (rest dirs) child-dirs)
                    (inc dir-count)
                    (+ file-count (count child-files))
                    (+ byte-count sum-files)))
           (let [result {:dir-count  dir-count
                         :file-count  file-count
                         :byte-count byte-count}]
             (when progress-callback
               (progress-callback (assoc result :cwd (fs/canonicalize root-file-dir))))
             result))))))

(defn get-backup-info
  "Returns map of details regarding this backup"
  []

  (:backup-info @collector-state))

(defn start
  "Start process to wait for new paths from which to create onsite blocks.

   Params:
   backup-paths     A sequence of backup path definitions (maps) and exclusions"
  [backup-paths]

  (when-not (:started @collector-state)
    (println "Starting Collector started: " (:started @collector-state))

    (try
      (let [backup-info (db/start-backup! backup-paths :adhoc)]
        (dosync (alter collector-state assoc :started true :backup-info backup-info))
        (doseq [path-def backup-paths]
          (dosync (alter collector-state update-in [:backup-paths] conj path-def))
          (-> (create-root-path-block path-def)
              (db/add-path-block!)
              (recurse-paths!))))
      (catch Exception e
        (log/error "Collector stopped with exception: " (.getMessage e))))))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @collector-state)
    (println "Collector: stopping")

    (dosync (alter collector-state assoc-in [:started] false))
    #_(put! :path-chan stop-key)))

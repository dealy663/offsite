(ns offsite-cli.collector.col-core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [offsite-cli.channels :refer :all]
            [mount.core :as mount]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.channels :as ch]
            [clojure.tools.logging :as log]))

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
           (new-channel! :path-chan stop-key)
           #_(start))
  :stop (stop))

(defn create-block
  "Create a backup block

  params:
  path-defs     A backup path, with possible exclusions
  parent-block  (optional - default nil) The ID of the ons parent block

  returns:     A backup block"
  ([{:keys [path exclusions] :as path-defs} parent-block]

   ;; I'm still of mixed opinions about whether to store an actual file object vs a path string
   ;; here. It looks like a valid file object can be written to XTDB, staying with path string for now
   (let [file-dir (io/file path)]
     {:xt/id     (.hashCode file-dir)
      :root-path (.getCanonicalPath file-dir)
      :orig-path path
      :data-type :path-block
      :backup-id (:backup-id (get-backup-info))
      ;:file-dir
      :parent-id (if (some? parent-block) (:xt/id parent-block))
      :size      (if (.isDirectory file-dir) 0 (.length file-dir))}))

  ([path-defs]
   (create-block path-defs nil)))

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

  (a/go-loop []
    (when-some [path (ch/take! :path-chan)]
      )))

(defn included?
  "Returns true if the file-block is not in an exclusion list

   Params:
   file-dir-block"
  [file-dir-block]

  ;; logic will need to be smart enough to figure out relative exclusion paths and
  ;; wildcard designators
  ;;
  ;; always returns true for now
  true)

(defn get-backup-info
  "Returns map of details regarding this backup"
  []

  (:backup-info @collector-state))

(defn start
  "Start process to wait for new paths from which to create onsite blocks.

   Params:
   backup-paths     A sequence of backup path definitions and exclusions"
  [backup-paths]

  (when-not (:started @collector-state)
    (println "Starting Collector started: " (:started @collector-state))

    (try
      (let [backup-info (db/start-backup! backup-paths :adhoc)]
        (dosync (alter collector-state assoc :started true :backup-info backup-info))
        (doseq [path-def backup-paths]
          (dosync (alter collector-state update-in [:backup-paths] conj path-def))
          (-> (create-block path-def)
              (db/add-path-block)
              #_(ch/put! :onsite-block-chan))))
      (catch Exception e
        (log/error "Collector stopped with exception: " (.getMessage e))))))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @collector-state)
    (println "Collector: stopping")

    (dosync (alter collector-state assoc-in [:started] false))
    #_(put! :path-chan stop-key)))

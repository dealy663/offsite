(ns offsite-cli.collector.col-core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [offsite-cli.channels :refer :all]
            [mount.core :as mount]
            [offsite-cli.db.db-core :as db]
            [clojure.tools.logging :as log]))

(def stop-key :stop-collector)

(def collector-state (ref {:files         []
                           :started      false
                           :backup-count 0
                           :push-count   0
                           :total-bytes  0
                           :push-bytes   0}))

(declare start stop)

(mount/defstate collector-chans
  :start (do
           (new-channel! :path-chan stop-key)
           #_(start))
  :stop (stop))

(defn create-block [{:keys [path exclusions] :as path-defs}]
  "Create a backup block

  params:
  path-defs     A backup path, with possible exclusions

  returns:     A backup block"
  (let [file-dir (io/file path)
        block   {:root-path  (.getCanonicalPath file-dir)
                 :file-dir    file-dir
                 :size       (if (.isDirectory file-dir) 0 (.length file-dir))}]
    ;; only add the :exclusions kv pair if the exclusions vector has data
    (let [return (if (or (nil? exclusions) (empty? exclusions))
                      block
                      (assoc block :exclusions exclusions))]
      (put! :onsite-block-chan return)
      return)))

;(defn start [backup-paths]
;  "Start processing the files in the backup paths, cataloguing current state, changed files
;   and passing them off to the file-processor for backup
;
;   params:
;   backup-paths    A sequence of paths to recurse through containing the dirs to backup"
;  (doseq [path-def backup-paths]
;    (create-block path-def)))

(defn start [backup-paths]
  "Start process to wait for new paths from which to create onsite blocks.

   Params:
   backup-paths     A sequence of backup path definitions and exclusions"

  (when-not (:started @collector-state)
    (println "Starting Collector started: " (:started @collector-state))

    (try
      (db/start-backup backup-paths :adhoc)
      (dosync (alter collector-state assoc-in [:started] true))
      (doseq [path-def backup-paths]
        (create-block path-def))
      (catch Exception e
        (log/error "Collector stopped with exception: " (.getMessage e))))))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @collector-state)
    (println "Collector: stopping")

    (dosync (alter collector-state assoc-in [:started] false))
    #_(put! :path-chan stop-key)))

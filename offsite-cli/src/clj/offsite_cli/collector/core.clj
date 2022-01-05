(ns offsite-cli.collector.core
  (:require [clojure.java.io :as io]
            [offsite-cli.block-processor.core :as bp]))

(def db-ref (ref   {:files             []
                    :backup-count     0
                    :push-count       0
                    :total-bytes      0
                    :push-bytes       0}))

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
    (if (or (nil? exclusions) (empty? exclusions))
      block
      (assoc block :exclusions exclusions))))

(defn start [backup-paths]
  "Start processing the files in the backup paths, cataloguing current state, changed files
   and passing them off to the file-processor for backup

   params:
   backup-paths    A sequence of paths to recurse through containing the dirs to backup"
  (doseq [path-def backup-paths]
    (-> path-def create-block bp/process-block)))
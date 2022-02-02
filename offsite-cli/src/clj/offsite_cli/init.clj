(ns offsite-cli.init
  (:require
    [clojure.edn :as edn]
    [mount.core :refer [defstate]]
    [offsite-cli.block-processor.onsite]
    [clojure.java.io :as io]
    [offsite-cli.system-utils :as su]
    [clojure.string :as str]
    [babashka.fs :as fs])
  (:import (java.io File)
           (java.nio.file Path Paths)))


(declare get-paths)

(defstate backup-paths
  :start (do
           (get-paths)
           su/paths-config))

(defn reset-default-backup-paths
  "Reset the configuration to use the default backup-paths.edn file."
  []

  (swap! backup-paths assoc :paths-file su/default-paths-file)
  (:paths-file @backup-paths))

(defn build-exclusions
  "Builds a vector of usable exclusions from a backup path.

   Params:
   backup-path-def       An entry from a backup config file {:path 'some/path' :exclusions [ex1, ex2]}

   Returns a vector of full paths to excluded dirs or files. Does not yet support wildcards or globbing"
  [backup-path-def]

  (let [{:keys [path exclusions]} backup-path-def
        exclusions (filterv #(not (str/blank? %)) exclusions)]
    ;(su/dbg "got path: " path " got exclusions: " exclusions)
    (when (seq exclusions)
      (let [root-dir       (io/file path)
            canonical-path (fs/canonicalize root-dir)
            path-root      (-> root-dir fs/canonicalize .getRoot str) #_(.getRoot (fs/canonicalize root-dir))]
        (mapv #(when-some [excl %]
                 (if (str/starts-with? excl path-root)
                   excl
                   (str canonical-path File/separator excl)))
              exclusions)))))

;; should we adapt this to take an optional override to paths-config?
(defn get-paths
  "Get the paths to be backed up, use those specified in backup-paths.edn or prompt the user to define
   the paths to be backed up

   Params:
   path-override   (optional) Set a custom file for the backup-paths.edn config file,
                   in the future we probably should support merging multiple path configs"
  [& paths-override]

  (let [paths-file (or (first paths-override) (:paths-file @su/paths-config))]
    (if (.exists (io/as-file paths-file))
      (do (swap! su/paths-config assoc :backup-paths (edn/read-string (slurp paths-file))
                 :paths-file paths-file)
           @su/paths-config)
      ;(edn/read-string (slurp paths-config))
      ;; Need to add some sort of logging facility SLF4J? Timbre?
      #_nil)))

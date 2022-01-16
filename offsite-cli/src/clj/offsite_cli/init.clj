(ns offsite-cli.init
  (:require
    [clojure.edn :as edn]
    [mount.core :refer [defstate]]
    [offsite-cli.block-processor.bp-core]
    [clojure.java.io :as io]))

;; Default name of backup paths configuration, which is expected to be found at the top-level of the installation
;; directory for the offsite client
(def default-paths-file "backup-paths.edn")
(def paths-config (atom {:paths-file default-paths-file
                         :backup-paths []}))

(declare get-paths)

(defstate backup-paths
  :start (do
           (get-paths)
           paths-config))

(defn reset-default-backup-paths []
  "Reset the configuration to use the default backup-paths.edn flle."

  (swap! backup-paths assoc :paths-file default-paths-file)
  (:paths-file @backup-paths))

;; should we adapt this to take an optional override to paths-config?
(defn get-paths [& paths-override]
  "Get the paths to be backed up, use those specified in backup-paths.edn or prompt the user to define
   the paths to be backed up

   Params:
   path-override   (optional) Set a custom file for the backup-paths.edn config file,
                   in the future we probably should support merging multiple path configs"

  (let [paths-file (or (first paths-override) (:paths-file @paths-config))]
    (if (.exists (io/as-file paths-file))
      (do (swap! paths-config assoc :backup-paths (edn/read-string (slurp paths-file))
                 :paths-file paths-file)
           @paths-config)
      ;(edn/read-string (slurp paths-config))
      ;; Need to add some sort of logging facility SLF4J? Timbre?
      #_nil)))

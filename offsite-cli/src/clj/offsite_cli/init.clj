(ns offsite-cli.init
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]))

;; Default name of backup paths configuration, which is expected to be found at the top-level of the installation
;; directory for the offstite client
(def default-paths-config "backup-paths.edn")

;; should we adapt this to take an optional override to paths-config?
(defn get-paths [& paths-override]
  "Get the paths to be backed up, use those specified in backup-paths.edn or prompt the user to define
   the paths to be backed up

   Params:
   path-override   (optional) Set a custom file for the backup-paths.edn config file,
                   in the future we probably should support merging multiple path configs"

  (let [paths-config (or (first paths-override) default-paths-config)]
    (if (.exists (io/as-file paths-config))
      (edn/read-string (slurp paths-config))
      ;; Need to add some sort of logging facility SLF4J? Timbre?
      #_nil)))

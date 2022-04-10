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
           (java.util.regex Pattern)
           (java.nio.file Path Paths FileSystems)))


(declare get-paths)

(defstate backup-paths
  :start (do
           (get-paths)
           ;;wtf is this? should be removed
           su/paths-config))

(defn reset-default-backup-paths
  "Reset the configuration to use the default backup-paths.edn file."
  []

  (swap! backup-paths assoc :paths-file su/default-paths-file)
  (:paths-file @backup-paths))

(def matcher (FileSystems/getDefault))

(defn pattern-matcher-fn
  "Returns a function that can be used to test if a file matches a glob or regex pattern"
  [pat-str]

  (fn [f]
    (su/dbg "matching file: " (.getName f) ", pat: " pat-str)
    (when-let [m (.getPathMatcher matcher pat-str)]
      (su/dbg "got path matcher: " m)
      (.matches m (fs/canonicalize f)))))

(defn string-path-matcher-fn
  "Returns a function which takes a path and returns true or false if the string-path either
  matches the pattern or begins with the pattern (if it is a directory?)

  Params:
  str-pat      The pattern that will be matched against a path"
  [str-pat]

  (fn [file]
    (let [f-path (-> file fs/path str)]
      (su/dbg "pattern: " str-pat ", path: " f-path)
      (or (= f-path str-pat)
          (when (str/ends-with? str-pat fs/file-separator)
            (let [f-path (if (.isDirectory file) (str f-path "/") f-path)]
              (su/dbg "f-path: " f-path)
              (str/starts-with? f-path str-pat)))))))

(defn build-exclusions
  "Builds a vector of usable exclusions from a backup path.

   Params:
   backup-path-def       An entry from a backup config file {:path 'some/path' :exclusions [ex1, ex2]}

   Returns a vector of full paths to excluded dirs or files. Does not yet support wildcards or globbing"
  [backup-path-def]

  (let [{:keys [path exclusions]} backup-path-def
        exclusions (filterv #(not (str/blank? %)) exclusions)]
    (su/dbg "got path: " path " got exclusions: " exclusions)
    (when (seq exclusions)
      (let [root-dir       (io/file path)
            canonical-path (fs/canonicalize root-dir)
            path-root      (-> canonical-path .getRoot str)]
        (mapv #(let [[type _] (str/split % #":")]
                 (if (or (= "glob" type) (= "regex" type))
                   (pattern-matcher-fn %)
                   (string-path-matcher-fn (str path "/" %))))
              exclusions)))))

(defn get-global-exclusions
  "Build a collection of global exclusions that will be applied to all paths

  Params:
  paths-config     (optional - default = su/paths-config) The paths that have already been read in from the
                  configuration EDN file.

  Returns a sequence of global exclusions"
  ([]
   (get-global-exclusions @su/paths-config))

  ([paths-config]
   (mapv (fn [exclusion-pat]
           (let [[type _] (str/split exclusion-pat #":")]
             (su/dbg "excl type: " type ", exclusion-str: " exclusion-pat)
             (if (or (= "regex" type) (= "glob" type))
               (pattern-matcher-fn exclusion-pat)
               (string-path-matcher-fn exclusion-pat)))) (-> paths-config :globals :exclusions))))

(defn get-exclusions
  "Adds the exclusions set for each path to the paths config

  Params:
  paths-config     (optional - default = su/paths-config) The paths that have already been read in from the
                  configuration EDN file.

  Returns a vector of excluded directories and files"
  ([]
   (get-exclusions @su/paths-config))

  ([paths-config]
   (let [exclusions (->> paths-config
                         :paths
                         (map #(build-exclusions %))
                         (reduce into []))]
     (if (seq exclusions)
       exclusions
       nil))))

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
      (let [paths             (edn/read-string (slurp paths-file))
            exclusions        (into (get-global-exclusions paths) (get-exclusions paths))]
        (swap! su/paths-config assoc :backup-paths (:paths paths) :paths-file paths-file
               :exclusions exclusions)
        @su/paths-config)
      ;(edn/read-string (slurp paths-config))
      ;; Need to add some sort of logging facility SLF4J? Timbre?
      #_nil)))

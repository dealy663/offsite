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
  "Returns a function that can be used to test if a file matches a glob or regex pattern.
  Path debug messages are triggered by preceding the path with !: the bang can follow the regex
  types (e.g. 'glob!:/some/path**' or simply '!:/some/path'

  Params:
  pat-str      The pattern to match against in the returned function
  debug        (optional - default = false) Outputs path debug messages"
  ([pat-str]
   (pattern-matcher-fn pat-str false))

  ([pat-str debug]

   (fn [f]
     (if debug
       (su/dbg "matching file: " (.getName f) ", pat: " pat-str))
     (when-let [m (.getPathMatcher matcher pat-str)]
       (if debug
         (su/dbg "got path matcher: " m))
       (.matches m (fs/canonicalize f))))))

(defn string-path-matcher-fn
  "Returns a function which takes a path and returns true or false if the string-path either
  matches the pattern or begins with the pattern (if it is a directory?)

  Params:
  str-pat      The pattern that will be matched against a path
  debug        (optional - default = false) Outputs path debug messages"
  ([str-pat]
   (string-path-matcher-fn str-pat false))

  ([str-pat debug]

   (let [str-pat-can (-> str-pat fs/canonicalize str)]
     (fn [file]
       (let [f-path (-> file fs/canonicalize str)]
         (if debug
           (su/dbg "pattern: " str-pat ", path: " f-path))
         (or (= f-path str-pat-can)
             (when (str/ends-with? str-pat fs/file-separator)
               (let [f-path (if (fs/directory? file) (str f-path "/") f-path)]
                 (if debug
                   (su/dbg "f-path: " f-path))
                 (str/starts-with? f-path str-pat-can)))))))))

(defn build-exclusions
  "Builds a vector of usable exclusion functions from a backup path. The functions take a file as a parameter
   and will return true or false if the supplied file is to be excluded from the backup

   Params:
   backup-path-def       An entry from a backup config file {:path 'some/path' :exclusions [ex1, ex2]}

   Returns a vector of exclusion functions for dirs or files."
  [backup-path-def]

  (let [{:keys [path exclusions]} backup-path-def
        exclusions (filterv #(not (str/blank? %)) exclusions)]
    ;    (su/dbg "got path: " path " got exclusions: " exclusions)
    (when (seq exclusions)
      #_(let [root-dir (io/file path)
            ;canonical-path (fs/canonicalize root-dir)
            path-root (-> canonical-path .getRoot str)])
      (mapv #(let [[type pat] (str/split % #":")
                   debug    (str/ends-with? type "!:")
                   type     (if debug (str/replace-first type #"!" "") type)]
               (if (or (str/starts-with? type "glob") (str/starts-with? type "regex"))
                 (pattern-matcher-fn (str type pat) debug)
                 (let [debug   (str/starts-with? % "!:")
                       str-pat (if debug
                                 (str/replace-first % #"!:" "")
                                 %)]
                   (string-path-matcher-fn (str path "/" str-pat) debug))))
            exclusions))))

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
             (if (str/ends-with? type "!")
               (su/dbg "excl type: " type ", exclusion-str: " exclusion-pat))
             (if (or (str/starts-with? type "regex") (str/starts-with? type "glob"))
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

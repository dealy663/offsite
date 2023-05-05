(ns offsite-cli.init-test
  (:require [clojure.test :refer :all]
            [offsite-cli.init :refer :all]
            [clojure.java.io :as io]
            [babashka.fs :as fs]
            [offsite-cli.system-utils :as su]
            [clojure.edn :as edn]
            [offsite-cli.collector.col-core :as col]
            [clojure.string :as str]
            [mount.core :as mount])
  (:import  (java.util.regex Pattern)))

(def test-configs-dir "test/configurations")
(def test-backup-data "test/backup-data")

(defn- with-components
  [components f]
  (apply mount/start components)
  (f)
  (apply mount/stop components))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.channels/channels] %))

(deftest test-init
  (testing "Found default backup-paths"
    (is (not (nil? (get-paths))))
    (is (nil? (get-paths "non-existing")))

    (let [short-paths (get-paths (str test-configs-dir "/short-backup-paths.edn"))]
      (is (not (nil? short-paths)))
      (is (vector? (:backup-paths short-paths)))
      (is (= 1 (-> short-paths :backup-paths count)))
      (is (= "test/backup-data/du.out" (-> short-paths :backup-paths first :path))))

    (let [paths       (get-paths (str test-configs-dir "/backup-paths.edn"))
          second-path (-> paths :backup-paths second)]
      (is (= "test/backup-data/music" (:path second-path)))
      (let [excluded (mapv #(str/replace-first % "!:" "") (:exclusions second-path))]
        (is (vector? excluded)
            (str "excluded: " excluded " is not a vector"))
        (is (.contains excluded "medium/")
            (str "excluded: " excluded " doesn't contain the dir: medium/")))))

  (testing "Backup path exclusions"
    (let [short-paths (get-paths (str test-configs-dir "/short-backup-paths.edn"))
          exclusions  (get-exclusions)]
      (is (nil? exclusions)
          "There should be no exclusions for short-backup-paths.edn"))
    (let [cfg-path       (str test-configs-dir "/backup-paths.edn")
          raw-cfg        (edn/read-string (slurp cfg-path))
          backup-cfg     (get-paths cfg-path)
          exclusions     (:exclusions @su/paths-config)
          file-exclusions (into (get-in raw-cfg [:globals :exclusions])
                               (mapcat #(:exclusions %) (:backup-paths backup-cfg)))]
      (is (some? exclusions)
          "The backup-paths.edn file should have several exclusions")
      (is (= (count file-exclusions) (count exclusions))
          "The number of exclusions in backup-paths.edn should match the count returned from get-paths"))))

(deftest build-exclusions-test
    (testing "negative tests for building the relative exclude paths"
    (let [backup-root  {:path (str test-backup-data "/music")}
          excl-none    (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions []}
          excl-empty   (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions [""]}
          excl-no-str  (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions ["" "small2" " \t"]}
          excl-small2  (build-exclusions backup-root)
          small-dir    (->> "music/small2" (fs/file test-backup-data) fs/canonicalize str)]
      (is (nil? excl-none)
          "nil should be returned when there are no exclusions")
      (is (nil? excl-empty)
          "nil should be returned when there are no exclusions")
      (is (nil? excl-no-str)
          "nil should be returned when there are no exclusions")
      (is (= 1 (count excl-small2))          "There should should be only one exclusion and the empty string should be ignored")
      (is (not (col/included? small-dir excl-small2))
          (str "The exclusion vector: " excl-small2 " is missing: " small-dir))))

  (testing "Fully qualified exclude paths"
    (let [music-dir        (fs/file test-backup-data "music")
          small-dir        (fs/file test-backup-data "music/small")
          medium-dir       (fs/file test-backup-data "music/medium")
          large-dir        (fs/file test-backup-data "music/large")
          exclusions       ["medium/" (str "large/")]
          backup-root      {:path (str (fs/path music-dir)) :exclusions exclusions}
          exclusion-fns    (build-exclusions backup-root)
          large-path       (-> large-dir fs/canonicalize (str "/"))
          lg-exclusions    [large-path]
          fq-backup-root   {:path "/" :exclusions lg-exclusions}
          lg-exclusion-fns (build-exclusions fq-backup-root)]
      (is (= 2 (count exclusion-fns))
          "There should only be 2 qualified paths for the small and medium sub directories")
      (is (col/included? small-dir exclusion-fns)
          (str "The directory: " small-dir " should be included. exclusions: " exclusions))
      (is (not (col/included? medium-dir exclusion-fns))
          (str "The exclusion vector: " exclusions " is missing: " medium-dir))
      (is (not (col/included? large-dir lg-exclusion-fns))
          (str "The exclusion vector: " lg-exclusions " didn't exclude: " large-dir)))))

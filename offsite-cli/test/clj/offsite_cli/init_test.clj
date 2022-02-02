(ns offsite-cli.init-test
  (:require [clojure.test :refer :all]
            [offsite-cli.init :refer :all]
            [clojure.java.io :as io]
            [babashka.fs :as fs]))

(def test-configs-dir "test/configurations")
(def test-backup-data "test/backup-data")

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
      (let [excluded (:exclusions second-path)]
        (is (vector? excluded))
        (is (.contains excluded "medium"))))))

(deftest build-exclusions-test
  (testing "the building of relative exclude paths"
    (let [backup-root {:path (str test-backup-data "/music") :exclusions ["small" "medium"]}
          exc1        (->> "music/small" (fs/file test-backup-data) fs/canonicalize str)
          exc2        (->> "music/medium" (fs/file test-backup-data) fs/canonicalize str)
          exclusions  (build-exclusions backup-root)]
      (is (= exc1 (some #{exc1} exclusions))
          (str "The exclusion vector: " exclusions " is missing: " exc1))
      (is (= exc2 (some #{exc2} exclusions))
          (str "The exclusion vector: " exclusions " is missing: " exc2))))
  (testing "negative tests for building the relative exclude paths"
    (let [backup-root  {:path (str test-backup-data "/music")}
          excl-none    (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions []}
          excl-empty   (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions [""]}
          excl-no-str  (build-exclusions backup-root)
          backup-root  {:path (str test-backup-data "/music") :exclusions ["" "small2"]}
          excl-small2  (build-exclusions backup-root)
          small-dir    (->> "music/small2" (fs/file test-backup-data) fs/canonicalize str)]
      (is (nil? excl-none)
          "nil should be returned when there are no exclusions")
      (is (nil? excl-empty)
          "nil should be returned when there are no exclusions")
      (is (nil? excl-no-str)
          "nil should be returned when there are no exclusions")
      (is (= 1 (count excl-small2))
          "There should should be only one exclusion and the empty string should be ignored")
      (is (= small-dir (some #{small-dir} excl-small2))
          (str "The exclusion vector: " excl-small2 " is missing: " excl-small2)))))

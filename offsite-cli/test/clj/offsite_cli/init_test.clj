(ns offsite-cli.init-test
  (:require [clojure.test :refer :all]
            [offsite-cli.init :refer :all]))

(def test-configs-dir "test/configurations")

(deftest test-init
  (testing "Found default backup-paths"
    (is (not (nil? (get-paths))))
    (is (nil? (get-paths "non-existing")))

    (let [short-paths (get-paths (str test-configs-dir "/short-backup-paths.edn"))]
      (is (not (nil? short-paths)))
      (is (vector? short-paths))
      (is (= 1 (count short-paths)))
      (is (= "test/backup-data/active/du.out" (-> short-paths first :path))))

    (let [paths       (get-paths (str test-configs-dir "/backup-paths.edn"))
          second-path (second paths)]
      (is (= "test/backup-data/active/music" (:path second-path)))
      (let [excluded (:exclude second-path)]
        (is (vector? excluded))
        (is (.contains excluded "medium"))))))

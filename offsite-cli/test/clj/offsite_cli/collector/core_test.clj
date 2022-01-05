(ns offsite-cli.collector.core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.core :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data/active")
(def empty-exclude-path {:path       "test/backup-data/active/music"
                         :exclusions []})

(deftest test-create-block
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "Block creation"
      (let [block (create-block (-> backup-cfg :backup-paths first))
            file-dir (io/file (str backup-data-dir "/du.out"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)))
        (is (nil? (:excludes block)) "there should be no exclusions on short-backup-paths.edn")
        (is (= file-dir (:file-dir block)) "Loaded file doesn't match expected file")
        (is (= (.length file-dir) (:size block)))) "Block file size doesn't match expected file size")
    (testing "Block creation with exclusions"
      (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
            block      (create-block (-> backup-cfg :backup-paths second))
            file-dir    (io/file (str backup-data-dir "/music"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)) "Block root-path mismatch")
        (is (.isDirectory (:file-dir block)) "Loaded file-dir should be a directory")
        (is (= file-dir (:file-dir block)) "Loaded file-dir doesn't match expected directory")
        (is (= 0 (:size block)) "A directory should start with a size of 0")
        (is (= ["medium" "large"] (:exclusions block)) "The loaded path exclusions don't match expected values")))
    (testing "Block creation with empty exclusions"
      (let [block      (create-block empty-exclude-path)]
        (is (= nil (:exclusions block)) "The loaded path exclusions don't match expected values")))
    (testing "Block creation (negative tests)"
      (is (thrown? NullPointerException (create-block nil)) "A nil block should throw an NPE"))))
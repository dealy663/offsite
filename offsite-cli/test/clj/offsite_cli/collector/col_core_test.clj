(ns offsite-cli.collector.col-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.col-core :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(deftest test-create-block
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "Block creation for single file"
      (let [block (create-block (-> backup-cfg :backup-paths first))
            file-dir (io/file (str backup-data-dir "/du.out"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)))
        (is (nil? (:exclusions block)) "there should be no exclusions on short-backup-paths.edn")
        (is (= file-dir (:file-dir block)) "Loaded file doesn't match expected file")
        (is (= (.length file-dir) (:size block)))) "Block file size doesn't match expected file size")

    (testing "Block creation with exclusions"
      (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
            block      (create-block (-> backup-cfg :backup-paths second))
            file-dir    (io/file (str backup-data-dir "/music"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)) "Block root-path mismatch")
        (is (.isDirectory (:file-dir block)) (str "Loaded file-dir (" (-> block :file-dir .getCanonicalPath)
                                                 ") should be a directory"))
        (is (= file-dir (:file-dir block)) ") Loaded file-dir doesn't match expected directory")
        (is (= 0 (:size block)) "A directory should start with a size of 0")
        (is (= ["medium" "large"] (:exclusions block)) "The loaded path exclusions don't match expected values")))

    (testing "Block creating for a directory only containing 1 file"
      (let [block    (create-block test-small-dir)
            test-dir (io/file (:path test-small-dir))
            test-file (io/file (str test-small-dir "/04 Joe Henry - Monkey.flac"))]
        (is (= (.getCanonicalPath test-dir) (:root-path block)) "The block's root should match the path originally given to create-bloc")
        (is (nil? (:dirs block)) "The block should have no sub dirs")))

    (testing "Block creation with empty exclusions"
      (let [block      (create-block empty-exclude-path)]
        (is (= nil (:exclusions block)) "The loaded path exclusions don't match expected values")))

    (testing "Block creation (negative tests)"
      (is (thrown? NullPointerException (create-block nil)) "A nil block should throw an NPE"))))
(ns offsite-cli.collector.core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.core :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data/active")

(deftest test-create-block
  (testing "Block creation"
    (let [backup-cfg   (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))
          block        (create-block (-> backup-cfg :backup-paths first))
          file-dir      (io/file (str backup-data-dir "/du.out"))]
      (is (= (.getAbsolutePath file-dir) (-> block :root-path)))
      (is (= (.length file-dir) (:size block))))))
(ns offsite-cli.collector.core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.core :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data/active")

(deftest test-create-block
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "Block creation"
      (let [block (create-block (-> backup-cfg :backup-paths first))
            file-dir (io/file (str backup-data-dir "/du.out"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)))
        (is (nil? (:excludes block)))
        (is (= file-dir (:file-dir block)))
        (is (= (.length file-dir) (:size block)))))
    (testing "Block creation (negative tests)"
      (is (thrown? NullPointerException (create-block nil)) "A nil block should throw an NPE"))))
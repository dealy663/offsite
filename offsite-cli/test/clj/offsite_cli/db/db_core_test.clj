(ns offsite-cli.db.db-core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [offsite-cli.db.db-core :refer [db-node*] :as db]
            [offsite-cli.init :as init]
            [xtdb.api :as xt]
            [mount.core :as mount]
            [offsite-cli.test-utils :as tu]
            [offsite-cli.system-utils :as su]
            [offsite-cli.channels :as ch]
            [offsite-cli.db.catalog :as dbc]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.collector.col-core :as col]
            [clojure.core.async :as a]
            [mount-up.core :as mu]
            [clojure.tools.logging :as log])
  (:import [java.io File]
           (org.slf4j LoggerFactory)
           (ch.qos.logback.classic Level)))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

;(mu/on-upndown :info mu/log :before)

(declare bp-start-test-impl bp-stop-test-impl bp-test-onsite-block-handler)
(defn- with-components
  [components f]
  (apply mount/start components)
  ;;(bp/start bp-start-test-impl)
  (f)
  (apply mount/stop components)
  ;(ch/stop-all-channels!)
  #_(bp/stop bp-stop-test-impl))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.db.db-core/db-node*] %))

(doto
  (LoggerFactory/getLogger "xtdb.query")
  (.setLevel (Level/valueOf "warn")))


(defn full-query
  [node]
  (xt/q
    (xt/db node)
    '{:find [(pull e [*])]
      :where [[e :xt/id id]]}))

(deftest test-core
  (testing "simple xtdb write and query"
    (xt/submit-tx db-node* [[::xt/put
                              {:xt/id "hi2u"
                               :user/name "zig"}]])
    (xt/sync db-node*)
    (let [result (xt/q (xt/db db-node*) '{:find  [e]
                                           :where [[e :user/name "zig"]]})
          id (-> result first first)]
      (is (= "hi2u" id)))))

(deftest backup
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "starting a backup"
      (db/stop-backup! :halted)
      (let [start-resp     (db/start-backup! (:backup-paths backup-cfg) :adhoc)
            current-backup (db/get-last-backup!)]
        (is (= true (:tx-success? start-resp))
            "The start-backup function is expected to return true if there is no other backup in progress")
        (is (= (:backup-paths backup-cfg) (:backup-paths current-backup))
            "The :backup-paths of the query result should match the configuration's")
        (is (= (:backup-id start-resp) (:backup-id current-backup))
            "The :backup-uuid must match between the start-resp and the query result")
        (is (= (su/offsite-id) (:xt/id current-backup))
            "The :offsite-id of the query result doesn't match")
        (is (= true (:in-progress current-backup))
            "The backup document should indicate that it is :in-progress after startup")
        (is (= (:backup-id start-resp) (:backup-id current-backup))
            "The :backup-id field of the current-backup doesn't match the one that was returned by start-backup")))

    (testing "Starting a backup while another is already in progress"
      (log/warn "********** The following test will generated an expected error when trying to start a backup while another is already running. ************")
      (let [result (db/start-backup! (:backup-paths backup-cfg) :adhoc)]
        (is (= false (:tx-success? result))
            "The start-backup function should return false if another backup is already in progress")
        (db/stop-backup! :halted)
        (let [last-backup (db/get-last-backup!)]
          (is (= :halted (:close-state last-backup))
              "A halted backup should have to :close-state that was supplied")
          (is (= false (:in-progress last-backup))
              "A halted or stopped backup should have :in-progress = false"))))))

(deftest get-ofs-block-state-test
  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        file-path   (-> backup-cfg :backup-paths first)]
    (testing "empty get-ofs-block-state"
      (let [result (db/get-ofs-block-state! :nonsense)]
        (is (= nil result)
            "A non-existing file block should return nil")))

    (testing "Retrieving ofs block-state from DB"
      (let [backup-cfg         (init/get-paths (str test-configs-dir "/backup-paths.edn"))
            file-path           (-> backup-cfg :backup-paths first) ;; first path is file du.out
            block              (col/create-path-block (:path file-path))
            block-info         (bp/make-block-info block)
            block-state-vec    (bp/create-ofs-block-state block-info)
            tx-info            (db/easy-ingest! block-state-vec)]
        (tu/validate-ofs-block (db/get-ofs-block-state! (:xt/id block-info))
                               tx-info)))))


;; this probably should be classified as an integration test
(deftest add-onsite-block-test
  (let [configs     (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        music-path (-> configs :backup-paths second)
        backup-id  1]
    (testing "Storing a path-block to the DB"
      (dosync (alter col/collector-state assoc :backup-info {:backup-id backup-id}))
      (let [music-dir-block (col/create-path-block (:path music-path))
            tx-info         (dbc/add-path-block! music-dir-block)
            path-block      (-> backup-id
                                (dbc/get-all-path-blocks)
                                first
                                first)]
        (is (= (:xt/id music-dir-block) (:xt/id path-block))
            (str "The " (:orig-path music-dir-block) " was not found in DB after it was added"))))

    (testing "Storing multiple path-blocks to DB"
      (let [music-dir-block (col/create-path-block (:path music-path))
            music-dir       (io/file (:orig-path music-dir-block))
            child-paths     (.listFiles music-dir)]
        (doseq [sub-dir child-paths]
          (dbc/add-path-block! (col/create-path-block (.getPath sub-dir) music-dir-block)))
        (with-open [child-path-blocks (dbc/get-path-blocks-lazy backup-id {:state :catalog})]
          (doseq [path-block (iterator-seq  child-path-blocks)]
            ;(su/dbg "got child path blocks: " path-block)
            (when (= (:xt/id music-dir-block) (:parent-id path-block))
              (is (= true (some #(= (-> path-block first :orig-path) (.getPath %)) child-paths))
                  (str "path-block: " (-> path-block first :orig-path) " not found in " (.getPath music-dir) " file list")))))))))

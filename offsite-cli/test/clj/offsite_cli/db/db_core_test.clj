(ns offsite-cli.db.db-core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [java-time :as jt]
            [offsite-cli.db.db-core :refer [db-node*] :as dbc]
            [offsite-cli.init :as init]
            [xtdb.api :as xt]
            [mount.core :as mount]
            [offsite-cli.system-utils :as su]
            [offsite-cli.channels :as ch]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.collector.col-core :as col]
            [clojure.core.async :as a]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

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
      (let [start-resp     (dbc/start-backup (:backup-paths backup-cfg) :adhoc)
            current-backup (dbc/get-last-backup)]
        ;(su/dbg "start resp:" start-resp)
        ;(su/dbg "current backup:" current-backup)
        ;(su/dbg "Full db: " (db/full-query))
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
      (let [result (dbc/start-backup (:backup-paths backup-cfg) :adhoc)]
        ;(su/dbg "start-backup result: " result)
        (is (= false (:tx-success? result))
            "The start-backup function should return false if another backup is already in progress")))))

(deftest create-ofs-block-state-test
    (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
          file-path   (-> backup-cfg :backup-paths first)
          block      (col/create-block file-path)
          block-info (bp/make-block-info (:file-dir block))]
      (testing "create-ofs-block-state"
        (is (= nil (db/create-ofs-block-state nil))
            "Creating ofs-block for a non-existing block-info should return nil")
        (let [tx-info (db/create-ofs-block-state block-info)
              _ (su/dbg "returned block-state: " tx-info)
              file         (:file-dir block)]
          (is (= true (some? tx-info))
              "A valid backup path and block-info should return a representative block-state")
          (is (= true (pos-int? (:xtdb.api/tx-id tx-info)))
              "A newly created block state should have a :tx/id")
          (let [dur (-> (jt/instant)
                        (jt/duration (:xtdb.api/tx-time tx-info))
                        .abs
                        .toMillis)]
            (is (= true (< dur 5000))
                (str "Duration: " dur " (ms) to write the block to DB was greater than 5 sec.")))))))

(deftest get-ofs-block-state-test
  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        file-path   (-> backup-cfg :backup-paths first)]
    (testing "get-ofs-block-state"
      (let [result (db/get-ofs-block-state :nonsense)]
        (is (= nil result)
            "A non-existing file block should return nil")))))


(ns offsite-cli.db.db-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.db.db-core :refer [db-node*] :as dbc]
            [offsite-cli.init :as init]
            [xtdb.api :as xt]
            [mount.core :as mount]
            [offsite-cli.system-utils :as su]
            [offsite-cli.db.db-core :as db]))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(defn- with-components
  [components f]
  (apply mount/start components)
  (f)
  (apply mount/stop components))

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
        ;(su/dbg-msg "start resp:" start-resp)
        ;(su/dbg-msg "current backup:" current-backup)
        ;(su/dbg-msg "Full db: " (db/full-query))
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
        ;(su/dbg-msg "start-backup result: " result)
        (is (= false (:tx-success? result))
            "The start-backup function should return false if another backup is already in progress")))))


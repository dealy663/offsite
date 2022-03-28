(ns offsite-cli.test-utils
  (:require [clojure.test :refer :all]
            [java-time :as jt]
            [offsite-cli.system-utils :as su]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.init :as init]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.db.dev-test]
            [offsite-cli.channels :as ch]
            [manifold.bus :as mb]))

(defn validate-ofs-block
  ""
  [block-state tx-info ]

  (is (= true (some? block-state))
      "A valid backup path and block-info should return a representative block-state")
  (is (= true (pos-int? (:xtdb.api/tx-id tx-info)))
      "A newly created block state should have a :tx/id")
  (let [dur (-> (jt/instant)
                (jt/duration (:xtdb.api/tx-time tx-info))
                .abs
                .toMillis)]
    (is (= true (< dur 5000))
        (str "Duration: " dur " (ms) to write the block to DB was greater than 5 sec."))))

(defn start-collector
  []

  (col/event-monitor (ch/m-subscribe :col-msg) #(su/dbg %1))
  (col/start (:backup-paths @init/backup-paths)))

(defn reset-db!
  []

  (col/stop)
  ;  (ch/m-drop-all-subscribers)
  (#'offsite-cli.db.dev-test/evict-backup (:backup-id (db/get-last-backup!))))


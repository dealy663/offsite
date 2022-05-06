(ns offsite-cli.test-utils
  (:require [clojure.test :refer :all]
            [java-time :as jt]
            [offsite-cli.system-utils :as su]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.init :as init]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.db.dev-test]
            [offsite-cli.channels :as ch]
            [offsite-cli.block-processor.onsite :as bpo]
            [manifold.bus :as mb]
            [manifold.stream :as ms]
            [manifold.go-off :as mg]
            [clojure.tools.logging :as log]
            [offsite-cli.db.catalog :as dbc]))

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

;(defn event-handler
;  "Handles event notifications
;
;   Params:
;   stream     A manifold stream that has subscribed to the event-bus for :root-path messages
;   handler-fn Function to handle the event"
;  [stream handler-fn]
;
;  (mg/go-off
;    (su/dbg "setting event-handler loop")
;    (loop []
;      (when-let [deferred (mg/<!? stream)]
;        (su/dbg "got an event: " deferred)
;        (handler-fn deferred)
;        #_(try
;          (su/dbg "got an event: " deferred)
;          (handler-fn deferred)
;          (catch Exception e (log/error (str "Got exception while handling event: " (.getMessage e)))))
;        (recur)))))

;(defn offsite-msg-handler
;  [msg]
;
;  (su/dbg "Got offsite msg: " msg))

(defn get-all-onsite
  []

  (dbc/get-all-path-blocks (:backup-id (db/get-last-backup!)) {:state :onsite}))

(defn start-collector
  []

  (init/reset)
  (ch/m-sub-monitor :col-msg #(su/dbg "Got collector msg: " %))
  (ch/m-sub-monitor :offsite-msg #(su/dbg (:message %) ": " (-> % :data :orig-path)))
  (bpo/start)
  ;(col/start (->> @init/backup-paths :backup-paths (take 2)))
  ;(col/start (-> @init/backup-paths :backup-paths (get 1) vector))
  (col/start (:backup-paths @init/backup-paths)))

(defn reset-db!
  []

  (col/stop)
  (bpo/stop)
  ;  (ch/m-drop-all-subscribers)
  (doseq [streams (-> @ch/channels :bus mb/topic->subscribers vals)]
    (mapv #(ms/close! %) streams))
  (#'offsite-cli.db.dev-test/evict-backup (:backup-id (db/get-last-backup!))))


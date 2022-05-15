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

(defn get-all-onsite
  []

  (dbc/get-all-path-blocks (:backup-id (db/get-last-backup!)) {:state :onsite}))

(defn event-msg-monitors
  "Sets up monitors for various event messages

  Params:
  topic      A keyword, typically a namespace
  events     A sequence of events to monitor (normally functions within the namespace"
  [topic events]

  (mapv #(ch/m-sub-monitor topic (ch/m-event-msg-handler-fn %)) events))

(defn start-collector
  []

  (init/reset)
  (event-msg-monitors :onsite-msg [:path-block-handler2 :catalog-block-handler :root-catalog-block-handler])
  (ch/m-sub-monitor :col-msg #(su/debug "Got collector msg: " %))
  (ch/m-sub-monitor :offsite-msg #(su/debug (:message %) ": " (-> % :data :orig-path)))
  (bpo/start)
  ;(col/start (->> @init/backup-paths :backup-paths (take 2)))
  ;(col/start (-> @init/backup-paths :backup-paths (get 2) vector))
  (col/start (:backup-paths @init/backup-paths)))

(defn reset-db!
  []

  (col/stop)
  (bpo/stop)
  ;  (ch/m-drop-all-subscribers)
  (doseq [streams (-> @ch/channels :bus mb/topic->subscribers vals)]
    (mapv #(ms/close! %) streams))
  (#'offsite-cli.db.dev-test/evict-backup (:backup-id (db/get-last-backup!))))

(defn restart
  "Restart the indicated client service

  Params:
  service     A keyword representing the service to restart, must be a member of (:service-keys @init/client-state)"
  [service]

  (case service
    :collector (do (reset-db!) (start-collector))
    (su/warn service " is an unknown service type, no action. Service keys: " (:service-keys @init/client-state))))
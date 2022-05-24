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
  "Sets up monitors for various event messages. These monitors are intended to be used with the m-sub-monitor and
  m-publish or the publish-msg functions created by ch/gen-publisher.

  Params:
  topic      A keyword, typically a namespace
  events     (optional - default nil) A sequence of events to monitor (normally functions within the namespace).
             If events is nil then this function is a no-op. Each of the events is a keyword which can have
             additional adornments which trigger certain behaviors.

             Event messages can have an optional last parameter that is a set of tags.

             > - Ending an event keyword with '>' will print all messages that have the :exit tag, events without the
                 exit adornment will ignore messages with the :exit tag.

             < - Starting an event keyword with '<' will print all messages that have the :start tag, events without
                 the start adornment will ignore messages with the :start tag (this feature hasn't been
                 implemented yet).

  Use case: Monitor for a set of events on :example-topic, prints all :foo-listener messages and will also
  print all :bar-listener messages (including ones with the :exit tag). If any :foo-listener messages have
  the :exit tag they will be ignored.
  (event-msg-monitors :example-topic #{:foo-listener :msg-event-fn>}

  (defn msg-event-fn []
    (let [publish-msg (ch/gen-publisher :example-topic :msg-event-fn)
          some-data   {:foo 234}]
      ...
      (publish-msg (str 'a message' some-data)
      ...
      (publish-msg 'leaving function' #{:exit})"
  ([_])

  ([topic events]

   (mapv #(ch/m-sub-monitor topic (ch/m-event-msg-handler-fn %)) events)))

(defn start-collector
  []
  (init/reset)
  (event-msg-monitors :onsite-msg #_#{:path-block-handler2 :catalog-block-handler :root-catalog-block-handler})
  (event-msg-monitors :col-msg #_#{:start})
  (event-msg-monitors :catalog-msg #_#{:get-all-path-blocks})
  (event-msg-monitors :offsite-msg #_#{:offsite-block-listener :onsite-block-handler})
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
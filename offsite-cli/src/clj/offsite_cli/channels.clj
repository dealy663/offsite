(ns offsite-cli.channels
  (:require [mount.core :refer [defstate]]
            [mount.core :as mount]
            [offsite-cli.system-utils :as su]
            [clojure.tools.logging :as log]
            [manifold.deferred :as md]
            [manifold.stream :as ms]
            [manifold.bus :as mb]
            [manifold.go-off :as mg]
            [clojure.string :as str]))

(def empty-channels {:chan-depth       10
                     :map              {}
                     :bus              nil
                     :m-streams        []
                     :sub-stream-count 0})
(def empty-channel {:chan     nil
                    :closed   nil
                    :stop-key nil})                         ;; stop key probably unnecessary, can either be channel-key
                                                            ;; or a keyword based off of channel-key

(def event-handler-fn-ref (ref {}))

(declare close-all-channels! m-drop-all-subscribers gen-publisher)
(mount/defstate channels
  :start (do
           (let [chs (ref empty-channels)]
             (dosync (alter chs assoc :bus (mb/event-bus)))
             chs))
  :stop (do
          (m-drop-all-subscribers)))

(defn m-event-msg-handler-fn
  "Returns a function for handling event messages. The function expects a message event as its single
  parameter and will call the appropriate logger to output the message. The default logger is debug
  alternate log levels can be supplied in the :log-level entry within the :args map of the message event.

  Params:
  msg-type     The keyword defining the type of message this function will match against"
  [msg-type]

  (let [exit?    (-> msg-type name (str/ends-with? ">"))
        msg-type (-> msg-type name (str/split #">") first keyword)]
    (fn [msg-event]
      (let [{:keys [event-type payload args tags]} msg-event]
        (when (= msg-type event-type)
          (let [log-level (get args :log-level :debug)]
            (if (or
                  (not (contains? tags :exit))
                  exit?)
              (su/log-msg log-level (str (:ns args) msg-type " - " payload)))))))))

(defn m-event-handler
  "Event handler loop that processes messages from the Manifold event bus. A Manifold go loop is started
  that will accept messages from the stream and dispatch them to the handler-fn.

   Params:
   stream      A Manifold stream that has subscribed to the event-bus for events
   handler-fn  Function to handle the event taken from the stream
   topic-key   (optional - default nil) The Manifold event bus topic keyword that this handler services

   Returns a map of the stream and the deferred result of the final call to handler-fn"
  ([stream handler-fn]
   (m-event-handler stream handler-fn nil))

  ([stream handler-fn key]

   (let [deferred (md/deferred)]
     (mg/go-off
       (loop [acc nil]
         (if-let [d-event (mg/<!? stream)]
           (recur
             (try
               (handler-fn d-event)
               (catch Exception e
                 (log/error (str "Event handler exception: " (.getMessage e))))))
           (do
             (su/debug "exiting m-event-handler: " key)
             (md/success! deferred acc)))))
     {:stream stream :result deferred})))

(defn m-subscribe
  "Subscribe to events on the message bus

  Params:
  event-topic    The type of event to subscribe to

  Returns a stream which acts as a sink for the subscribed event"
  [event-topic]

  (let [bus (:bus @channels)]
    (when (nil? bus)
      (su/error "Event bus has not been initialized")
      (throw (Exception. "Event bus has not been initialized")))
    (let [s (mb/subscribe bus event-topic)]
      (dosync (alter channels update :m-streams conj s))
      s)))

(defn m-sub-monitor
  "Subscribe to an event on the message bus and pass stream to a monitor loop

  Params:
  event-topic    The event to subscribe and listen for
  handler-fn     A function to be called when each event is taken off the stream

  Returns a map of the stream and the deferred result of the final call to handler-fn"
  [event-topic handler-fn]

  (-> event-topic
      m-subscribe
      (m-event-handler handler-fn event-topic)))

(defn m-publish
  "Publish an event to the message bus

  Params:
  event-topic    The type of event to publish
  payload        The event data

  Returns a deferred that will be realized when all subscribers have processed the event"
  [event-topic payload]

  (mb/publish! (:bus @channels) event-topic payload))

(defn m-drop-stream!
  "Close and drop a stream from channels

  Params:
  s      The stream to close and drop"
  [s]

  (ms/close! s)
  (dosync (alter channels update :m-streams #(remove identical? %))))

(defn m-close-all-subscribers
  "Close all subscriber streams"
  []

  (mapv #(ms/close! %) (:m-streams @channels)))

(defn m-drop-all-subscribers
  "Close and drop all subscriber streams"
  []

  (m-close-all-subscribers)
  (dosync (alter channels assoc :m-streams [])))

(defmacro gen-publisher
  "Generate a function that will publish event on the main channels' message bus for the given topic.
  The generated event publishing function will take a single parameter of the payload to be published
  for the topic and event provided to the generator.

  Params:
  topic      The topic to publish the event to
  event      The keyword specifying the event type
  gen-args   (optional) A map of additional args for the event handler.
             e.g. {:log-level :debug :ns (str *ns*) :exit true}"
  ([topic event]
   `(fn
      ([p#]
       (m-publish ~topic {:event-type ~event :payload p# :args ~{:ns (str *ns*)}}))
      ([p# tags#]
       (m-publish ~topic {:event-type ~event :payload p# :args ~{:ns (str *ns*)} :tags tags#}))))

  ([topic event gen-args]

   `(fn
      ([p#]
       (m-publish ~topic {:event-type ~event :payload p# :args ~(assoc gen-args :ns (str *ns*))}))
      ([p# tags#]
       (m-publish ~topic {:event-type ~event :payload p# :args ~(assoc gen-args :ns (str *ns*)) :tags tags#})))))


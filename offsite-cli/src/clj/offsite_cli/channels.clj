(ns offsite-cli.channels
  (:require [clojure.core.async :as a]
            [mount.core :refer [defstate]]
            [mount.core :as mount]
            [offsite-cli.system-utils :as su]
            [clojure.tools.logging :as log]
            [clojure.core.reducers :as r]
            [manifold.deferred :as md]
            [manifold.stream :as ms]
            [manifold.bus :as mb]))

;(def chan-atom (atom {:chan-depth         10
;                      :channels           {}
;                      :path-chan          {:chan nil :stop-key nil}
;                      :onsite-block-chan  nil
;                      :offsite-block-chan nil}))

(def empty-channels {:chan-depth       10
                     :map              {}
                     :bus              (mb/event-bus)
                     :sub-stream-count 0})
;(def channels (ref empty-channels))

(def empty-channel {:chan     nil
                    :closed   nil
                    :stop-key nil})                         ;; stop key probably unnecessary, can either be channel-key
                                                            ;; or a keyword based off of channel-key

(declare close-all-channels!)
(mount/defstate channels
  :start (do
           (su/dbg "starting channels")
           (ref empty-channels))
  :stop (do
          (su/dbg "closing channels")
          #_(close-all-channels! channels)))

(defn get-ch
  "Fetch a channel

   Params:
   chan-key          The keyword of the channel to retrieve
   show-chan-count   (optional - default false) Shows how many items are in channel if true

   Returns the requested channel or nil"
  ([chan-key]
   (get-ch chan-key false))

  ([chan-key show-chan-count]
   (if-let [chan (-> @channels :map chan-key :chan)]
     (do
       (if (and show-chan-count chan)
         (su/dbg "Before channel operation " chan-key " count: " (.count (.buf chan))))
       chan)
     (log/warn "Unable to find a channel named: " chan-key))))

(defn put-path!
  "Adds a backup path to the path channel

   Params:
   backup-path     An onsite path map for backup"
  [backup-path]

  (a/put! (get-ch :path-chan) backup-path))

(defn put-onsite-block!
  "Adds a backup block to the block channel

   Params:
   block    An onsite block map for backup"
  [block]

  (a/put! (get-ch :onsite-block-chan) block))


(defn channel-count
  "Returns the number of items waiting in a channel

   Params:
   channel      The channel to query for remaining items"
  [channel]

  (if-let [buf (.buf channel)]
    (.count buf)
    0))

(defn put!
  "Asynchronously puts a value on to the channel specified.

   Params:
   chan-key The keyword of the channel to operate on
   val      The value to put on the channel
   dbg      (optional - default false) if true prints channel info after put!"
  ([chan-key val dbg]

   (if-let [chan (get-ch chan-key)]
     (let [dbg-fn (fn [val] (su/dbg "After put! channel " chan-key " count: " (channel-count chan) " val: " val))
           result (if dbg
                    (a/put! chan val dbg-fn)
                    (a/put! chan val))]
       (when-not result
         (log/error "Attempted put! on a closed channel, val: " val))
       result)
     (throw (UnsupportedOperationException. (str "Unable to find channel named: " chan-key)))))

  ([chan-key val]
   (put! chan-key val false)))

(defn put!d
  "Asynchronously puts a value on to the channel specified and prints channel info.

   Params:
   chan-key  The keyword of the channel to operate on
   val       The value to put on the channel"
  [chan-key val]

  (put! chan-key val true))

(defn take!
  "Asynchronously takes the next item from the specified channel.

   Params:
   chan-key    The keyword of the channel to operate on
   dbg         (optional - default false) If true prints channel info after the take!

   Returns the item taken out of the channel"
  ([chan-key dbg]

   (if-not (keyword? chan-key)
     (throw (IllegalArgumentException. "The function take needs a channel key as its first parameter.")))
   (su/dbg "taking from chan: " chan-key)
   (if-let [chan (get-ch chan-key)]
     (let [dbg-fn (when dbg (fn [_]
                              (su/dbg "After take! channel " chan-key " count: " (channel-count chan))))
           result (if dbg
                    (a/take! chan dbg-fn)
                    (a/<!! chan))]
       result)
     (let [err-msg (str "Cannot take! from non-existing channel: " chan-key)]
       (log/error err-msg)
       (throw (UnsupportedOperationException. err-msg)))))

  ([chan-key]
   (take! chan-key :false)))

(defn take!d
  "Asynchronously takes the next item from the specified channel.

   Params:
   chan-key

   Returns the item taken from the channel or nil if channel is closed."
  [chan-key]

  (take! chan-key true))

;(defn put!!
;  "Puts a value on to the channel specified.
;
;   Params:
;   chan-key The keyword of the channel to operate on
;   val      The value to put on the channel"
;  [chan-key val]
;
;  (su/dbg "Before put! channel " chan-key " count: " (.count (.buf (-> @chan-atom chan-key :chan))) " val: " val)
;  (let [result (a/>!! (-> @chan-atom chan-key :chan) val)]
;    (when-not result
;      (log/error "Attempted put! on a closed channel, val: " val))
;    result))
;
;(defn take!!
;  "Takes the next item from the specified channel or blocks the thread if it is unavailable.
;
;   Params:
;   chan-key    The keyword of the channel to operate on
;
;   Returns the item taken out of the channel"
;  [chan-key]
;
;  (su/dbg "Before take! channel " chan-key " count: " (.count (.buf (-> @chan-atom chan-key :chan))))
;  (let [result (a/<!! (-> @chan-atom chan-key :chan))]
;    result))

(defn drain-channel!
  "Removes all items from a channel

   Params:
   chan-key     The channel to drain
   item-fn     (optional) - A function to handle each item pulled from the channel"
  ([chan-key item-fn]

   (if-not (keyword? chan-key)
     (throw (IllegalArgumentException. "drain-channel can only accept a channel key")))
   (loop []
     (when-let [val (take! chan-key)]
       (when-not (nil? item-fn)
         (item-fn val))
       (recur))))

  ([channel]
   (drain-channel! channel nil)))

(defn new-channel!
  "Creates a new  channel discarding the old one

   Params
   channel-key        Keyword for either :path-chan or ::onsite-block-chan to create
   stop-key           Keyword that the channel handler will listen for to close its thread
   options            (optional - default {:depth (:chan-depth @chan-atom)})
                      A map of channel parameters, currently accepts :depth n and :timeout millis. If the :timeout
                      option is specified then any other options are ignored (in the future we may support
                      buf-or-n, xform, ex-handler)

   Returns the newly created channel"
  ([channel-key stop-key options]
   (let [chan     (if (contains? options :timeout) (a/timeout (:timeout options)) (a/chan (:depth options)))
         new-chan (assoc empty-channel :chan        chan
                                       :publishers  {}
                                       :closed      false
                                       :stop-key    stop-key)]
     (dosync
       (let [old-chan (get-ch channel-key)]
         (when (and old-chan (> (channel-count old-chan) 0))
           (log/warn "Replacing active channel: " channel-key " that has items remaining in its buffer."))
         (alter channels assoc-in [:map channel-key] new-chan)))
     (:chan new-chan)))

  ([channel-key stop-key]
   (new-channel! channel-key stop-key {:depth (:chan-depth @channels)})))

(defn new-publisher!
  "Creates a new publisher on a channel. Updates the channels ref with the new publication

   Params:
   chan-key     The channel's ID keyword
   topic        A keyword defining the topic for the subscribers

   Returns a new publication or nil if the channel doesn't exist"
  [chan-key topic]

  (if-let [chan (get-ch chan-key)]
    (let [pub (a/pub chan #(topic %))]
      (when-some [_ (get-in @channels [:map chan-key :publishers topic])]
        (log/warn "Replacing existing publisher: " topic ", on chan: " chan-key))
      (dosync (alter channels update-in [:map chan-key :publishers] assoc topic pub))
      {:publisher pub
       :chan-key  chan-key
       :topic     topic})
    (do
      (log/error "Cannot create publisher for non-existing channel: " chan-key)
      nil)))

(defn get-publication
  "Retrieves a publication (chan-key, publisher and topic)

   Params:
   chan-key     The channel key
   topic        The topic of the publisher

   Returns a publication"
  [chan-key topic]

  (if-let [pub (get-in @channels [:map chan-key :publishers topic])]
    {:publisher pub
     :chan-key  chan-key
     :topic     topic}
    nil))

(defn subscribe
  "Subscribe to a topic-val on the given channel. Creates a monitor go-loop which will execute
   monitor-fn for each message received for topic-val. If the channel or publisher don't exist
   an UnsupportedOperationException will be thrown.

   Params:
   publication    A publisher map
   topic-val      The topic subject to subscribe to
   monitor-fn     Monitor function that will run in go loop on receipt of topic-val (fn [message] ...)
   chan-cfg       (optional) - channel config options {:timeout ms or :buf-depth (default 1)} either supply timeout
                  or buf-depth, they are mutually exclusive

   Returns a channel that is subscribed to the requested topic"
  ([publication topic-val monitor-fn]
   (subscribe publication topic-val monitor-fn {:buf-depth 1}))

  ([publication topic-val monitor-fn chan-cfg]
   (let [chan-map (get-in @channels [:map (:chan-key publication)])
         channel  (:chan chan-map)]
     (if (nil? channel)
       (throw (UnsupportedOperationException. (str "Channel " (:chan-key publication) " does not exist."))))
     (let [{:keys [buf-depth timeout]} chan-cfg
           chan (cond
                  (some? buf-depth) (a/chan buf-depth)
                  (some? timeout) (a/timeout timeout))]
       ;(su/dbg pub-topic-fn " publisher: " pub)
       (su/dbg "Subscribing to publication: " publication   ;(select-keys publication [:chan-key :topic])
               ", for subject: " topic-val)
       (a/sub (:publisher publication) topic-val chan)
       (a/go-loop []
         (if-let [message (a/<! chan)]
           (when-not (= :halt (monitor-fn message))
             (recur))
           (su/dbg "Closing monitor for publisher: " (:topic publication) ", subject: " topic-val)))
       chan))))

(defn eb-subscribe
  "Subscribe to a topic on the event-bus in channels

   Params:
   topic       The topic to subscribe to

   Returns a Manifold stream where the subscribed messages will arrive"
  [topic]

  (let [sub-stream (mb/subscribe (:bus @channels) topic)]
    (dosync (alter channels update-in [:sub-stream-count] inc))
    sub-stream))

(defn new-all-channels!
  "Creates all channels

   Params:
   stop-keys          A map of stop keywords based on the channel name
   depth-override     (optional) Override the default :chan-depth value

   Returns a map of the channels that were created"

  ([stop-keys]
   (new-all-channels! stop-keys (:chan-depth @channels)))

  ([stop-keys depth-override]
   {:path-chan         (new-channel! :path-chan (:path-chan stop-keys) depth-override)
    :onsite-block-chan (new-channel! :onsite-block-chan (:onsite-block-chan stop-keys) depth-override)}))

(defn close!
  "Closes the requested channel

   Params:
   chan-key       Keyword of the channel to close"
  [chan-key]

  (dosync
    (when-let [chan (get-ch chan-key)]
      (a/close! chan)
      (alter channels assoc-in [:map chan-key :closed] true))))

(defn stop!
  "Sends the stop key to the specified channel

   Params
   chan-key      The key identifying the channel to stop"
  [chan-key]

  (when-let [chan (get-ch chan-key)]
    (let [chan-map (get-in @channels [:map chan-key])]
      (when-some [closed? (:closed chan-map)]
        (when-not closed?
          (a/put! chan (:stop-key chan-map)))))))

(defn stop-all-channels!
  "Stops all channels and then closes them

   Params:
   channels    The channels ref"
  [channels]

  (su/dbg "Stopping all channels")
  (mapv #(let [chan-info (-> @channels :map %)]
           (a/go
             (su/dbg "Stopping channel: " %)
             (a/>! (:chan chan-info) (:stop-key chan-info))))
        (-> @channels :map keys)))

(defn chan-unsub-all
  "Unsubscribe from all topics on all pubs on a channel

   Params:
   chan-map      A map of channel's data"
  [chan-map]

  (when-let [pubs (:publishers chan-map)]
    (mapv #(a/unsub-all %) (vals pubs))))

(defn unsub-all-pubs
  "Unsubscribe all publications on all channels

   Params:
   channels    The channels ref
   chan-key    The key of channel with publications"
  [channels]

  (when-let [all-channels (-> @channels :map)]
    (mapv #(chan-unsub-all %) (vals all-channels))))

(defn close-all-channels!
  "Close all channels

   Params:
   channels    The channels ref"
  [channels]

  ;(a/go)
  ;; this will probably need to come out if we abandon core.async
  (mapv (fn [ck]
          (su/dbg "closing: " ck)
          (when-let [chan-map (-> @channels :map ck)]
            (su/dbg "unsubscribing all pubs from " ck ", pubs: " (keys (:publishers chan-map)))
            (chan-unsub-all chan-map)
            (when-let [chan (:chan chan-map)]
              (a/close! chan)
              (dosync (alter channels assoc-in [:map ck :closed] true)))))
        (-> @channels :map keys))
  (doseq [sub-streams (-> (:bus @channels) (mb/topic->subscribers) vals)]
    (mapv #(ms/close! %) sub-streams)))

(defn reset-channels!
  "Resets the channels ref back to the empty state"
  []

  (stop-all-channels! channels)
  (close-all-channels! channels)
  (dosync (ref-set channels empty-channels)))

(defn get-channel-info
  "Get current channel status information. Includes state (open, closed), items in channel buffer etc.

   Params:
   channel-key      The key of the channel in the manager

   Returns a channel info map, or an {:error message} if the channel doesn't exist"
  [channel-key]

  (let [chan (get-ch channel-key)]
    (if (nil? chan)
      (let [error-msg (str "Channel " channel-key " doesn't exist.")]
        (log/error error-msg)
        {:error error-msg})
      (let [chan-map  (-> @channels :map channel-key)
            chan-info {:channel-key   channel-key
                       :channel-count (channel-count chan)}]
        (merge chan-info (dissoc chan-map :chan))))))

(defn get-all-channels-info
  "Get the channel info for each channel in the manager(

   Returns a vector of channel info maps"
  []

  ;; must learn how to properly use transducers
  (->> (-> @channels :map keys)
       (map #(get-channel-info %1))
       (reduce conj [])))

;(defn create-pipeline
;  ""
;  [])
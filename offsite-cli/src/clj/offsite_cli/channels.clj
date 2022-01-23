(ns offsite-cli.channels
  (:require [clojure.core.async :as a]
            [mount.core :refer [defstate]]
            [mount.core :as mount]
            [offsite-cli.system-utils :as su]
            [clojure.tools.logging :as log]))

(def chan-atom (atom {:chan-depth         10
                      :channels           {}
                      :path-chan          {:chan nil :stop-key nil}
                      :onsite-block-chan  nil
                      :offsite-block-chan nil}))

(def channels (ref {:chan-depth 10
                    :map        {}}))

(def empty-channel {:chan     nil
                    :closed   nil
                    :stop-key nil})                         ;; stop key probably unnecessary, can either be channel-key
                                                            ;; or a keyword based off of channel-key

;(mount/defstate channels
;                :start (new-channels!)
;                :stop  ((or (:stop defaults) (fn [] #_(db/stop-xtdb!)))))


(defn get-ch
  "Fetch a channel

   Params:
   chan-key          The keyword of the channel to retrieve
   show-chan-count   (optional - default false) Shows how many items are in channel if true

   Returns the requested channel or nil"
  ([chan-key]
   (get-ch chan-key false))

  ([chan-key show-chan-count]
   (let [chan (-> @channels :map chan-key :chan)]
     (if (and show-chan-count chan)
       (su/dbg "Before channel operation " chan-key " count: " (.count (.buf chan))))
     chan)))

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

  (.count (.buf channel)))

(defn put!
  "Asynchronously puts a value on to the channel specified.

   Params:
   chan-key The keyword of the channel to operate on
   val      The value to put on the channel
   dbg      (optional - default false) if true prints channel info after put!"
  ([chan-key val dbg]

   (let [chan   (get-ch chan-key)
         dbg-fn (fn [val] (su/dbg "After put! channel " chan-key " count: " (channel-count chan) " val: " val))
         result (if dbg
                  (a/put! chan val dbg-fn)
                  (a/put! chan val))]
     (when-not result
       (log/error "Attempted put! on a closed channel, val: " val))
     result))

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

   (let [chan (get-ch chan-key)
         dbg-fn (when dbg (fn [_]
                            (su/dbg "After take! channel " chan-key " count: " (channel-count chan))))
         result (if dbg
                  (a/take! chan dbg-fn)
                  (a/<!! chan))]
     result))

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
   channel     The channel to drain
   item-fn     (optional) - A function to handle each item pulled from the channel"
  ([channel item-fn]

   (loop []
     (when-let [val (take! channel)]
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
   depth-override     (optional - default (-> @chan-atom :chan-depth)) Override the default :chan-depth value

   Returns the newly created channel"
  ([channel-key stop-key depth-override]
   (let [new-chan (assoc empty-channel :chan     (a/chan depth-override)
                                       :closed   false
                                       :stop-key stop-key)]
     (dosync
       (let [old-chan (get-ch channel-key)]
         (when (and old-chan (> (channel-count old-chan) 0))
           (log/warn "Replacing active channel: " channel-key " that has items remaining in its buffer."))
         (alter channels assoc-in [:map channel-key] new-chan)))
     new-chan))

  ([channel-key stop-key]
   (new-channel! channel-key stop-key (:chan-depth @chan-atom))))

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
    (let [chan-map (-> @channels :map chan-key)]
      (-> chan-map :chan a/close!)
      (alter channels assoc-in [:map chan-key :closed] true))))

(defn stop!
  "Sends the stop key to the specified channel

   Params
   chan-key      The key identifying the channel to stop"
  [chan-key]

  (a/put! (get-ch chan-key) (-> @channels :map chan-key :stop-key)))

(defn stop-all-channels!
  "Stops all channels and then closes them"
  []

  (let [path-chan      (get-ch :path-chan)
        ons-block-chan (get-ch :onsite-block-chan)]
    (a/go
      (when path-chan
        (a/>! path-chan (-> @chan-atom :path-chan :stop-key)))
      (when ons-block-chan
        (a/>! ons-block-chan (-> @chan-atom :onsite-block-chan :stop-key))))))

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
  "Get the channel info for each channel in the manager

   Returns a vector of channel info maps"
  []

  ;; must learn how to properly use transducers
  (->> (-> @channels :map keys)
       (map #(get-channel-info %1))
       (reduce conj [])))

;(defn create-pipeline
;  ""
;  [])
(ns offsite-cli.channels
  (:require [clojure.core.async :as a]
            [mount.core :refer [defstate]]
            [mount.core :as mount]))

(def chan-atom (atom {:chan-depth         10
                      :path-chan          {:chan nil :stop-key nil}
                      :onsite-block-chan  nil
                      :offsite-block-chan nil}))

;(mount/defstate channels
;                :start (new-channels!)
;                :stop  ((or (:stop defaults) (fn [] #_(db/stop-xtdb!)))))

(defn put-path!
  "Adds a backup path to the path channel

   Params:
   backup-path     An onsite path map for backup"
  [backup-path]

  (a/>!! (:path-chan @chan-atom) backup-path))

(defn put-onsite-block!
  "Adds a backup block to the block channel

   Params:
   block    An onsite block map for backup"
  [block]

  (a/>!! (:onsite-block-chan @chan-atom) block))

(defn put!
  "Puts a value on to the channel specified.

   Params:
   chan-key The keyword of the channel to operate on
   val      The value to put on the channel"
  [chan-key val]

  (a/>!! (-> @chan-atom chan-key :chan) val))

(defn take!
  "Takes the next item from the specified channel or blocks the thread if it is unavailable.

   Params:
   chan-key    The keyword of the channel to operate on

   Returns the item taken out of the channel"
  [chan-key]

  (a/<!! (-> @chan-atom chan-key :chan)))

(defn new-channel!
  "Creates a new  channel discarding the old one

   Params
   channel-key        Keyword for either :path-chan or ::onsite-block-chan to create
   stop-key           Keyword that the channel handler will listen for to close its thread
   depth-override     (optional) Override the default :chan-depth value

   Returns the newly created channel"
  ([channel-key stop-key depth-override]
   (let [new-chan (a/chan (:chan-depth @chan-atom))]
     (swap! chan-atom update-in [channel-key] assoc :chan new-chan :stop-key stop-key)
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
   (new-all-channels! stop-keys (:chan-depth @chan-atom)))

  ([stop-keys depth-override]
   {:path-chan         (new-channel! :path-chan (:path-chan stop-keys) depth-override)
    :onsite-block-chan (new-channel! :onsite-block-chan (:onsite-block-chan stop-keys) depth-override)}))

(defn close!
  "Closes the requested channel

   Params:
   chan-key       Keyword of the channel to close"
  [chan-key]

  (-> @chan-atom chan-key :chan a/close!))

(defn stop!
  "Sends the stop key to the specified channel

   Params
   chan-key      The key identifying the channel to stop"
  [chan-key]

  (put! chan-key (-> @chan-atom chan-key :stop-key)))

(defn stop-all-channels!
  "Stops all channels and then closes them"
  []

  (put! :path-chan (-> @chan-atom :path-chan :stop-key))
  (put! :onsite-block-chan (-> @chan-atom :onsite-block-chan :stop-key)))
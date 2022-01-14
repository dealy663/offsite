(ns offsite-cli.channels
  (:require [clojure.core.async :as a]
            [mount.core :refer [defstate]]
            [mount.core :as mount]))

(def chan-atom (atom {:chan-depth       10
                      :path-chan        {:chan nil :stop-key nil}
                      :block-chan       nil
                      :offsite-block-ch nil}))
;(def chan-depth 10)
;(def path-chan  (a/chan chan-depth))
;(def block-chan (a/chan chan-depth))

;(mount/defstate channels
;                :start (new-channels!)
;                :stop  ((or (:stop defaults) (fn [] #_(db/stop-xtdb!)))))

(defn put-path!
  "Adds a backup path to the path channel

   Params:
   backup-path     An onsite path map for backup"
  [backup-path]

  (a/>!! (:path-chan @chan-atom) backup-path))

(defn put-block!
  "Adds a backup block to the block channel

   Params:
   block    An onsite block map for backup"
  [block]

  (a/>!! (:block-chan @chan-atom) block))

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
   channel-key        Keyword for either :path-chan or :block-chan to create
   stop-key           Keyword that the channel handler will listen for to close its thread
   depth-override     (optional) Override the default :chan-depth value"
  ([channel-key stop-key]
   (swap! chan-atom update-in [channel-key] assoc :chan (a/chan (:chan-depth @chan-atom))
                                                  :stop-key stop-key))

  ([channel-key stop-key depth-override]
   (swap! chan-atom update-in [channel-key] assoc :chan (a/chan depth-override)
                                                  :stop-key stop-key)))

(defn new-all-channels!
  "Creates all channels

   Params:
   stop-keys          A map of stop keywords based on the channel name
   depth-override     (optional) Override the default :chan-depth value"

  ([stop-keys]
   (new-channel! :path-chan (:path-chan stop-keys))
   (new-channel! :block-chan (:block-chan stop-keys)))
  ([stop-keys depth-override]
   (new-channel! :path-chan (:path-chan stop-keys) depth-override)
   (new-channel! :block-chan (:block-chan stop-keys) depth-override)))

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

  (println (str "chan-key: " chan-key " chan atom: " @chan-atom))
  (put! chan-key (-> @chan-atom chan-key :stop-key)))

(defn stop-all-channels!
  "Stops all channels and then closes them"
  []

  (put! :path-chan (-> @chan-atom :path-chan :stop-key))
  (put! :block-chan (-> @chan-atom :block-chan :stop-key)))
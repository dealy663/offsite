(ns offsite-cli.system-utils
   ^{:author       "Derek Ealy <dealy663@gmail.com>"
     :organization "http://grandprixsw.com"
     :date         "1/15/2022"
     :doc          "Offsite Client System Utilities for interacting at the OS level"
     :no-doc       true}
   (:require [clojure.java.shell :as shell]
             [clojure.string :as str]
             [clojure.tools.logging :as log])
   (:import [java.net InetAddress]
            (java.util UUID)))

(defn hostname
   "Returns the name of the system that this Offsite Client is running on."
   []
   (try
      (-> (shell/sh "hostname") (:out) (str/trim))
      (catch Exception _e
         (try
            (str/trim (slurp "/etc/hostname"))
            (catch Exception _e
               (try
                  (.getHostName (InetAddress/getLocalHost))
                  (catch Exception _e
                     nil)))))))

(defn offsite-user-id
   "Returns the Offsite ID for this current instance"
   []

   ;; this is fixed for now, but will need to have infrastructure support in the future so that
   ;; it can be handed out when a new user is registered via the Offsite Service
   ;; it would eventually be stored locally and also available for retrieval via the Offsite Service
   {:user-uuid (UUID/fromString "d5405da0-9d36-40c4-ac39-cebc7b95938f")})

(defn key-info
   "Returns key info for signing, encryption, decryption of blocks

    Params:
    key-token    A valid key retrieval token (to be implemented in the future"
   [key-token]

   (when (= key-token :unimplemented)
      {:public-key  ""
       :private-key ""}))

(defn get-sys-info
   "Returns system info about this Offsite Client instance."
   []

   {:hostname     (hostname)
    :offsite-user {:id         (offsite-user-id)
                   :key-info   (key-info :unimplemented)}})

(defn offsite-id
   "Returns the Offsite ID of this system."
   []

   (str (:user-uuid (offsite-user-id)) "-" (hostname)))

(defmacro dbg [& messages]
   `(log/debug "\n\t***------->>>" (str ~@messages)))


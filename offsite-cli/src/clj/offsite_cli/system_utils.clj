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

;; Default name of backup paths configuration, which is expected to be found at the top-level of the installation
;; directory for the offsite client
(def default-paths-file "backup-paths.edn")
(def paths-config (atom {:paths-file default-paths-file
                         :backup-paths []}))

(def backup-close-states [:paused :completed :halted])

(def regex-pattern-type (type #""))

(defn hostname
   "Returns the name of the system that this Offsite Client is running on."
   []

   ;; This needs to be adjusted, so it will work on Windows also
   (when-let [fq-hostname (try
                           (-> (shell/sh "hostname") (:out) (str/trim))
                           (catch Exception _e
                              (try
                                 (str/trim (slurp "/etc/hostname"))
                                 (catch Exception _e
                                    (try
                                       (.getHostName (InetAddress/getLocalHost))
                                       (catch Exception _e
                                          nil))))))]
      (-> fq-hostname (str/split #"\.") first)))

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

   (.toLowerCase (str (:user-uuid (offsite-user-id)) "-" (hostname))))

(defmacro log-msg [level & messages]
   "Writes a log messages at the specified level, adds an additional string to make it standout

   Params:
   level      The log level to write at
   messages   Data to write out in the log message"
   `(clojure.tools.logging/logf ~level "\n\t***------->>> %s" (str ~@messages)))

(defmacro debug [& messages]
   "Writes a debug log message

   Params:
   messages     Data to write out in the log message"
   `(log-msg :debug ~@messages))

(defmacro info [& messages]
   "Writes a info log message

   Params:
   messages     Data to write out in the log message"
   `(log-msg :info ~@messages))

(defmacro warn [& messages]
   "Writes a warn log message

   Params:
   messages     Data to write out in the log message"
   `(log-msg :warn ~messages))

(defmacro error [& messages]
   "Writes a error log message

   Params:
   messages     Data to write out in the log message"
   `(log-msg :error ~messages))

(defmacro payload-gen
   "Returns a function that will generate a payload map for the supplied payload-key context. It is expected
   that this will be called within a let binding so that payloads can easily be generated for publishing.

   Ex.    (let [payload (payload-gen :fn-name)]
            ...
            (ch/m-publish :msg (payload (str value msg))))

   Params:
   payload-key    The context key which will function as a second topic->event discriminator in the :msg event handler"
   [payload-key]
   `(fn [p#] {:event-type ~payload-key :payload p#}))

(defn get-client-info
   "Returns the client info map.

    This needs to be updated to read from an EDN file created at install time."
   []

   {:client-id   42
    :public-key  "invalid key"
    :quality     100})

(defn regex?
   "Returns true if the parameter is a regular expression pattern, otherwise returns false

   Params:
   pattern      The potential regular expression pattern"
   [pattern]

   (= regex-pattern-type (type pattern)))
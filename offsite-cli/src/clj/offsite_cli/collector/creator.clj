(ns offsite-cli.collector.creator
  (:require [clojure.core.async :as a]
            [clojure.java.io :as io]
            [offsite-cli.channels :refer :all]
            [mount.core :refer [defstate]]))

;(def chan-depth 10)
;(def intake-chan (a/chan chan-depth))
(def stop-key :stop-creator)

(declare start stop)

(defstate creator-atom
          :start (do
                   (start)
                   (atom {:intake-q  []
                          :started   false}))
          :stop (stop))

;(defn create-block [{:keys [path exclusions] :as path-defs}]
;  "Create a backup block
;
;  params:
;  path-defs     A backup path, with possible exclusions
;
;  returns:     A backup block"
;  (if-let [file-dir (if (nil? (:file-dir path)) (io/file path))
;        block   {:root-path  (.getCanonicalPath file-dir)
;                 :file-dir    file-dir
;                 :size       (if (.isDirectory file-dir) 0 (.length file-dir))}]
;    ;; only add the :exclusions kv pair if the exclusions vector has data
;    (if (or (nil? exclusions) (empty? exclusions))
;            block
;            (assoc block :exclusions exclusions))))

;(defn path-listener
;  "Creates a go block to wait for new backup paths to be added and then creates
;   an onsite block to represent the path and adds it to the block channel"
;  []
;
;  (a/go
;    (println "creator: in loop thread, started: " (:started @creator-atom))
;
;    (while (:started @creator-atom)
;      (println "creator: waiting for onsite backup item, started: " (:started @creator-atom))
;      (let [backup-path (take! :path-chan)]
;        (when-not (= :stop-creator backup-path)
;          (println (str "received backup path: " backup-path))
;
;          (->> backup-path create-block (put! :block-chan))
;          #_(dosync (alter creator-atom update-in [:intake-q] conj backup-item))
;          #_(fsm/send fsm-svc {:type :enqueued :block block}))))
;
;    (close! :path-chan)
;    (println "creator stopped")))
;
;(defn start []
;  "Start process to wait for new paths from which to create onsite blocks. This will
;   run in its own thread."
;
;  (when-not (:started @creator-atom)
;    (println "starting creator, started: " (:started @creator-atom))
;
;    (dosync (alter creator-atom assoc-in [:started] true))
;    (new-channel! :path-chan stop-key)
;    (path-listener)))
;
;(defn stop []
;  "Stops the block-processor, will wait for all blocks in queue to be finished."
;
;  (when (:started @creator-atom)
;    (println "creator-atom: stopping")
;
;    (dosync (alter creator-atom assoc-in [:started] false))
;    (put! :path-chan stop-key)))

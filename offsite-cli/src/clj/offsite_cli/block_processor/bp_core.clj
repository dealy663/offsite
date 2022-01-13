(ns offsite-cli.block-processor.bp-core
  (:require [clojure.core.async :as a]
            #_[statecharts.core :as fsm]))

;; keeping the channel buffer small for now to limit overuse of CPU during the backup process
(def chan (a/chan 10))

(def bp-data (ref {:started     false
                   :halt        false
                   :queue       []}))


(defn add-block [state event]
  "Adds a block to the processing channel

   Params:
   state     The state of the bp-fsm
   event     The event that triggered this fn call"

  (println "handling unprocessed block: " state " event: " event)
  ;  (a/>!! chan (:block event))
  ;(dosync
  ;  (alter bp-data update-in [:queue] conj block))
  )

(defn process-block [state event]
  "Handles an onsite block. If it refers to a file then it is read, disintegrated, encrypted and broadcast.
   If it is a directory then a new block for the directory is created and entered into the block queue"

  ;; after block has been processed remove it from block-data
  ;; (dosync (alter block-data update-in [:queue] rest)
  )

;(defn add-block [backup-block]
;  "Add a backup block to the block-data queue
;
;  params:
;  backup-block     A block of data for backup"
;  (alter bp-data update-in :queue conj backup-block))

;(defn start []
;  "Start processing block-data from the queue"
;
;  (dosync (alter bp-data assoc-in :started true))
;
;  (let [machine (fsm/machine bfsm/machine)
;        service (fsm/service machine)]
;    (fsm/start service)
;
;    (a/go
;      (while (:started @bp-data)
;        (let [block (a/<!! chan)]
;          (sync
;            (alter bp-data update-in [:queue] conj block))
;          (fsm/send service :unprocessed))))))
;
;(defn stop []
;  "Stops the block-processor, will wait for all blocks in queue to be finished."
;
;  (dosync (alter bp-data assoc-in :started false )))
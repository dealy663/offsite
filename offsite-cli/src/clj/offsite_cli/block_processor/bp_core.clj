(ns offsite-cli.block-processor.bp-core
  (:require [clojure.core.async :as a]
            ;[offsite-cli.collector.creator :as cr]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.channels :refer :all]))

(def stop-key :stop-bp)
(def bp-state (ref {:started        false
                    :halt           false
                    :onsite-block-q []}))


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

(defn process-file [file-block]
  "Processes an onsite block which represents a file. The file can be read, disintegrated, encrypted
   and broadcast to nodes for offsite backup.

   Params:
   file-block     The onsite block representing a file")

(defn process-dir [dir-block]
  "Processes an onsite block which represents a directory. The directory will have all of its
   children enqueued for block processing and finally the directory itself will be entered
   into the onsite DB and broadcast to nodes for offsite backup.

   Params:
   dir-block     The onsite block representing a directory"

  (doseq [file (.listFiles (:file-dir dir-block))]
    (dosync (alter bp-state update-in [:queue] conj (col/create-block {:file-dir file})))))

(defn process-block [block]
  "Handles an onsite block. If it refers to a file then it is read, disintegrated, encrypted and broadcast.
   If it is a directory then a new block for the directory is created and entered into the block queue

   Params:
   block        The onsite block to be processed"

  ;; If the block is a file
  (if (.isDirectory (:file-dir block))
    (process-dir block)
    (process-file block))
  ;; after block has been processed remove it from block-data
  (dosync (alter bp-state update-in [:onsite-block-q] rest)))

(defn get-client-info
  "Returns the client info map"
  []

  {:client-id   42
   :public-key  "invalid key"
   :quality     100})

;; Maybe we will leave it to the nodes to figure out the distribution of copies
;; to other nodes
(defn request-nodes
  "Reach out to the offsite-svc to get a group of nodes that it thinks will be good
   candidates for the offsite-blocks to be sent to.

   Params:
   client-info      A map of details about this client :client-id :uptime :speed :quality etc"
  [client-info]

  ;; will need to make rest call against offsite-svc
  ;; for now this returns placeholder node-info maps for testing
  [{:node-id  1
    :quality  100}
   {:node-id  2
    :quality  50}
   {:node-id  3
    :quality  25}])

(defn broadcast
  "Send the split, encrypted offsite block out to offsite-nodes for remote backup.

   Params:
   offsite-block      A block representing a file ready for offsite backup"
  [offsite-block]

  (request-nodes (get-client-info)))

(defn encrypt
  "Encrypt an offsite-block with the user's key. Then pass on to broadcast"
  [offsite-block]

  ;; add logic to encrypt the file block
  (broadcast offsite-block))

(defn split
  "Splits an offsite file block into pieces no larger than max-block-size

   Params:
   offsite-block      A block representing a file ready for offsite backup"

  [offsite-block]

  ;; add logic for breaking up the file and passing it off to encrypt
  (encrypt offsite-block))

(defn backup-offsite
  "Define the process for prepping an offsite-block for backup and broadcasting the prepped
   offsite-block to the nodes provided by offsite-svc

   Params:
   offsite-block    The block representing a file or directory ready for offsite backup"
  [offsite-block]

  (-> offsite-block split encrypt broadcast))

(defn block-listener
  "Starts a thread which listens to the :block-chan for new blocks which need
   to be processed"
  []

  (a/go
    (println "bp: in loop thread, started: " (:started @bp-state))

    (while (:started @bp-state)
      (println "bp: waiting for block, started: " (:started @bp-state))
      (let [block (take! :block-chan)]
        (when-not (= stop-key block)
          (println (str "received block: " block))
          (dosync (alter bp-state update-in [:onsite-block-q] conj block))
          #_(fsm/send fsm-svc {:type :enqueued :block block}))))

    (println "Closing :block-chan.")
    (close! :block-chan)

    (println "block-processor stopped")))

(defn offsite-block-listener
  "Starts a thread which listens to the offsite-block channel for blocks that are
   ready for offsite broadcast"
  []

  (a/go
    (println "OBL: started loop thread for offsite-blocks")
    (while (:started @bp-state)
      (println "OBL: waiting for next offsite block")
      (let [offsite-block (take! :offsite-block-ch)]
        (when-not (= stop-key offsite-block)
          (println (str "OBL: received offsite block: " (:root-dir offsite-block)))
          (backup-offsite offsite-block))))))
;(defn add-block [backup-block]
;  "Add a backup block to the block-data queue
;
;  params:
;  backup-block     A block of data for backup"
;  (alter bp-data update-in :queue conj backup-block))

(defn start []
  "Start processing block-data from the queue"

  (when-not (:started @bp-state)
    (dosync (alter bp-state assoc-in :started true))
    (new-channel! :block-chan       stop-key)
    (new-channel! :offsite-block-ch stop-key)
    (block-listener)
    (offsite-block-listener)))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @bp-state)
    (dosync (alter bp-state assoc :started false))
    (put! :block-chan       stop-key)
    (put! :offsite-block-ch stop-key)))
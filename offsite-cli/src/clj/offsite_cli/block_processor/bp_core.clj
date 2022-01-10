(ns offsite-cli.block-processor.bp-core)

(def bp-data (ref {:started     false
                   :halt        false
                   :queue       []}))

(defn process-block [backup-block]
  "TODO: Write block processor"

  ;; after block has been processed remove it from block-data
  ;; (dosync (alter block-data update-in [:queue] rest)
  )

(defn add-block [backup-block]
  "Add a backup block to the block-data queue

  params:
  backup-block     A block of data for backup"
  (alter bp-data update-in :queue conj backup-block))

(defn start []
  "Start processing block-data from the queue"

  (dosync (alter bp-data assoc-in :started true))
  (doseq [block (-> bp-data :q first)]
    (process-block block)))

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (dosync (alter bp-data assoc-in :started false )))
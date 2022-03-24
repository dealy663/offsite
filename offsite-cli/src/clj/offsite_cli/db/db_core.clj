(ns offsite-cli.db.db-core
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [mount.core :refer [defstate]]
            [offsite-cli.system-utils :as su]
            [offsite-cli.config :refer [env]]
            [clojure.tools.logging :as log])
  (:import (java.util UUID)))

(defstate db-node*
  :start (doto (xt/start-node (:xtdb-config env))
           xt/sync)
  :stop (-> db-node* .close))

(defn start-db! [env]
  (doto (xt/start-node (:xtdb-config env))
    xt/sync))

(defn stop-db! [node]
  (.close node))

(defn- full-query!
  "Queries the whole DB and will bring it all into memory, this should only be used during testing"
  []
  (xt/q
    (xt/db db-node*)
    '{:find [(pull e [*])]
      :where [[e :xt/id id]]}))

(defn get-entity!
  "Fetch a full entity doc from XTDB by its ID

  @Params
  entity-id     The :xt/id of the document being requested"
  [entity-id]

  (xt/entity (xt/db db-node*) entity-id))

(defn easy-ingest!
  "Use XTDB put transaction to add a vector of documents to the db-node*

  @Params
  doc      A vector of documents to add to the XTDB

  Returns the #inst timestamp of the documents"
  [docs]

  (xt/await-tx
    db-node*
    (xt/submit-tx db-node*
                  (vec (for [doc docs]
                         [::xt/put doc])))))

(defn get-last-backup!
  "Returns the DB doc for the last or current backup."
  []

  (let [backup-doc (get-entity! (su/offsite-id))]
    (if (empty? backup-doc)
      (log/warn "No previous backup document exists, this should only happen once after first install.")
      backup-doc)))

(defn start-backup!
  "Creates a new backup operation in the DB.

   Params:
   backup-paths     The set of paths that make up this backup
   backup-type      Can be either :scheduled or :adhoc

   Returns the #inst of this DB TX and the ID of the backup that was created"
  [backup-paths backup-type]

  ;; probably need to do a match here to ensure that only one backup is active at a time
  (let [last-backup         (get-last-backup!)
        current-backup-uuid (UUID/randomUUID)
        matcher             [::xt/match
                             (su/offsite-id)
                             (assoc last-backup :in-progress false)]
        db-put              [[::xt/put
                              {:xt/id        (su/offsite-id)
                               :backup-id    current-backup-uuid
                               :backup-type  backup-type
                               :backup-paths backup-paths
                               :in-progress  true
                               :close-state  nil
                               :onsite-paths []}]]
        db-put (if-not (nil? last-backup)                   ;; If a backup record already exists
                 `[~matcher ~@db-put]                       ;; this is an interesting way to kinda cons on to the front of a vector and still maintain a vector
                 db-put)                                    ;; otherwise a standalone ::xt/put will do
        tx-inst (xt/await-tx db-node* (xt/submit-tx db-node* db-put))
        success? (xt/tx-committed? db-node* tx-inst)]
    (if success?
      (log/info "Successfully started backup, id: " current-backup-uuid)
      (do
        (let [msg (if (:in-progress last-backup)
                    (str "The previous backup: " (:backup-id last-backup) " is still running")
                    "An error occurred when fetching last backup details from DB.")]
          (log/error "An error has occurred when trying to start a new backup, " msg))))
    {:backup-id   current-backup-uuid
     :tx-inst     tx-inst
     :tx-success? success?}))

(defn stop-backup!
  "Stop the ongoing backup and set it's state to :halted.

   Params:
   close-reason      Reason for stopping backup, member of su/backup-close-states [:paused :completed :halted].

   Returns the #tx-inst for the close operation"
  [close-reason]

  (when-let [last-backup (get-last-backup!)]
    ;(su/dbg "got last-back: " last-backup)
    (if-let [close-state (:close-state last-backup)]
      (log/warn "The last backup had already been stopped with close-state: " close-state)
      (let [tx-inst  (easy-ingest! [(assoc last-backup :close-state close-reason :in-progress false)])
            success? (xt/tx-committed? db-node* tx-inst)]
        (if success?
          (log/info "Successfully closed backup, id: " (:backup-id last-backup))
          (log/error "An error has occurred when trying to close backup: " (:backup-id last-backup)))
        tx-inst))))

(defn add-path-block!
  "Add onsite path(s) to the actively running backup

   Params:
   onsite-block    A path to an onsite file-dir to be scheduled for backup

   Returns an #inst of the DB TX"
  [onsite-block]

  (easy-ingest! [onsite-block]))

(defn get-entity-tx
  "Returns transaction details for an entity

   Params:
   xt-id       The ::xt/id of the entity"
  [xt-id]

  (xt/entity-tx (xt/db db-node*) xt-id))

(defn get-entity-history
  "Returns the transaction history of an entity.

   Params:
   xt-id        The ::xt/id of the entity
   sort-order   (optional - default :desc) #{:asc, :desc}
   options      (optional - default nil)
                * `:with-docs?` (boolean, default false): specifies whether to include documents in the entries under the `::xt/doc` key
                * `:with-corrections?` (boolean, default false): specifies whether to include bitemporal corrections in the sequence, sorted first by valid-time, then tx-id.
                * `:start-valid-time`, `:start-tx-time`, `:start-tx-id` (inclusive, default unbounded): bitemporal co-ordinates to start at
                * `:end-valid-time`, `:end-tx-time`, `:end-tx-id` (exclusive, default unbounded): bitemporal co-ordinates to stop at

                No matter what `:start-*` and `:end-*` parameters you specify, you won't receive results later than the valid-time and tx-id of this DB value.

                Each entry in the result contains the following keys:
                * `::xt/valid-time`,
                * `::xt/tx-time`,
                * `::xt/tx-id`,
                * `::xt/content-hash`
                * `::xt/doc` (see `with-docs?`)"
  ([xt-id]
   (get-entity-history xt-id :desc))

  ([xt-id sort-order]
   (get-entity-history xt-id sort-order nil))

  ([xt-id sort-order options]
   (xt/entity-history (xt/db db-node*) xt-id sort-order options)))

(defn- get-orphaned-blocks
  "Returns a sequence of IDs of all blocks that don't have a valid backup-id. For now this only
   returns blocks with a nil backup-id, this should be smart enough to query for the
   existence of a supplied backup-id. Not sure if it should check on the parent-id."
  []

  (let [orphan-block-ids (xt/q
                          (xt/db db-node*)
                            '{:find   [e]
                              :where [[e :backup-id nil]]})]
    orphan-block-ids))

(defn get-entity-tx-time
  "Return the tx-instance(s) indicated by the selector.

  Params:
  xt-id         The ::xt/id of the entity
  selector      #{:eldest :newest :all} tx-time is an instance already retrieved.
                :eldest - the #inst of the oldest entity
                :newest - the #inst of the youngest entity
                :all - all #instances of the entity

  Returns a vector of #inst"
  [xt-id selector]

  (cond
    (= :newest selector)
    [(-> xt-id get-entity-tx ::xt/tx-time)]

    (= :eldest selector)
    [(-> xt-id (get-entity-history :asc) first :xtdb.api/tx-time)]

    (= :all selector)
    (mapv #(:xtdb.api/tx-time %) (get-entity-history xt-id))

    :else (do
            (log/warn selector " is an invalid selector, must be from #{:eldest :newest :all}"))))


(defn traverse-entities
  "Transform entities"
  [f coll]

  (fn
    ;([inst-opts coll]
    ; ())

    ([inst-opts coll sync?]
     (xt/submit-tx db-node*
                   (mapv #(let [tx-time (get-entity-tx-time (first %) inst-opts)]
                            (f tx-time %)) coll)))))

(defn- delete-entities
  "A sequence of xt/id values for the entities to be deleted.

   Params:
   inst-opts   #{:eldest :newest :all}  (required) - one keyword from this set indicating which #inst to delete
               #{:delete :evict}        (required) - one keyword indicating to delete or evict
   id-seq      The IDs of the entities to be deleted (a seq of vectors containing xt/id values
   sync?       (optional - default = true) Wait for DB transaction fo finishe before return"
  ([inst-opts id-seq]
   (delete-entities inst-opts id-seq true))

  ([inst-opts id-seq sync?]
   (xt/submit-tx db-node*
                 (mapv #(let [tx-time (get-entity-tx-time (first %) inst-opts)]
                          (println "got tx-time " tx-time)
                          (-> (concat [::xt/delete] %)
                              (concat tx-time)
                              vec)) id-seq))
   (when sync?
     (xt/sync db-node*))))

(defn- evict-entities
  "Event multiple entities from XTDB

   Params:
   id-seq      The xt/IDs of the entities to be deleted (a seq of vectors containing xt/id values)
   inst-opts   #{:eldest :newest :all}  one keyword from this set indicating which #inst to delete
   sync?       (optional - default true) If true does a sync operation before returning"
  ([id-seq inst-opts]
   (evict-entities id-seq inst-opts true))

  ([id-seq inst-opts sync?]
   (let [f (fn []
             (xt/submit-tx db-node*
                           (mapv #(let [tx-time (get-entity-tx-time (first %) inst-opts)]
                                    (-> (concat [::xt/evict] %)
                                        (concat tx-time)
                                        vec)) id-seq)))]
     (if sync?
       (xt/await-tx db-node* (f))
       (f)))))

(defn- evict-ents
  [inst-opts id-seq]

  (traverse-entities (fn [tx-time xt-id]
                       (into (concat [::xt/evict xt-id]) tx-time)) id-seq))

(defn get-all-path-blocks
  "Retrieves a group of path-blocks

   Params:
   backup-id   The ID of the backup in progress
   count       (optional - default 1) The number of path-blocks to retrieve from DB,
               a count of -1 will return all path blocks (be careful about memory usage)

   Returns a lazy seq of path-blocks"
  ([backup-id count]

   (let [all-paths-set (xt/q
                         (xt/db db-node*)
                         '{:find  [(pull e [*])]
                           :where [[e :backup-id backup-id]
                                   [e :data-type :path-block]]})]
     (if (> 0 count)
       all-paths-set
       (take count all-paths-set))))

  ([backup-id]
   (get-all-path-blocks backup-id 1)))

(defn list-all-path-ids
  "Generate a sequence of path-block IDs belonging to a backup

  Params:
  backup-id        The :xt/id of the backup

  Returns a sequence of :xt/id"
  [backup-id]

  (let [uuid (if (string? backup-id) (UUID/fromString backup-id) backup-id)]
    (xt/q
      (xt/db db-node*)
      '{:find  [e]
        :where [[e :backup-id bid]
                [e :data-type :path-block]]
        :in    [bid]} uuid)))

(defn- delete-backup
  "Deletes all backup entities: path-blocks and the backup entity itself

   Params:
   backup-id     The :xt/id of the backup

   Returns a sequence of all entities that were deleted"
  [backup-id]

  (let [all-path-ids (concat (list-all-path-ids backup-id) [backup-id])]
    (delete-entities all-path-ids :all)
    all-path-ids))

;(defn backup-xf
;  "Transform a backup's data"
;  [f id-coll]
;
;  (fn [backup-id]
;    (let [ids (into id-coll [[(su/offsite-id)]])]
;      )))
;
(defn- evict-backup
  "Evicts all backup entities: path-blocks and the backup entity itself. This function will first call
   delete-backup.

   Params:
   backup-id     The :xt/id of the backup"
  [backup-id]

  (let [evict-ids (list-all-path-ids backup-id)
        evict-ids (if (empty? evict-ids)
                    [[(su/offsite-id)]]                     ;; this needs to be a seq of xt/id vectors
                    (concat evict-ids [(su/offsite-id)]))]
    (evict-entities evict-ids :all)))

(defn get-path-blocks-lazy
  "Creates a cursor like lazy seq for a backup's path-blocks. When processing from this seq
   the logic should probably be in a with-open expression to auto-close the iterator.

   e.g. (with-open [path-seq (get-path-blocks-lazy backup-id)]
          (doseq [path-block (iterator-seq path-seq)]
            ;; process path into onsite-block
            ...))

   Params:
   backup-id     The ID of the backup in progress

   Returns a lazy iterator-seq of path-blocks"
  ([backup-id]
   (xt/open-q (xt/db db-node*)
              '{:find  [(pull e [*])]
                :where [[e :backup-id backup-id]
                        [e :data-type :path-block]]
                :in    [backup-id]} backup-id)))

(defn get-ofs-block-state!
  "Retrieves the latest file state info from the DB

   Params:
   file-id        The file's path hash code to the file

   Returns a file info map"
  [file-id]

  (let [ofs-block (get-entity! file-id)]
    (when (nil? ofs-block)
      (log/warn "OFS block: " file-id " not found."))
    ofs-block))


;(defn start-xtdb! [env]
;  (letfn [(kv-store [dir]
;            {:kv-store {:xtdb/module 'xtdb.lmdb/->kv-store
;                        :db-dir (io/file dir)
;                        :sync? true}})]
;    (xt/start-node
;      {:xtdb/tx-log         (kv-store "data/dev/tx-log")
;       :xtdb/document-store (kv-store "data/dev/doc-store")
;       :xtdb/index-store    (kv-store "data/dev/index-store")})))
;
;(def xtdb-node (start-xtdb! env))
;
;(defn stop-xtdb! []
;  (.close xtdb-node))
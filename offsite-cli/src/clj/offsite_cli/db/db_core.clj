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
      (log/error "An error has occurred when trying to start a new backup."))
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
    (su/dbg "got last-back: " last-backup)
    (if-let [close-state (:close-state last-backup)]
      (log/warn "The last backup had already been stopped with close-state: " close-state)
      (let [tx-inst  (easy-ingest! [(assoc last-backup :close-state close-reason :in-progress false)])
            success? (xt/tx-committed? db-node* tx-inst)]
        (if success?
          (log/info "Successfully closed backup, id: " (:xt/id last-backup))
          (log/error "An error has occurred when trying to close backup: " (:xt/id last-backup)))
        tx-inst))))

(defn add-path-block!
  "Add onsite path(s) to the actively running backup

   Params:
   onsite-block    A path to an onsite file-dir to be scheduled for backup

   Returns an #inst of the DB TX"
  [onsite-block]

  (easy-ingest! [onsite-block]))

(defn get-all-path-blocks
  "Retrieves a group of path-blocks

   Params:
   backup-id   The ID of the backup in progress
   count       (optional - default 1) The number of path-blocks to retrieve from DB

   Returns a vector of path-blocks"
  ([backup-id count]

   (let [all-paths-set (xt/q
                         (xt/db db-node*)
                         '{:find  [(pull e [*])]
                           :where [[e :backup-id backup-id]
                                   [e :data-type :path-block]]})]
     (first all-paths-set)))

  ([backup-id]
   (get-all-path-blocks backup-id 1)))

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
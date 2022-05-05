(ns offsite-cli.db.db-core
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [mount.core :refer [defstate]]
            [offsite-cli.system-utils :as su]
            [offsite-cli.config :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import (java.util UUID Date)))

(declare db-node*)

(defstate db-node*
  :start (doto (xt/start-node (:xtdb-config env))
           xt/sync)
  :stop (-> db-node* .close))

(defn start-db! [env]
  (doto (xt/start-node (:xtdb-config env))
    xt/sync))

(defn stop-db! [node]
  (.close node))

(s/def ::tx-id int?)
(s/def ::tx-time #(instance? Date %))
(s/def ::xt-timestamp (s/keys :req [:xtdb.api/tx-time :xtdb.api/tx-id]))

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
  ([docs]
   ;(cp/with-cp-node [db-node* (cp/get-node)])
   (easy-ingest! docs db-node*))

  ([docs node]

   (xt/await-tx
     node
     (xt/submit-tx node
                   (vec (for [doc docs]
                          [::xt/put doc]))))))

(s/fdef easy-ingest!
        :args (s/coll-of map?)

        :ret ::xt-timestamp)

(defn get-last-backup!
  "Returns the DB doc for the last or current backup."
  []

  (let [backup-doc (get-entity! (su/offsite-id))]
    (if (empty? backup-doc)
      (log/warn "No previous backup document exists, this should only happen once after first install.")
      backup-doc)))

(defn get-backup!
  "Fetch the specified backup from the DB

  Params:
  backup-id    The ID of the backup"
  [backup-id]

  (let [backup-id (if (string? backup-id) (UUID/fromString backup-id) backup-id)
        doc       (xt/q
                    (xt/db db-node*)
                    '{:find  [(pull e [*])]
                      :where [[e :backup-id bk-id]
                              [e :xt/id xt-id]]
                      :in    [[bk-id xt-id]]} [backup-id (su/offsite-id)])]
    (-> doc first first)))

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
                              {:xt/id          (su/offsite-id)
                               :backup-id      current-backup-uuid
                               :backup-type    backup-type
                               :backup-paths   backup-paths
                               :in-progress    true
                               :catalog-state  :started
                               :close-state    nil
                               :onsite-paths   []}]]
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
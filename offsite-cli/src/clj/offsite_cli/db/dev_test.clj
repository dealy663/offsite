(ns offsite-cli.db.dev-test
   ^{:author         "Derek Ealy <dealy663@gmail.com>"
     :date           "3/27/22"
     :organization   "http://grandprixsw.com"
     :doc            "DB functions to be used in dev/test that shouldn't be available in production"
     :no-doc         true
     :project        "offsite"}
  (:require
    [offsite-cli.system-utils :as su]
    [offsite-cli.db.db-core :as db]
    [offsite-cli.db.catalog :as dbc]
    [xtdb.api :as xt]
    [clojure.tools.logging :as log]))

(defn- full-query!
  "Queries the whole DB and will bring it all into memory, this should only be used during testing"
  []
  (xt/q
    (xt/db db/db-node*)
    '{:find [(pull e [*])]
      :where [[e :xt/id id]]}))

(defn- get-orphaned-blocks
  "Returns a sequence of IDs of all blocks that don't have a valid backup-id. For now this only
   returns blocks with a nil backup-id, this should be smart enough to query for the
   existence of a supplied backup-id. Not sure if it should check on the parent-id."
  []

  (let [orphan-block-ids (xt/q
                           (xt/db db/db-node*)
                           '{:find   [e]
                             :where [[e :backup-id nil]]})]
    orphan-block-ids))


(defn- delete-entities
  "A sequence of xt/id values for the entities to be deleted.

   Params:
   inst-opts   #{:eldest :newest :all}  (required) - one keyword from this set indicating which #inst to delete
               #{:delete :evict}        (required) - one keyword indicating to delete or evict
   id-seq      The IDs of the entities to be deleted - a seq of vectors containing xt/id values
   sync?       (optional - default = true) Wait for DB transaction fo finished before return"
  ([inst-opts id-seq]
   (delete-entities inst-opts id-seq true))

  ([inst-opts id-seq sync?]
   (xt/submit-tx db/db-node*
                 (mapv #(let [tx-time (db/get-entity-tx-time (first %) inst-opts)]
                          (println "got tx-time " tx-time)
                          (-> (concat [::xt/delete] %)
                              (concat tx-time)
                              vec)) id-seq))
   (when sync?
     (xt/sync db/db-node*))))

(defn- evict-entities
  "Event multiple entities from XTDB

   Params:
   id-seq      The xt/IDs of the entities to be deleted (a seq of vectors containing xt/id values)
   inst-opts   #{:eldest :newest :all}  one keyword from this set indicating which #inst to delete
   sync?       (optional - default true) If true does a sync operation before returning"
  ([id-seq inst-opts]
   (evict-entities id-seq inst-opts true))

  ([id-seq inst-opts sync?]
   (su/info "Evicting the following entities: " id-seq)
   (let [f (fn []
             (xt/submit-tx db/db-node*
                           (mapv #(let [tx-time (db/get-entity-tx-time (first %) inst-opts)]
                                    (-> (concat [::xt/evict] %)
                                        (concat tx-time)
                                        vec)) id-seq)))]
     (if sync?
       (xt/await-tx db/db-node* (f))
       (f)))))

(defn- evict-ents
  [inst-opts id-seq]

  (db/traverse-entities (fn [tx-time xt-id]
                       (into (concat [::xt/evict xt-id]) tx-time)) id-seq))

(defn- delete-backup
  "Deletes all backup entities: path-blocks and the backup entity itself

   Params:
   backup-id     The :xt/id of the backup

   Returns a sequence of all entities that were deleted"
  [backup-id]

  (let [all-path-ids (concat (dbc/list-backup-path-ids backup-id) [backup-id])]
    (delete-entities all-path-ids :all)
    all-path-ids))

(defn- force-evict-all-path-blocks
  "Forcibly removes all path blocks from the DB regardless of which backup they belong to."
  []

  (evict-entities (dbc/list-all-path-ids) :all))

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

  (let [evict-ids (dbc/list-backup-path-ids backup-id)
        evict-ids (if (empty? evict-ids)
                    [[(su/offsite-id)]]                     ;; this needs to be a seq of xt/id vectors
                    (concat evict-ids [[(su/offsite-id)]]))]
    (evict-entities evict-ids :all)))


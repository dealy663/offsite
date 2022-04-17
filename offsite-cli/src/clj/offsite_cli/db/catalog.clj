(ns offsite-cli.db.catalog
   ^{:author         "Derek Ealy <dealy663@gmail.com>"
     :date           "3/28/22"
     :organization   "http://grandprixsw.com"
     :doc            "DB functions for the catalog process"
     :no-doc         true
     :project        "offsite"}
  (:require
    [offsite-cli.db.db-core :as dbc]
    [xtdb.api :as xt]
    [clojure.tools.logging :as log]
    [clojure.string :as str])
  (:import (java.util UUID)))


(defn add-path-block!
  "Add onsite path(s) to the actively running backup

   Params:
   onsite-block    A path to an onsite file-dir to be scheduled for backup

   Returns an #inst of the DB TX"
  [onsite-block]

  (dbc/easy-ingest! [onsite-block]))

(defn catalog-complete!
  "Update the backup doc to show that the catalog process has completed.

  Params:
  backup-id     The :xt/id of the backup that has finished cataloging"
  [backup-id]

  (when-let [backup (dbc/get-backup! backup-id)]
    (if (:in-progress backup)
      (dbc/easy-ingest! [(assoc backup :catalog-state :complete)])
      (log/warn "Cannot update the state of a backup that is no longer in progress"))))

(defn get-all-path-blocks
  "Retrieves a group of path-blocks

   Params:
   backup-id   The ID of the backup in progress
   count       (optional - default 1) The number of path-blocks to retrieve from DB,
               a count of -1 will return all path blocks (be careful about memory usage)

   Returns a seq of path-blocks"
  ([backup-id count]

   (let [all-paths-set (xt/q
                         (xt/db dbc/db-node*)
                         '{:find  [(pull e [*])]
                           :where [[e :backup-id backup-id]
                                   [e :data-type :path-block]]})]
     (if (> 0 count)
       all-paths-set
       (take count all-paths-set))))

  ([backup-id]
   (get-all-path-blocks backup-id 1)))

(defn get-root-path-blocks
  "Returns a sequence of root path-blocks for a given backup

  Params:
  backup-id       The ID of the backup to query"
  [backup-id]

  (xt/q
    (xt/db dbc/db-node*)
    '{:find [(pull e [*])]
      :where [[e :backup-id backup-id]
              [e :data-type :path-block]
              [e :parent-id nil]]}))

(defn get-child-path-blocks
  "Returns a sequence of path-blocks that are children of the queried path-block

  Params:
  path-block-id     The :xt/id of a path-block"
  [path-block-id]

  (xt/q
    (xt/db dbc/db-node*)
    '{:find [(pull e [*])]
      :where [[e :data-type :path-block]
              [e :parent-id pb-id]]
      :in    [pb-id]} path-block-id))

(defn list-backup-path-ids
  "Generate a sequence of path-block IDs belonging to a backup

  Params:
  backup-id        The :xt/id of the backup

  Returns a sequence of :xt/id"
  [backup-id]

  (let [uuid (if (string? backup-id) (UUID/fromString backup-id) backup-id)]
    (xt/q
      (xt/db dbc/db-node*)
      '{:find  [e]
        :where [[e :backup-id bid]
                [e :data-type :path-block]]
        :in    [bid]} uuid)))

(defn list-all-path-ids
  "Returns a sequence of all path-block IDs in the DB"
  []

  (xt/q
    (xt/db dbc/db-node*)
    '{:find [e]
      :where [[e :data-type :path-block]]}))

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
   (xt/open-q (xt/db dbc/db-node*)
              '{:find  [(pull e [*])]
                :where [[e :backup-id backup-id]
                        [e :data-type :path-block]]
                :in    [backup-id]} backup-id)))

(defn find-path-block
  "Fetches a path-block by navigating through the DB following the path defined by parent-id references
  for path blocks belonging to a backup and starting at the given path-block.

  Params:
  starting-path-block-id     The path-block ID of the block to start the search from (could be a root)
  ending-path                The path to search for can be as short as the filename, but might need to include
                             some of it's preceding directories in case the file is found in multiple paths
                             and only one is desired.

  Returns a vector of path-blocks which match the search criteria"
  [starting-path-block-id ending-path]

  (loop [acc               []
         child-path-blocks (get-child-path-blocks starting-path-block-id)]
    (if (empty? child-path-blocks)
      acc
      (let [block (-> child-path-blocks first first)]
        (if (str/ends-with? (:root-path block) ending-path)
          (recur (conj acc block) (rest child-path-blocks))
          (recur acc (concat (rest child-path-blocks) (get-child-path-blocks (:xt/id block)))))))))
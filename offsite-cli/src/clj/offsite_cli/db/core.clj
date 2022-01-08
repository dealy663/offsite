(ns offsite-cli.db.core
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [mount.core :refer [defstate]]
            [offsite-cli.config :refer [env]]))

(defstate db-node*
  :start (doto (xt/start-node (:xtdb-config env))
           xt/sync)
  :stop (-> db-node* .close))

(defn start-db! [env]
  (doto (xt/start-node (:xtdb-config env))
    xt/sync))

(defn stop-db! [node]
  (.close node))

(defn full-query []
  (xt/q
    (xt/db db-node*)
    '{:find [(pull e [*])]
      :where [[e :xt/id id]]}))

(defn get-entity [entity-id]
  "Fetch a full entity doc from XTDB by its ID

  @Params
  entity-id     The :xt/id of the document being requested"
  (xt/entity (xt/db db-node*) entity-id))

(defn easy-ingest [docs]
  "Use XTDB put transaction to add a vector of documents to the db-node*

  @Params
  doc      A vector of documents to add to the XTDB

  Returns the #inst timestamp of the documents"
  (xt/await-tx
    db-node*
    (xt/submit-tx db-node*
                  (vec (for [doc docs]
                         [::xt/put doc])))))

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
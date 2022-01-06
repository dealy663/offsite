(ns offsite-cli.db.core
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [mount.core :refer [defstate]]
            [offsite-cli.config :refer [env]]))

;(defstate node
;          :start (doto (xt/start-node (:xtdb-config env))
;                   xt/sync)
;          :stop (-> node .close))

(defn start-db! [env]
  (doto (xt/start-node (:xtdb-config env))
    xt/sync))

(defn stop-db! [node]
  (.close node))

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
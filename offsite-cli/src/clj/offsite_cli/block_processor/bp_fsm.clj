;(ns offsite-cli.block-processor.bp-fsm
;  (:require [statecharts.core :as fsm]
;            [clojure.core.async :as a]
;            [mount.core :refer [defstate]]
;            [offsite-cli.channels :refer :all]
;            [offsite-cli.block-processor.bp-core :as bp]))
;
;(declare create-machine start stop)
;
;(defstate fsm-svc
;          :start (let [machine (create-machine)
;                       service (fsm/service machine)]
;                   (start service))
;          :stop (stop))
;
;
;(defn- create-machine []
;  (fsm/machine
;    {:id      :onsite-block
;     :initial :unprocessed
;     :context nil
;     :states  {:unread      {:on {:read {:target  :integrated
;                                         :actions (fn [onsite-block & _]
;                                                    (println (str "Splitting bloc: " (:id onsite-block))))}}}
;               :broadcast   {:on {:confirmed {:target  :unprocessed
;                                              :actions (fn [onsite-block & _]
;                                                         (println (str "Block " (:id onsite-block) " was pushed.")))}}}
;               :integrated  {:on {:disintegrated {:target  :crypto
;                                                  :actions (fn [onsite-block & _]
;                                                             (println (str "Encrypting block: " (:id onsite-block))))}}}
;               :distribute  {:on {:targets-ready {:target  :broadcast
;                                                  :actions (fn [ready-block & _]
;                                                             (println (str "Broadcasting block: " (:id ready-block))))}}}
;               :crypto      {:on {:encrypted {:target  :distribute
;                                              :actions (fn [encrypted-block & _]
;                                                         (println (str "Distributing block: " (:id encrypted-block))))}}}
;               :unprocessed {:on {:enqueued {:target  :unread
;                                             :actions bp/add-block}}}}}))
;
;
;(defn start [service]
;  "Start processing block-data from the queue, does nothing if the
;   block-processor is already running"
;
;  (when-not (:started @bp/bp-state)
;    (println "starting block-processor, started: " (:started @bp/bp-state))
;    (dosync (alter bp/bp-state assoc-in [:started] true)))
;
;  (fsm/start service)
;
;
;  service)
;
;(defn stop []
;  "Stops the block-processor, will wait for all blocks in queue to be finished."
;
;  (when (:started @bp/bp-state)
;    (println "bp: stopping")
;    (dosync (alter bp/bp-state assoc-in [:started] false))
;    (put! :onsite-block-chan :stop-bp)))
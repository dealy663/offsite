(ns offsite-cli.block-processor.bp-fsm
  (:require [statecharts.core :as fsm]
            [clojure.core.async :as a]
            [mount.core :refer [defstate]]
            [offsite-cli.block-processor.bp-core :as bp]))

(declare create-machine start stop)

(defstate fsm-svc
          :start (let [machine (create-machine)
                       service (fsm/service machine)]
                   (start service))
          :stop (stop))


(defn- create-machine []
  (fsm/machine
    {:id      :onsite-block
     :initial :unprocessed
     :context nil
     :states  {:unread      {:on {:read {:target  :integrated
                                         :actions (fn [onsite-block & _]
                                                    (println (str "Splitting bloc: " (:id onsite-block))))}}}
               :broadcast   {:on {:confirmed {:target  :unprocessed
                                              :actions (fn [onsite-block & _]
                                                         (println (str "Block " (:id onsite-block) " was pushed.")))}}}
               :integrated  {:on {:disintegrated {:target  :crypto
                                                  :actions (fn [onsite-block & _]
                                                             (println (str "Encrypting block: " (:id onsite-block))))}}}
               :distribute  {:on {:targets-ready {:target  :broadcast
                                                  :actions (fn [ready-block & _]
                                                             (println (str "Broadcasting block: " (:id ready-block))))}}}
               :crypto      {:on {:encrypted {:target  :distribute
                                              :actions (fn [encrypted-block & _]
                                                         (println (str "Distributing block: " (:id encrypted-block))))}}}
               :unprocessed {:on {:enqueued {:target  :unread
                                             :actions bp/add-block}}}}}))

(defn start [service]
  "Start processing block-data from the queue, does nothing if the
   block-processor is already running"

  (dosync
    (when-not (:started @bp/bp-data)
      (println "starting block-processor")
      (alter bp/bp-data assoc-in [:started] true))

    (fsm/start service)

    (a/go
      (println "bp: in loop thread")
      (dosync
        (while (:started @bp/bp-data)
          (println "bp: waiting for block")
          (let [block (a/<!! bp/chan)]
            (when-not (= :stop-bp block)
              (println (str "received block: " block))
              (alter bp/bp-data update-in [:queue] conj block)
              (fsm/send fsm-svc {:type :enqueued :block block})))))

      (println "block-processor stopped")))
  service)

(defn stop []
  "Stops the block-processor, will wait for all blocks in queue to be finished."

  (when (:started @bp/bp-data)
(println "bp: stopping")
    (sync (alter bp/bp-data assoc-in [:started] false))
    (a/>!! bp/chan :stop-bp)))
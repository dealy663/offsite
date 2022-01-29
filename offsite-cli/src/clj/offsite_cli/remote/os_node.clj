(ns offsite-cli.remote.os-node
  ^{:author       "Derek Ealy <dealy663@gmail.com>"
    :date         "1/26/22"
    :organization "http://grandprixsw.com"
    :doc          "Some Clojure Program"
    :no-doc       true}
  (:require [org.httpkit.client :as client]
            [java-time :as t]))

(defn send-block!
  "Sends an offsite block to a remote node for storage

   Params:
   node            An Offsite-node connection
   offsite-block   A block representing a file or sub-block ready for storage on a remote Offsite-node

   Returns a response indicating the success of the remote operation"
  [node offsite-block]

  (let [start-time (t/instant)]
    (println "Stored block: " (:xt/id offsite-block) " to node: " (:node-id node))
    {:status   200
     :node     node
     :duration (- (t/instant) start-time)}))
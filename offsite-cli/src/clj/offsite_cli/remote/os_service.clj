(ns offsite-cli.remote.os-service
   ^{:author         "Derek Ealy <dealy663@gmail.com>"
     :date           "1/26/22"
     :organization   "http://grandprixsw.com"
     :doc            "Offsite-Cli functions for interacting with remote servers"
     :no-doc         true}
  (:require [org.httpkit.client :as client]))


;; Maybe we will leave it to the nodes to figure out the distribution of copies
;; to other nodes
(defn request-nodes
   "Reach out to the offsite-svc to get a group of nodes that it thinks will be good
    candidates for the offsite-blocks to be sent to.

    Params:
    client-info      A map of details about this client :client-id :uptime :speed :quality etc

    Returns a vector of offsite node info"
   [client-info]

   ;; will need to make rest call against offsite-svc
   ;; for now this returns placeholder node-info maps for testing
   [{:node-id       1
     :quality       100
     :last-response -1}
    {:node-id       2
     :quality       50
     :last-response -1}
    {:node-id       3
     :quality       25
     :last-response -1}])
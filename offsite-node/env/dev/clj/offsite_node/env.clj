(ns offsite-node.env
  (:require
    [selmer.parser :as parser]
    [clojure.tools.logging :as log]
    [offsite-node.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[offsite-node started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[offsite-node has shut down successfully]=-"))
   :middleware wrap-dev})

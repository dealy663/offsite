(ns offsite-node.env
  (:require [clojure.tools.logging :as log]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[offsite-node started successfully]=-"))
   :stop
   (fn []
     (log/info "\n-=[offsite-node has shut down successfully]=-"))
   :middleware identity})

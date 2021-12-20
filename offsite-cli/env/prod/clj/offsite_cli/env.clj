(ns offsite-cli.env
  (:require [clojure.tools.logging :as log]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[offsite-cli started successfully]=-"))
   :stop
   (fn []
     (log/info "\n-=[offsite-cli has shut down successfully]=-"))
   :middleware identity})

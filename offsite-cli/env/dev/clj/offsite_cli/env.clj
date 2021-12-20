(ns offsite-cli.env
  (:require
    [selmer.parser :as parser]
    [clojure.tools.logging :as log]
    [offsite-cli.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[offsite-cli started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[offsite-cli has shut down successfully]=-"))
   :middleware wrap-dev})

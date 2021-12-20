(ns offsite-svc.env
  (:require
    [selmer.parser :as parser]
    [clojure.tools.logging :as log]
    [offsite-svc.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[offsite-svc started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[offsite-svc has shut down successfully]=-"))
   :middleware wrap-dev})

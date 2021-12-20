(ns offsite-svc.app
  (:require [offsite-svc.core :as core]))

;;ignore println statements in prod
(set! *print-fn* (fn [& _]))

(core/init!)

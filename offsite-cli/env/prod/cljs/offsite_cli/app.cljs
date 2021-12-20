(ns offsite-cli.app
  (:require [offsite-cli.core :as core]))

;;ignore println statements in prod
(set! *print-fn* (fn [& _]))

(core/init!)

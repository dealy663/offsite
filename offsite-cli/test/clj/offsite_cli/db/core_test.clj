(ns offsite-cli.db.core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.core :refer [db-node*]]
            [xtdb.api :as xt]
            [mount.core :as mount]))

(defn- with-components
  [components f]
  (apply mount/start components)
  (f)
  (apply mount/stop components))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.core/db-node*] %))

(defn full-query
  [node]
  (xt/q
    (xt/db node)
    '{:find [(pull e [*])]
      :where [[e :xt/id id]]}))

(deftest test-core
  (testing "simple xtdb write and query"
    (xt/submit-tx db-node* [[::xt/put
                              {:xt/id "hi2u"
                               :user/name "zig"}]])
    (xt/sync db-node*)
    (let [result (xt/q (xt/db db-node*) '{:find  [e]
                                           :where [[e :user/name "zig"]]})
          id (-> result first first)]
      (is (= "hi2u" id)))))


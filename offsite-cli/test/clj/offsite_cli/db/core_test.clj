(ns offsite-cli.db.core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.db.core :refer :all]
            [xtdb.api :as xt]))

(deftest test-core
  (testing "simple xtdb write and query"
    (xt/submit-tx xtdb-node [[::xt/put
                              {:xt/id "hi2u"
                               :user/name "zig"}]])
    (let [result (xt/q (xt/db xtdb-node) '{:find  [e]
                                           :where [[e :user/name "zig"]]})
          id (-> result first first)]
      (is (= "hi2u" id)))))

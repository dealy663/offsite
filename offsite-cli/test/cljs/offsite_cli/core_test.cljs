(ns offsite-cli.core-test
  (:require [cljs.test :refer-macros [is are deftest testing use-fixtures]]
            [pjstadig.humane-test-output]
            [reagent.core :as reagent :refer [atom]]
            [offsite-cli.core :as rc]))

(deftest test-home
  #_ (is (= true true))
  (is (= true false)))


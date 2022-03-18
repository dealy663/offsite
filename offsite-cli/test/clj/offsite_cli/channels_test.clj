(ns offsite-cli.channels-test
  ^{:author       "Derek Ealy <dealy663@gmail.com>"
    :organization "http://grandprixsw.com"
    :project      "offsite"
    :date         "2/5/22"
    :doc          ""
    :no-doc       true}
  (:require [clojure.test :refer :all]
            [offsite-cli.channels :refer :all]
            [clojure.core.async :as a]
            [mount.core :as mount]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.system-utils :as su]))

(defn- with-components
  [components f]
  ;(apply mount/start components)
  (new-channel! :onsite-block-chan bp/stop-key)
  (f)
  (reset-channels!)
  ;(apply mount/stop components)
  )

(use-fixtures
  :each
  #(with-components [#'offsite-cli.config/env
                     #_#'offsite-cli.db.db-core/db-node*] %))

(deftest new-publisher!-test
  (testing "Create a new publisher on a channel"
    (let [pub (new-publisher! :onsite-block-chan :test-pub)]
      (is (some? pub)
          "A publisher should have been created on :onsite-block-chan"))
    (let [new-ch (new-channel! :new-chan :new-chan-stop)
          pub    (new-publisher! :new-chan :test-new-pub)]
      (is (some? pub)
          "A publisher should have been created on :new-chan")
      (is (some? (get-in @channels [:map :new-chan :publishers :test-new-pub]))
          "The newly created publisher should be in the channels ref")
      (let [foo-pub (new-publisher! :new-chan :foo-pub)]
        (is (some? foo-pub)
            "A channel should be able to support multiple publishers")
        (let [pub (get-in @channels [:map :new-chan :publishers :foo-pub])]
          (is (= foo-pub pub)
              "The publisher returned should match the one stored in the channels ref"))))))

(deftest subscribe-test
  (let [foo-chan  (new-channel! :foo-chan :foo-chan-stop)
        pub       (new-publisher! :foo-chan :topics)
        <listener (subscribe :foo-chan :topics :subject-foo {:timeout 2000})
        payload   {:topics :subject-foo :data :bar}]
    (put! :foo-chan payload)
    (if-let [msg (a/<!! <listener)]
      (is (= payload msg)
          "The message :foo should already be ready before we take from <listener")
      (throw (Exception. "Message never arrived on subscribed channel")))))


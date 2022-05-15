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
            [manifold.bus :as mb]
            [manifold.deferred :as md]
            [manifold.stream :as ms]
            [offsite-cli.system-utils :as su]
            [offsite-cli.channels :as ch]))

(defn- with-components
  [components f]
  ;(apply mount/start components)
  #_(new-channel! :onsite-block-chan bp/stop-key)
  (f)
  #_(reset-channels!)
  ;(apply mount/stop components)
  )

(use-fixtures
  :each
  #(with-components [#'offsite-cli.config/env
                     #_#'offsite-cli.db.db-core/db-node*] %))

#_(deftest new-publisher!-test
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
          (is (= (:publisher foo-pub) pub)
              "The publisher returned should match the one stored in the channels ref"))))))

(defn take-and-print
  [<channel prefix >router-ch]
  (a/go-loop []
    (if-let [result (a/<! <channel)]
      (do
        (su/debug "TAP got - " prefix ": " result)
        ;(a/>! >router-ch {:routes :waiter :action inc})
        (recur))
      (su/debug prefix " - done!"))))

#_(deftest subscribe-test
  (testing "Subscribing to topics on a channel."
    (su/debug "Pre channels: " (-> @channels :map keys))
    (let [timer 2000
          test-bus (a/timeout timer)
          test-pub (a/pub test-bus :topic-1)
          foo-chan (new-channel! :foo-chan :foo-chan-stop)
          foo-pub (new-publisher! :foo-chan :topic)
          ;_ (su/dbg "created publication: " foo-pub #_(select-keys foo-pub [:chan-key :topic]))
          promise (promise)
          payload {:topic-1 :subject-bar :data promise}
          monitor (fn [msg]
                    (su/debug "monitor got msg: " msg)
                    (when-let [match (if (= payload msg) :success :failed)]
                      (deliver (:data msg) :done)))
          <listener (subscribe foo-pub :subject-foo monitor {:timeout timer})
          <test-listener (a/timeout timer)
          ]
      ;(a/sub test-pub :subject-bar <test-listener)
      (su/debug "Post channels: " (-> @channels :map keys))
      (su/debug "putting payload on :foo-chan")
      ;(a/put! test-bus payload)
      (put! :foo-chan payload true)

      (let [result (deref promise timer :timeout)]
        (is (= :done result)
            "Promise was never realized in monitor function"))
      (a/close! <test-listener)
      (a/close! test-bus)
      (a/unsub-all test-pub)
      #_(if-let [msg (a/<!! <listener)]
        (do
          (su/debug "<test-listener got: " msg)
          (is (= :success (:status msg))
              "The message :foo should already be ready before we take from <listener"))
        (is false "The message didn't arrive before the channel timed out."))
      ))

  #_(testing "Multiple subscribers to a single subject"
    (let [bar-chan      (new-channel! :bar-chan :generic-stopper)
          bar-pub       (new-publisher! :bar-chan :routes)
          chan42        (new-channel! :ch42 :generic-stopper)
          pub2          (a/pub chan42 :routes)
          <listenerA    (subscribe bar-pub :subject-biz {:timeout 2000})
          ;<lA           (a/timeout 2000)
          ;<lB           (a/timeout 2000)
          ;<lWait        (a/timeout 2000)
          ;<listenerB    (subscribe :bar-chan :routes :subject-biz {:timeout 2000})
          ;<waitListener (subscribe :bar-chan :routes :waiter {:timeout 2000})
          payload {:routes :subject-biz :data :mar-kee}
          payload-baz {:routes :subject-baz :data :mar-kee}
          monitorA      (fn [msg]
                          (if msg
                            (do
                              (su/debug "channels: " (:map @channels))
                              (put! :bar-chan {:routes :waiter :inc 1})
                              (is (= payload msg)
                                  "The message :biz should already be ready before we take from <listenerA"))
                            (throw (Exception. "Message never arrived on subscribed channelA"))))]
      ;(a/sub pub2 :subject-biz <lA)
      ;(a/sub pub2 :subject-biz <lB)
      ;(a/sub pub2 :waiter <lWait)
      ;      (put! :bar-chan payload)
      ;(a/>!! chan42 payload)
      ;(put! :bar-chan payload-baz)
      ;(take-and-print <lA "bizA" chan42)
      ;(take-and-print <lB "bizB" chan42)
      ;(a/go
      ;  (if-let [msgA (a/<!! <listenerA)]
      ;    (do
      ;      (su/dbg "channels: " (:map @channels))
      ;      (put! :bar-chan {:routes :waiter :inc 1})
      ;      (is (= payload msgA)
      ;          "The message :biz should already be ready before we take from <listenerA"))
      ;    (throw (Exception. "Message never arrived on subscribed channelA"))))
      ;(a/timeout 250)                                     ;; wait for a quarter of a second
      ;(if-let [msgB (a/<!! <listenerB)]
      ;  (do
      ;    (put! :bar-chan {:routes :waiter :inc 1})
      ;    (is (= payload msgB)
      ;        "The message :baz should already be ready before we take from <listenerB"))
      ;  (throw (Exception. "Message never arrived on subscribed channelB")))
      #_(loop [cc 0]
          (if (<= cc 1)
            (let [result (a/<!! <waitListener)]
              (is (some? result)
                  "<waitListener was terminated likely due to a timeout")
              (recur (+ cc (:inc result))))
            )))))

#_(deftest multi-streams-ex
  (testing "multiple streams example"
    (let [>publisher (a/timeout 2000)
          publication (a/pub >publisher :routes #_#(:routes %))
          <subscriber-1 (a/timeout 2000)
          <subscriber-2 (a/timeout 2000)
          <subscriber-3 (a/timeout 2000)]
      (a/sub publication :account-created <subscriber-1)
      (a/sub publication :account-created <subscriber-2)
      (a/sub publication :user-logged-in <subscriber-2)
      (a/sub publication :change-page <subscriber-3)

      (take-and-print <subscriber-1 "subscriber-one" >publisher)
      (take-and-print <subscriber-2 "subscriber-two" >publisher)
      (take-and-print <subscriber-3 "subscriber-three" >publisher)

      (a/go
        (a/>! >publisher {:routes :change-page :dest "/#home"}))
      #_(a/go
        (a/>! >publisher {:routes :account-created :username "billy"})))))

(def pub-channel (a/chan 1))
(def publisher (a/pub pub-channel :tag))
(def print-channel (a/chan 1))

(defn run-print-channel
  []
  (a/go-loop []
                 (when-let [value (a/<! print-channel)]
                   (println value)
                   (recur))))

(defn close-channels
  []
  (a/close! pub-channel)
  (a/close! print-channel))

(defn subscribe1
  [publisher subscriber tags callback]
  (let [channel (a/timeout 2000)]
    (doseq [tag tags]
      (a/sub publisher tag channel))
    (a/go-loop []
                   (when-let [value (a/<! channel)]
                     (a/>! print-channel (callback subscriber tags value))
                     (recur)))))

(defn callback
  [subscriber tags value]
  (pr-str
    (format "%s Got message:%s"
            subscriber value)))

(defn send-with-tags
  [channel msg]
  (doseq [tag (:tags msg)]
    (println "sending... " tag)
    (a/>!! channel {:tag tag
                        :msg (:msg msg)})))

#_(deftest pub-sub-callbacks
  (testing "another example"
    (run-print-channel)

    (subscribe1 publisher "I care about DOGS!" [:dogs] callback)
    (subscribe1 publisher "I care about *cats*." [:cats] callback)
    (subscribe1 publisher "I'm all about cats and dogs." [:dogs :cats] callback)

    (send-with-tags pub-channel {:msg "New Dog Story" :tags [:dogs]})
    (send-with-tags pub-channel {:msg "New Cat Story" :tags [:cats]})
    (send-with-tags pub-channel {:msg "New Pet Story" :tags [:dogs :cats]})

    (close-channels)))

(defn tap
  [<stream prefix]
  (a/go-loop []
    (if-let [result (ms/take! <stream)]
      (do
        (su/debug "TAP got - " prefix ": " result)
        ;(a/>! >router-ch {:routes :waiter :action inc})
        (recur))
      (su/debug prefix " - done!"))))

(defn manifold-init
  []

  (let [bus (mb/event-bus)]
    (mb/subscribe bus :dogs)
    (mb/subscribe bus :cats)
    (mb/subscribe bus :dogs)
    bus))

(defn manifold-ex
  [])

#_(deftest pub-sub-manifold
  (testing "Manifold usefulness"
    (let [bus (mb/event-bus)
          sub1 (mb/subscribe bus :dogs)
          sub2 (mb/subscribe bus :cats)
          sub3 (mb/subscribe bus :dogs)
          pub1 (mb/publish! bus :dogs "New Dog Story")
          pub2 (mb/publish! bus :cats "New Cat Story")]
      )))

(defn monitor! [name publisher topic]
  (su/debug "Created monitor: " name)
  (let [<listen (a/chan 1)]
    (a/sub publisher topic <listen)
    (a/go-loop []
      (if-let [{:keys [message]} (a/<! <listen)]
        (do
          (su/debug name ": " topic \- message)
          (recur))
        (su/debug "closing monitor: " name)))))

(defn start []
  (let [<publish (a/chan 1)
        publisher (a/pub <publish :source)
        ]
    (monitor! "alice1" publisher :alice)
    (monitor! "bob" publisher :bob)
    (a/put! <publish {:source :alice :message "Alice says Hi!"})
    (a/put! <publish {:source :bob :message "Bob says Hi!"})
    (a/put! <publish {:source :john :message "John says Hi!"})
    (a/put! <publish {:source :alice :message "Alice says Hi again"})
    ;    (a/timeout 5000)
    (Thread/sleep  2000)
    (monitor! "alice2" publisher :alice)
    (monitor! "alice3" publisher :alice)
    (monitor! "alice4" publisher :alice)
    (a/put! <publish {:source :alice :message "Alice says to her friends"})
    (a/close! <publish)
    ))

#_(deftest stop-all-channels!-test
  (let [ch1 (ch/new-channel! :ch-1 :stop-ch {:timeout 2000})
        ch2 (ch/new-channel! :ch-2 :stop-ch {:timeout 2000})]
    (is (not (:closed (ch/get-channel-info :ch-1)))
        "ch-1 should not be closed at start of test2")
    (is (not (:closed (ch/get-channel-info :ch-2)))
        "ch-2 should not be closed at start of test")
    (ch/close-all-channels! ch/channels)
    (is (:closed (ch/get-channel-info :ch-1))
        "ch-1 should be closed after calling stop-all-channels")
    (is (:closed (ch/get-channel-info :ch-2))
        "ch-2 should be closed after calling stop-all-channels")))

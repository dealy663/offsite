(ns offsite-cli.block-processor.bp-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.block-processor.bp-core :refer [start]]
            [clojure.core.async :as a]
            [offsite-cli.channels :as ch]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.system-utils :as su]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.init :as init]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.test-utils :as tu])
  (:import [java.io File]
           (org.slf4j LoggerFactory)
           (ch.qos.logback.classic Level)))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(defn- with-components
  [components f]
  (apply mount/start components)
  ;;(bp/start bp-start-test-impl)
  (f)
  (apply mount/stop components)
  ;(ch/stop-all-channels!)
  #_(bp/stop bp-stop-test-impl))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.db.db-core/db-node*] %))

;(doto
;  (LoggerFactory/getLogger "xtdb.query")
;  (.setLevel (Level/valueOf "warn")))

(defn simple-block-handler
  [done-chan]

  (fn [block]
    (when-not (= block bp/stop-key)
      (su/dbg "Received block: " block))
    (a/>!! done-chan block)))

(defn chan-start-test-impl
  "Test implementation of start, can be overridden in test by passing customized body

   Returns a function with the start logic"
  [handler]

  (su/dbg "returning testing start logic fn, with handler: " handler)
  (fn []
    (ch/new-channel! :onsite-block-chan bp/stop-key)
    ;(ch/new-channel! :offsite-block-chan bp/stop-key)
    (bp/onsite-block-listener handler)
    #_(offsite-block-listener)))

(defn chan-stop-test-impl
  "Test implementation of stop"
  []

  (ch/put!d :onsite-block-chan bp/stop-key)
  ;(ch/put! :onsite-block-chan bp/stop-key)
  #_(ch/put! :offsite-block-chan bp/stop-key)
  :stopped)

(defn simple-start-test-impl
  "Test implementation of start, can be overridden in test by passing customized body

   Returns a function with the start logic"
  []

  (fn []
    :started))

(defn simple-stop-test-impl
  "Test implementation of stop"
  []

  :stopped)


;(defn bp-test-onsite-block-handler
;  "Overridable logic for handling onsite blocks to prep for cataloguing and backup
;
;   Params:
;   backup-cfg     The backup configuration that holds the backup paths
;   done-chan      A channel that the test is waiting on to know when this handler logic is complete
;
;   Returns a function that can be injected into the block processor listener, which will be able to execute
;   tests on the block and make use of the configuration wrapped in this closure"
;  [backup-cfg done-chan]
;
;  (fn [block]
;    (when-not (= bp/stop-key block)
;      (su/dbg "received block: " block)
;      (is (= false (empty? block))
;          "A valid block should have been created before sending to the onsite-block-q")
;      (is (= (-> backup-cfg second :path) (-> block :file-dir .getPath))
;          "The relative paths of the backup-cfg should match that of the block")
;      (is (= true (-> block :file-dir .isDirectory))
;          "The :file-dir should be: test/backup-data/music which is a directory")
;      (is (= 0 (-> block :size))
;          "A directory block should have a size of 0 at this point"))
;    (a/>!! done-chan :test-complete)))
;
;(deftest get-ofs-block-state-test
;  (testing "Initial creation of an ofs-block-state map"
;    (let [backup-cfg (:backup-paths (init/get-paths (str test-configs-dir "/backup-paths.edn")))
;          done-chan  (a/timeout 1000)
;          _ (su/dbg "backup-cfg: " backup-cfg)]
;      ;; start the block processor but inject local implementations for startup and handling blocks
;      (bp/start (bp-start-test-impl (bp-test-onsite-block-handler backup-cfg done-chan)))
;      ;; kick off the process of creating a block and waiting for it to be handled
;      (let [new-block (col/create-block (second backup-cfg))]
;        (su/dbg "Added block to onsite-block-channel, val: " new-block)
;        (is (= :test-complete (a/<!! done-chan))
;            "Unexpected value on done-chan, expecting :test-complete"))
;      (bp/stop bp-stop-test-impl)



      ;#_(let [block (col/create-block (second backup-cfg))]
      ;    (su/dbg "block that was created: " block)))))

;(deftest process-file-test
;  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
;        file-path   (-> backup-cfg :backup-paths first)]
;    (testing "creation of a file-state map"
;      (let [block     (col/create-block file-path)
;            _ (su/dbg "created block: " block "\n from path: " file-path)
;            file-state (bp/process-file block)]))))

(deftest start-stop-test
  (testing "Start and stop of bp-core functions as expected"
    (is (= false (:started @bp/bp-state)))
    (is (= :started (bp/start (simple-start-test-impl)))
        "The simple start implementation should just return :started")
    (is (= true (:started @bp/bp-state)))
    (su/dbg "stopping simple-stop-test-impl")
    (is (= :stopped (bp/stop simple-stop-test-impl))
        "The simple start implementation should just return :stopped")
    (is (= false (:started @bp/bp-state)))

    ;(let [done-chan (a/timeout 5000)]
    ;  (bp/start (chan-start-test-impl (simple-block-handler done-chan)))
    ;  (is (= true (:started @bp/bp-state)))
    ;  (ch/put! :onsite-block-chan :test-block)
    ;  ;(a/>!! (ch/get-ch :onsite-block-chan) :test-block)
    ;  (is (= :test-block (a/<!! done-chan))
    ;      "The item pushed on to the :onsite-block-chan should match the one received on the done-chan, if result is nil then done-chan timed out"))
    ;(su/dbg "stopping chan-stop-test-impl\n\tCHANNEL INFO\n\t" (ch/get-all-channels-info))
    ;(is (= :stopped (bp/stop chan-stop-test-impl)))
    #_(is (= false (:started @bp/bp-state)))))

(deftest make-block-info-test
  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        file-path   (-> backup-cfg :backup-paths first)]
    (testing "Validate creating a block-info map"
      (let [block      (col/create-block file-path)
            block-info (bp/make-block-info (:file-dir block))
            file        (io/file (:path file-path))]
        (is (= true (some? block-info))
            "A valid file-block should return a valid block-info map")
        (is (= (.hashCode file) (:xt/id block-info))
            "The block-info :xt/id should match the file's hashcode")
        (is (= (if (.isDirectory file) :dir :file) (:type block-info))
            "The block-info type should have the expected value of :dir or :file")
        (is (= (.isHidden file) (:hidden? block-info))
            "The block-info hidden status should match that of the file it points to.")
        (is (= (bp/get-checksum file) (:checksum block-info))
            "The block-info checksum should match that of the file it represents")
        (is (= (.lastModified file) (:last-modified block-info))
            "The block-info last-modified time should match that of the file it represents")))))

(deftest create-ofs-block-state-test
  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        file-path   (-> backup-cfg :backup-paths first)
        block      (col/create-block file-path)
        block-info (bp/make-block-info (:file-dir block))]
    (testing "create-ofs-block-state"
      (is (= nil (bp/create-ofs-block-state nil))
          "Creating ofs-block for a non-existing block-info should return nil")
      (let [block-state (bp/create-ofs-block-state block-info)
            tx-info     (db/easy-ingest! block-state)
            _ (su/dbg "returned block-state: " block-state)
            file         (:file-dir block)]
        (tu/validate-ofs-block block-state tx-info)))))


(deftest process-file-test
  (let [                                                    ;backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        tmp-file    (File/createTempFile "tmp" nil)
        file-path   {:path (.getPath tmp-file)}]
    (testing "Processing a block representing a single file"
      (let [block       (col/create-block file-path)
            block-info  (bp/make-block-info (:file-dir block))
            block-state (bp/create-ofs-block-state block-info)
            tx-info     (db/easy-ingest! block-state)
            file-state   (bp/process-file block)]
        (is (= true (some? file-state))
            "Processing a valid file block should not return nil")
        (su/dbg "process-file gave back file-state: " file-state)
        (is (= (if (.isDirectory tmp-file) :dir :file) (:type file-state))
            "The block being processed is a file, the state map should match")))))

(ns offsite-cli.block-processor.bp-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.block-processor.bp-core :refer [start]]
            [clojure.core.async :as a]
            [offsite-cli.channels :as ch]
            [offsite-cli.block-processor.bp-core :as bpc]
            [offsite-cli.block-processor.onsite :as bpon]
            [offsite-cli.system-utils :as su]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.init :as init]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.test-utils :as tu]
            [clojure.string :as str]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.block-processor.bp-core :as bpc])
  (:import [java.io File]
           [java.nio.file Files]
           (java.util UUID)))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(def current-backup-uuid (UUID/randomUUID))
(def test-backup-info {:backup-id   current-backup-uuid
                       :tx-inst     :test
                       :tx-success? true})

(defn- with-components
  [components f]
  (apply mount/start components)
  (f)
  (apply mount/stop components))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.db.db-core/db-node*
                     #'offsite-cli.channels/channels] %))

;(doto
;  (LoggerFactory/getLogger "xtdb.query")
;  (.setLevel (Level/valueOf "warn")))

#_(defn simple-block-handler
  [done-chan]

  (fn [block]
    (when-not (= block bpc/stop-key)
      (su/debug "Received block: " block))
    (a/>!! done-chan block)))

#_(defn chan-start-test-impl
    [handler]
    "Test implementation of start, can be overridden in test by passing customized body

     Returns a function with the start logic"

  (su/debug "returning testing start logic fn, with handler: " handler)
  (fn []
    (ch/new-channel! :onsite-block-chan bpc/stop-key)
    ;(ch/new-channel! :offsite-block-chan bp/stop-key)
    (bpon/onsite-block-listener handler)
    #_(offsite-block-listener)))

#_(defn chan-stop-test-impl
  "Test implementation of stop"
  []

  (ch/put!d :onsite-block-chan bpc/stop-key)
  ;(ch/put! :onsite-block-chan bp/stop-key)
  #_(ch/put! :offsite-block-chan bp/stop-key)
  :stopped)

(defn simple-start-test-fn
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
    (is (= false (:started @bpc/bp-state)))
    (is (= :started (bpon/start (simple-start-test-fn)))
        "The simple start implementation should just return :started")
    (is (= true (:started @bpc/bp-state)))
    ;(su/dbg "stopping simple-stop-test-impl")
    (is (= :stopped (bpon/stop simple-stop-test-impl))
        "The simple start implementation should just return :stopped")
    (is (= false (:started @bpc/bp-state)))

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
      (let [block      (col/create-path-block (:path file-path))
            block-info (bpc/make-block-info block)
            file        (io/file (:path file-path))]
        (is (= true (some? block-info))
            "A valid file-block should return a valid block-info map")
        (is (= (.hashCode file) (:xt/id block-info))
            "The block-info :xt/id should match the file's hashcode")
        (is (= (if (.isDirectory file) :dir :file) (:block-type block-info))
            "The block-info type should have the expected value of :dir or :file")
        (is (= :offsite-block (:data-type block-info))
            "Block Info is the beginnings of an offsite block and should indicate so")
        (is (= (.isHidden file) (:hidden? block-info))
            "The block-info hidden status should match that of the file it points to.")
        (is (= (bpc/get-checksum file) (:checksum block-info))
            "The block-info checksum should match that of the file it represents")
        (is (= (.lastModified file) (:last-modified block-info))
            "The block-info last-modified time should match that of the file it represents")))))

(deftest create-ofs-block-state-test
  (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        file-path   (-> backup-cfg :backup-paths first)
        block      (col/create-path-block (:path file-path))
        block-info (bpc/make-block-info block)]
    (testing "create-ofs-block-state"
      (is (= nil (bpc/create-ofs-block-state nil))
          "Creating ofs-block for a non-existing block-info should return nil")
      (let [block-state (bpc/create-ofs-block-state block-info)
            tx-info     (db/easy-ingest! block-state)
            file         (:file-dir block)]
        (tu/validate-ofs-block block-state tx-info)))))

(defn get-temp-file
  ""
  ([prefix suffix]

   (let [f (File/createTempFile prefix suffix)]
     (.deleteOnExit f)
     f))

  ([prefix]
   (get-temp-file prefix nil)))

(defn get-temp-dir
  ""
  ([prefix attrs]

   (let [d (Files/createTempDirectory prefix attrs)]
     (-> d .toFile .deleteOnExit)
     d))

  ([prefix]
   (get-temp-dir prefix nil))

  ([]
   (get-temp-dir nil nil)))

(deftest process-file-test
  (let [tmp-file    (get-temp-file "tmp")
        file-path   {:path (.getPath tmp-file)}]
    (.deleteOnExit tmp-file)
    (testing "Processing a block representing a single file"
      (let [block       (col/create-path-block (:path file-path))
            block-info  (bpc/make-block-info block)
            block-state (bpc/create-ofs-block-state block-info)
            tx-info     (db/easy-ingest! block-state)
            file-state   (bpon/process-file block)]
        (is (= true (some? file-state))
            "Processing a valid file block should not return nil")
        (is (= (if (.isDirectory tmp-file) :dir :file) (:block-type file-state))
            "The block being processed is a file, the state map should match")))))

(deftest create-child-dir-test
  (let [backup-cfg   (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        music-path   (-> backup-cfg :backup-paths second)]
    (testing "Validate the creation of a child dir path"
      (let [ons-dir-block (col/create-path-block (:path music-path))
            xs-dir-path   (io/file (str (:path music-path) "/extra-small"))
            child-dir     (bpon/create-child-dir xs-dir-path ons-dir-block)]
        (is (= true (str/ends-with? (-> music-path :path) "music"))
            "The second item in backup-paths.edn should be the music directory")
        (is (= (.hashCode xs-dir-path) (:xt/id child-dir))
            "The hash code of the new child-dir should match that of the file it represents")
        (is (= (.getCanonicalPath xs-dir-path) (:path child-dir))
            "The path of the new child-dir should match that of the file it represents")
        (is (= (:backup-id (col/get-backup-info)) (:backup-id child-dir))
            "The backup-id of the new child-dir should match the one in backup-info")
        (is (= :backup-path (:data-type child-dir))
            "The data-type of the child-dir should be :backup-path")
        (is (= (:xt/id ons-dir-block) (:ons-parent-id child-dir))
            "The :ons-parent-id of the new child-block should point to it's parent dir")))))

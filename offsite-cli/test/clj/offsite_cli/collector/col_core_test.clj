(ns offsite-cli.collector.col-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.col-core :refer :all]
            [offsite-cli.channels :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.system-utils :as su]
            [mount.core :as mount]
            [babashka.fs :as fs]
            [offsite-cli.db.db-core :as db]
            [offsite-cli.db.catalog :as dbc]
            [offsite-cli.collector.col-core :as col]
            [offsite-cli.channels :as ch]
            [manifold.stream :as ms]
            [manifold.deferred :as md]
            [manifold.bus :as mb]
            [clojure.core.async :as a]))

(defn- with-components
  [components f]
  (apply mount/start components)
  (f)
  (apply mount/stop components))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.db.db-core/db-node*
                     #'offsite-cli.channels/channels
                     #'offsite-cli.init/backup-paths] %))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(deftest test-create-block
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "Block creation for single file"
      (let [block     (create-path-block (-> backup-cfg :backup-paths first :path))
            block-file (io/file (:orig-path block))
            file-dir   (io/file (str backup-data-dir "/du.out"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block)))
        (is (nil? (:exclusions block))
            "there should be no exclusions on short-backup-paths.edn")
        (is (= (.hashCode file-dir) (:xt/id block))
            "The path-block's hash code should match the file-dir it represents")
        (is (= file-dir block-file)
            "Loaded file doesn't match expected file")
        (is (= (.length file-dir) (:size block))
            "Block file size doesn't match expected file size")))

    (testing "Block creation with exclusions"
      (let [backup-cfg (init/get-paths (str test-configs-dir "/backup-paths.edn"))
            block      (create-path-block (-> backup-cfg :backup-paths second :path))
            block-dir  (io/file (:orig-path block))
            file-dir    (io/file (str backup-data-dir "/music"))]
        (is (= (.getCanonicalPath file-dir) (:root-path block))
            "Block root-path mismatch")
        (is (= (.hashCode file-dir) (:xt/id block))
            "The path-block's hashcode should match the file-dir it represents")
        (is (= :path-block (:data-type block))
            "A path block should have the :path-block data type")
        (is (.isDirectory block-dir)
            (str "Loaded file-dir (" (-> block :root-path) ") should be a directory"))
        (is (= file-dir block-dir)
            "Loaded file-dir doesn't match expected directory")
        (is (= 0 (:size block))
            "A directory should start with a size of 0")))

    (testing "Block creating for a directory only containing 1 file"
      (let [block    (create-path-block (:path test-small-dir))
            test-dir (io/file (:path test-small-dir))
            test-file (io/file (str test-small-dir "/04 Joe Henry - Monkey.flac"))]
        (is (= (.getCanonicalPath test-dir) (:root-path block))
            "The block's root should match the path originally given to create-bloc")
        (is (nil? (:dirs block))
            "The block should have no sub dirs")))

    (testing "Block creation with empty exclusions"
      (let [block  (create-path-block (:path empty-exclude-path))]
        (is (= nil (:exclusions block))
            "The loaded path exclusions don't match expected values")))

    (testing "Block creation (negative tests)"
      (is (thrown? NullPointerException (create-path-block nil))
          "A nil block should throw an NPE"))))

(defn recurse-callback
  [progress-map]

  (let [{:keys [cwd dir-count file-count byte-count]} progress-map]
    (su/debug "Recursing through: " cwd
              ", dir-count: " dir-count
              ", file-count: " file-count
              ", byte-count: " byte-count)))

(def dir-info-empty {:root       nil
                     :dir-count  0
                     :file-count  0
                     :byte-count 0})

(def dir-info (atom dir-info-empty))

(defn- pre-visit-dir
  [dir attrs]

  (let [dir-path (-> dir .toFile .getCanonicalPath)]
    (if (included? dir-path)
      (do
        #_(su/debug "pre-visiting dir: " dir ", dir-info: " @dir-info)
        ;; The root directory isn't stored in the DB so don't look for it, but it should still
        ;; be added to the dir-count
        (when (> (:dir-count @dir-info) 0)
          (is (some #(= (str dir) (-> % first :orig-path)) (:all-path-blocks @dir-info))
              (str "The path blocks fetched from DB don't contain path: " dir)))
        (swap! dir-info update :dir-count inc)
        :continue)
      :skip-subtree)))

(defn- visit-file
  [path attrs]

  (let [file      (.toFile path)
        file-path (.getCanonicalPath file)]
    (if (included? file-path)
      (do
        ;(su/dbg "visiting file: " path)
        (swap! dir-info update :byte-count + (fs/size file))
        (swap! dir-info update :file-count inc)
        :continue)
      :skip-subtree)))

(defn- pre-visit-dir-fn
  [dir-info-atom]

  (fn [dir attrs]
    (let [dir-path (-> dir .toFile .getCanonicalPath)]
      (if (included? dir-path)
        (do
          ;(su/dbg "pre-visiting dir: " dir ", dir-info: " @dir-info)
          ;; The root directory isn't stored in the DB so don't look for it, but it should still
          ;; be added to the dir-count
          (when (> (:dir-count @dir-info-atom) 0)
            (is (some #(= (str dir-path) (-> % first :root-path)) (:all-path-blocks @dir-info-atom))
                (str "The path blocks fetched from DB don't contain path: " dir-path)))
          (swap! dir-info-atom update :dir-count inc)
          :continue)
        :skip-subtree))))

(defn- visit-file-fn
  [dir-info-atom]

  (fn [path attrs]
    (let [file (.toFile path)
          file-path (.getCanonicalPath file)]
      (if (included? file-path)
        (do
          ;(su/dbg "visiting file: " file-path)
          ;(su/dbg "visiting file: " path)
          (swap! dir-info-atom update :byte-count + (fs/size file))
          (swap! dir-info-atom update :file-count inc)
          :continue)
        :skip-subtree))))

(defn fs-get-info-test
  "Returns the number of directories, files and total bytes pointed to by the path. This function does no
   optimizations to avoid blowing the stack. It could have issues on really deep file systems.

   Params:
   path       A path to a directory or file"
  [path]

  (reset! dir-info (assoc dir-info-empty :root            path
                                         :all-path-blocks (dbc/get-all-path-blocks (:backup-id col/get-backup-info))))
  #_(su/debug "got all-path-blocks: " (:all-path-blocks @dir-info))
  (let [file-dir (if (string? path) (fs/file path) path)]
    (fs/walk-file-tree file-dir {:pre-visit-dir pre-visit-dir :visit-file visit-file})
    @dir-info))

(defn fs-get-root-path-info
  "Returns the number of directories, files and total bytes pointed to by the path. This function does no
  optimizations to avoid blowing the stack. It could have issues on deeply nested file systems

  Params:
  root-path     The root path to walk and get info from"
  [root-path]

  (let [dir-info (atom (assoc dir-info-empty :root            root-path
                                             :all-path-blocks (dbc/get-all-path-blocks (:backup-id col/get-backup-info))))]
    (let [file-dir (if (string? root-path) (fs/file root-path) root-path)]
      (fs/walk-file-tree file-dir {:pre-visit-dir (pre-visit-dir-fn dir-info)
                                 :visit-file (visit-file-fn dir-info)})
      @dir-info)))

(defn walk-tree-handler
  "Handles events while walking the directory tree

  Returns a deferred that will hold then number of excluded directories or a :timedout error"
  []

  (let [wt-msg (ch/m-subscribe :walk-tree)
        ret    (md/deferred)]
    (a/go-loop [event              (md/timeout! (ms/take! wt-msg) 1000 :timedout)
                excluded-dir-count 0]
      (cond
        (= :timedout @event) (md/error! ret :timedout)
        (= :complete @event) (md/success! ret excluded-dir-count)
        :else (let [[wt-event _] @event]
                (recur (md/timeout! (ms/take! wt-msg) 1000 :timedout)
                       (if (= :excluded-dir wt-event)
                         (inc excluded-dir-count)
                         excluded-dir-count)))))
    ret))

(deftest walk-paths-test
  (let [backup-cfg         (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        music-root-path    (-> backup-cfg :backup-paths second)
        music-path         (:path music-root-path)
        ;music-exclusions   (:exclusions music-root-path)
        music-block        (create-path-block (-> backup-cfg :backup-paths second :path))]
    (testing "Create a whole directory tree in the DB"
      (let [orig-col-state @col/collector-state
            d-excl         (walk-tree-handler)
            test-dir       (fs/file (:orig-path music-block))
            result         (col/walk-paths test-dir)
            x-dir-count    @d-excl]
        (is (not (= :timedout x-dir-count))
            "walk-tree-handler timedout while waiting for dir exclusion events.")
        (let [sub-dirs       (->> test-dir fs/list-dir (filter #(fs/directory? %)))
              included-count (- (count sub-dirs) x-dir-count)]
          (is (some? result)
              "A valid result is expected after recurse-paths!")
          (is (= (inc included-count) (:dir-count result))
              "The number of directories processed should match the sub-dirs of music-block + 1
               for the music-block root and taking away the count of excluded dirs")
          (let [fs-info (fs-get-info-test music-path)]
            (is (= music-path (:root fs-info))
                "The root of fs-info should match the root that was used to start the search")
            (is (= (:dir-count fs-info) (:dir-count result))
                "The directory count on the file-system doesn't match that returned by recurse-paths!")
            (is (= (:file-count fs-info) (:file-count result))
                "The file count on the file-system doesn't match that returned by recurse-paths!")
            (is (= (:byte-count fs-info) (:byte-count result))
                "The total byte count on the file-system doesn't match that returned by recurse-paths!")))))))

(deftest included?-test
  (testing "included? function"
    (init/get-paths (str test-configs-dir "/backup-paths.edn"))
    (let [music-dir       (fs/file backup-data-dir "music")
          xs-dir          (fs/file backup-data-dir "music/extra-small")
          xs-path         (fs/canonicalize xs-dir)
          music-path      (fs/canonicalize music-dir)
          medium-dir      (fs/file backup-data-dir "music/medium")
          medium-path     (fs/canonicalize medium-dir)
          medium-foo-path (str medium-path "/foo.txt")
          no-backup-path  (str xs-path "/nobackup.txt")
          ignore-path     (str xs-path "/ignore.xcld")
          ds-store-path   (str music-path "/.DS_Store")]
      (is (included? (str music-path))
          (str "The path " (str music-path) " should not be on the exclusion list"))
      (is (not (included? (str medium-path)))
          "The music/medium path should be on the exclusion list")
      (is (not (included? medium-foo-path))
          "Files within the excluded music/medium path should not be included")
      (is (not (included? (str medium-path "/bar/file.txt")))
          "Files within sub-dirs fo the excluded music/medium path should not be included")
      (is (not (included? no-backup-path))
          (str "The file " no-backup-path " should not be included."))
      (is (not (included? ignore-path))
          (str "The file " ignore-path " should not be included."))
      (is (not (included? ds-store-path))
          (str "The file " ds-store-path " should not be included"))))

  (testing "included? negative tests"
    (init/get-paths (str test-configs-dir "/backup-paths.edn"))
    (is (false? (included? nil))
        "Testing if nil is included doesn't make sense and should just return false")
    (is (false? (included? ""))
        "An empty string should not be included as part of the backup effort")))

(defn sum-fs-info
  "Sum the file system info for the root paths"
  [fs-paths]

  (into {} (map (fn [k] {k (reduce + (mapv k fs-paths))}) [:dir-count :file-count :byte-count])))

(deftest start-test
  (testing "collector start test"
    (try
      (let [paths      (:backup-paths (init/get-paths (str test-configs-dir "/backup-paths.edn")))
            s-msg      (ch/m-subscribe :col-msg)
            s-complete (ch/m-subscribe :col-finished)
            result     (col/start (:backup-paths @init/backup-paths))
            d-msg      (md/timeout! (ms/take! s-msg) 2000 :timedout)
            d-complete (md/timeout! (ms/take! s-complete) 2000 :timedout)
            ;_ (su/dbg "got paths: " paths)
            ;_ (su/dbg "got path-blocks: " (dbc/get-all-path-blocks (:backup-id db/get-last-backup!) -1))
            fs-info    (->> paths
                            (map #(fs-get-root-path-info (:path %)))
                            sum-fs-info)]
        (is (not (= :timedout @d-msg))
            "Start event not received within timeout period")
        (is (not (= :timedout @d-complete))
            "Start/collector complete message not received within timeout period")
        (doseq [path paths]
          (is (not (nil? (some #(= path %) (:backup-paths @col/collector-state))))
              (str "Path not found in collector-state - " path)))
        (let [last-backup (db/get-last-backup!)]
          (is (= :complete (:catalog-state last-backup))
              "The collector should indicate that the catalog is complete after processing all backup paths")
          (is (= (:dir-count fs-info) (:dir-count @d-complete))
              "The file system dir count doesn't match the DB dir count")
          (is (= (:file-count fs-info) (:file-count @d-complete))
              "The file system file count doesn't match the DB file count")
          (is (= (:byte-count fs-info) (:byte-count @d-complete))
              "The file system byte count doesn't match the DB byte count")
          (is (= (:byte-count fs-info) (:total-bytes @col/collector-state))
              "The collector-state byte count doesn't match the file system byte count")))
      (finally (col/stop)))))

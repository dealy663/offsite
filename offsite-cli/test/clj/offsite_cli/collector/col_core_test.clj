(ns offsite-cli.collector.col-core-test
  (:require [clojure.test :refer :all]
            [offsite-cli.collector.col-core :refer :all]
            [offsite-cli.channels :refer :all]
            [offsite-cli.init :as init]
            [clojure.java.io :as io]
            [offsite-cli.block-processor.bp-core :as bp]
            [offsite-cli.system-utils :as su]
            [mount.core :as mount]
            [babashka.fs :as fs]))

(defn- with-components
  [components f]
  (apply mount/start components)
  (new-channel! :onsite-block-chan bp/stop-key)
  (f)
  (stop! :onsite-block-chan)
  (apply mount/stop components))

(use-fixtures
  :once
  #(with-components [#'offsite-cli.config/env
                     #'offsite-cli.db.db-core/db-node*] %))

(def test-configs-dir "test/configurations")
(def backup-data-dir "test/backup-data")
(def empty-exclude-path {:path       "test/backup-data/music"
                         :exclusions []})
(def test-small-dir {:path "test/backup-data/music/small"})

(deftest test-create-block
  (let [backup-cfg (init/get-paths (str test-configs-dir "/short-backup-paths.edn"))]
    (testing "Block creation for single file"
      (let [block     (create-root-path-block (-> backup-cfg :backup-paths first))
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
            block      (create-root-path-block (-> backup-cfg :backup-paths second))
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
      (let [block    (create-root-path-block test-small-dir)
            test-dir (io/file (:path test-small-dir))
            test-file (io/file (str test-small-dir "/04 Joe Henry - Monkey.flac"))]
        (is (= (.getCanonicalPath test-dir) (:root-path block))
            "The block's root should match the path originally given to create-bloc")
        (is (nil? (:dirs block))
            "The block should have no sub dirs")))

    (testing "Block creation with empty exclusions"
      (let [block  (create-root-path-block empty-exclude-path)]
        (is (= nil (:exclusions block))
            "The loaded path exclusions don't match expected values")))

    (testing "Block creation (negative tests)"
      (is (thrown? NullPointerException (create-root-path-block nil))
          "A nil block should throw an NPE"))))

(defn recurse-callback
  [progress-map]

  (let [{:keys [cwd dir-count file-count byte-count]} progress-map]
    (su/dbg "Recursing through: " cwd
            ", dir-count: "  dir-count
            ", file-count: "  file-count
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
        (su/dbg "pre-visiting dir: " dir)
        (swap! dir-info update :dir-count inc)
        :continue)
      :skip-subtree)))

(defn- visit-file
  [path attrs]

  (let [file      (.toFile path)
        file-path (.getCanonicalPath file)]
    (when (included? file-path)
      (su/dbg "visiting file: " path)
      (swap! dir-info update :byte-count + (fs/size file))
      (swap! dir-info update :file-count inc)
      :continue)))

(defn fs-get-info
  "Returns the number of directories, files and total bytes pointed to by the path. This function does no
   optimizations to avoid blowing the stack. It could have issues on really deep file systems.

   Params:
   path       A path to a directory or file"
  [path]

  (reset! dir-info (assoc dir-info-empty :root path))
  (let [file-dir (if (string? path) (fs/file path) path)]
    (fs/walk-file-tree file-dir {:pre-visit-dir pre-visit-dir :visit-file visit-file})
    @dir-info))

(deftest recurse-paths!-test
  (let [backup-cfg         (init/get-paths (str test-configs-dir "/backup-paths.edn"))
        music-root-path    (-> backup-cfg :backup-paths second)
        music-path         (:path music-root-path)
        music-exclusions   (:exclusions music-root-path)
        music-block        (create-root-path-block (-> backup-cfg :backup-paths second))]
    ;(su/dbg "got music-block: " music-block)
    (testing "Creating a whole directory tree in the DB"
      (let [result         (recurse-paths! music-path recurse-callback)
            test-dir       (io/file (:orig-path music-block))
            sub-dirs       (->> test-dir .listFiles (filter #(.isDirectory %)))
            included-count (- (count sub-dirs) (count music-exclusions))]
        (is (some? result)
            "A valid result is expected after recurse-paths!")
        ;; add 1 to account for the music dir itself
        (is (= (inc included-count) (:dir-count result))
            "The number of directories processed should match the sub-dirs of music-block + 1
             for the music-block root and taking away the count of excluded dirs")
        (let [fs-info (fs-get-info music-path)]
          (is (= music-path (:root fs-info))
              "The root of fs-info should match the root that was used to start the search")
          (is (= (:dir-count fs-info) (:dir-count result))
              "The directory count on the file-system doesn't match that returned by recurse-paths!")
          (is (= (:file-count fs-info) (:file-count result))
              "The file count on the file-system doesn't match that returned by recurse-paths!")
          (is (= (:byte-count fs-info) (:byte-count result))
              "The total byte count on the file-system doesn't match that returned by recurse-paths!"))))))

(deftest included?-test
  (testing "included? function"
    (init/get-paths (str test-configs-dir "/backup-paths.edn"))
    (let [music-dir       (fs/file backup-data-dir "music")
          music-path      (fs/canonicalize music-dir)
          medium-dir      (fs/file backup-data-dir "music/medium")
          medium-path     (fs/canonicalize medium-dir)
          medium-foo-path (str medium-path "/foo.txt")]
      (is (included? (str music-path))
          "The music path should not be on the exclusion list")
      (is (not (included? (str medium-path)))
          "The music/medium path should be on the exclusion list")
      (is (not (included? medium-foo-path))
          "Files within the excluded music/medium path should not be included")
      (is (not (included? (str medium-path "/bar/file.txt")))
          "Files within sub-dirs fo the excluded music/medium path should not be included")))

  (testing "included? negative tests"
    (init/get-paths (str test-configs-dir "/backup-paths.edn"))
    (is (nil? (included? nil))
        "Testing if nil is included doesn't make sense and should just return nil")
    (is (nil? (included? ""))
        "An empty string should not be included as part of the backup effort")))

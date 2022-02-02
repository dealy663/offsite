(defproject offsite-cli "0.1.0-SNAPSHOT"

  :description "The Offsite Client, for managing backups into the remote Offsite Nodes of the backup
                collective group."
  :url "http://offsite.grandprixsw.com/info"

  :dependencies [[ch.qos.logback/logback-classic "1.2.7"]
                 [cljs-ajax "0.8.4"]
                 [clojure.java-time "0.3.3"]
                 [com.cognitect/transit-clj "1.0.324"]
                 [com.cognitect/transit-cljs "0.8.269"]
                 [cprop "0.1.19"]
                 [day8.re-frame/http-fx "0.2.3"]
                 [expound "0.8.10"]
                 [funcool/struct "1.4.0"]
                 [json-html "0.4.7"]
                 ;                 [com.xtdb/xtdb-core "1.20.0-DME-SNAPSHOT"]
                 [com.xtdb/xtdb-core "dev-SNAPSHOT"]        ;locally built versions from xtdb fork
                 [com.xtdb/xtdb-lmdb "dev-SNAPSHOT"]        ;using lwjgl 3.3.0
                 ;[com.xtdb/xtdb-test "1.20.0-DME-SNAPSHOT"]
                 ;[com.xtdb/xtdb-core "1.19.0-beta1"]
                 ;[com.xtdb/xtdb-rocksdb "1.19.0-beta1"]
                 ;;[com.xtdb/xtdb-rocksdb "1.20.0"]
                 [luminus-http-kit "0.1.9"]
                 [luminus-transit "0.1.3"]
                 [luminus/ring-ttl-session "0.3.3"]
                 [markdown-clj "1.10.7"]
                 [metosin/muuntaja "0.6.8"]
                 [metosin/reitit "0.5.15"]
                 [metosin/ring-http-response "0.9.3"]
                 [mount "0.1.16"]
                 [nrepl "0.8.3"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/clojurescript "1.10.896" :scope "provided"]
                 [org.clojure/core.async "1.4.627"]
;                 [org.clojure/core.async "1.3.644"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.webjars.npm/bulma "0.9.3"]
                 [org.webjars.npm/material-icons "1.0.0"]
                 [org.webjars/webjars-locator "0.42"]
                 [re-frame "1.2.0"]
                 [reagent "1.1.0"]
                 [ring-webjars "0.2.0"]
                 [ring/ring-core "1.9.4"]
                 [ring/ring-defaults "0.3.3"]
                 [org.clj-commons/digest "1.4.100"]
                 [selmer "1.12.45"]
                 [thheller/shadow-cljs "2.16.7" :scope "provided"]
                 [babashka/fs "0.1.2"]

                 ;; statecharts isn't used now and probably should be removed. But keeping comment
                 ;; as this seems like it could be useful in the future
                 #_[clj-statecharts "0.1.1"]]

  :min-lein-version "2.0.0"
  
  :source-paths ["src/clj" "src/cljs" "src/cljc"]
  :test-paths ["test/clj"]
  :resource-paths ["resources" "target/cljsbuild"]
  :target-path "target/%s/"
  :main ^:skip-aot offsite-cli.core

  :plugins [] 
  :clean-targets ^{:protect false}
  [:target-path "target/cljsbuild"]
  

  :profiles
  {:uberjar {:omit-source true
             
             :prep-tasks ["compile" ["run" "-m" "shadow.cljs.devtools.cli" "release" "app"]]
             :aot :all
             :uberjar-name "offsite-cli.jar"
             :source-paths ["env/prod/clj"  "env/prod/cljs" ]
             :resource-paths ["env/prod/resources"]}

   :dev           [:project/dev :profiles/dev]
   :test          [:project/dev :project/test :profiles/test]

   :project/dev  {:jvm-opts ["-Dconf=dev-config.edn" ]
                  :dependencies [[binaryage/devtools "1.0.4"]
                                 [cider/piggieback "0.5.3"]
                                 [org.clojure/tools.namespace "1.1.1"]
                                 [pjstadig/humane-test-output "0.11.0"]
                                 [prone "2021-04-23"]
                                 [re-frisk "1.5.2"]
                                 [ring/ring-devel "1.9.4"]
                                 [ring/ring-mock "0.4.0"]]
                  :plugins      [[com.jakemccrary/lein-test-refresh "0.24.1"]
                                 [jonase/eastwood "0.3.5"]
                                 [cider/cider-nrepl "0.26.0"]] 
                  
                  
                  :source-paths ["env/dev/clj"  "env/dev/cljs" "test/cljs" ]
                  :resource-paths ["env/dev/resources"]
                  :repl-options {:init-ns user
                                 :timeout 120000}
                  :injections [(require 'pjstadig.humane-test-output)
                               (pjstadig.humane-test-output/activate!)]}
   :project/test {:jvm-opts ["-Dconf=test-config.edn" ]
                  :resource-paths ["env/test/resources"] 
                  
                  
                  }
   :profiles/dev {}
   :profiles/test {}})

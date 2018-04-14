(defproject panda "0.1.0-SNAPSHOT"
  :description "Panda - the distributed bmp grayscaler (server part)"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.4.474"]
                 [aleph "0.4.4"]]
  :main ^:skip-aot panda.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})

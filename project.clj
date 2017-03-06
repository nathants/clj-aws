(defproject aws "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [amazonica "0.3.88" :scope "provided"]

                 ;; because of issue in amazonica jackson dep? not sure
                 [com.fasterxml.jackson.core/jackson-databind "2.8.6"]
                 [com.fasterxml.jackson.core/jackson-core "2.8.6"]])

(defproject juxt/crux-redis "0.0.3"
  :description "Crux Redis"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "21.04-1.16.0-beta"]
                 [com.taoensso/nippy "3.1.1"]
                 [io.lettuce/lettuce-core "6.0.2.RELEASE"]]
  :profiles {:test {:dependencies  [[juxt/crux-test "dev-SNAPSHOT"]
                                    [criterium "0.4.6"]]}}
  :pedantic? :warn)

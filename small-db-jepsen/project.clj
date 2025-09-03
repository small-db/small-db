(defproject small-db-jepsen "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main small-db-jepsen.runner
  ;; clojure releases: https://clojure.org/releases/downloads
  ;; jepsen releases: https://github.com/jepsen-io/jepsen/releases
  :dependencies [[org.clojure/clojure "1.12.2"]
                 [jepsen "0.3.9"]
                 [com.github.igrishaev/pg2-core "0.1.40"]]
  :repl-options {:init-ns small-db-jepsen.runner})

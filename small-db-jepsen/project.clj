(defproject small-db-jepsen "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main small-db-jepsen.core
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.1-SNAPSHOT"]]
  :repl-options {:init-ns small-db-jepsen.core})

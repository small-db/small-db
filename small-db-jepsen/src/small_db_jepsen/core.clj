(ns small-db-jepsen.core
  (:require
   [clojure.java.shell :refer [sh]]
   [clojure.string :as str]
   [clojure.tools.logging :refer [info]]
   jepsen.cli
   jepsen.control
   jepsen.control.util
   jepsen.db
   jepsen.os.debian
   jepsen.tests))

(defn copy-dynamic-libs
  "Get dynamic libraries and copy them to /tmp/lib/ on VM"
  [binary-path]
  (let [result (clojure.java.shell/sh "ldd" binary-path)
        lines (str/split-lines (:out result))
        libs (for [line lines
                   :when (re-find #"=>" line)]
               (let [parts (str/split (str/trim line) #"\s*=>\s*")
                     name (first parts)
                     path (second parts)
                     path-only (first (str/split path #"\s"))]
                 {:name name :path path-only}))
        remote-lib-dir "/tmp/lib"]
    (jepsen.control/exec :mkdir :-p remote-lib-dir)
    (doseq [lib libs]
      (when (and (:path lib) (not= (:path lib) "not found"))
        (jepsen.control/upload [(:path lib)] (str remote-lib-dir "/" (:name lib)))
        (info "Copied" (:name lib))))
    (info "Copied" (count libs) "libraries to" remote-lib-dir)))

(defn small-db
  "Small DB"
  []
  (reify jepsen.db/DB
    (setup! [_ test node]
      (info node "installing small db")
      (let [host-binary "../build/debug/src/server/server"
            remote-dir "/tmp/small-db"]
        ;; ;; Copy server binary to VM
        ;; (jepsen.control/exec :mkdir :-p remote-dir)
        ;; (jepsen.control/upload [host-binary] (str remote-dir "/server"))
        ;; (jepsen.control/exec :chmod :+x (str remote-dir "/server"))
        ;; ;; Copy dynamic libraries to VM
        ;; (copy-dynamic-libs host-binary)

        ;; Start the server
        (let [logfile (str remote-dir "/server.log")
              pidfile (str remote-dir "/server.pid")
              data-dir (str remote-dir "/data")
              sql-port 5001
              grpc-port 50001]
          (jepsen.control/exec :mkdir :-p data-dir)
          (jepsen.control.util/start-daemon!
           {:logfile logfile
            :pidfile pidfile
            :chdir remote-dir
            :env {:LD_LIBRARY_PATH "/tmp/lib"}}
           (str remote-dir "/server")
           :--sql-port sql-port
           :--grpc-port grpc-port
           :--data-dir data-dir
           :--region "test")
          ;;  "/usr/bin/env")
          (info "Started small-db server on" node "with SQL port" sql-port "and gRPC port" grpc-port))))

    (teardown! [_ test node]
      (info node "tearing down small db"))))

(defn small-db-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge jepsen.tests/noop-test
         opts
         {:name "small-db"
          :os jepsen.os.debian/os
          :db (small-db)
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (jepsen.cli/run! (jepsen.cli/single-test-cmd {:test-fn small-db-test})
                   args))
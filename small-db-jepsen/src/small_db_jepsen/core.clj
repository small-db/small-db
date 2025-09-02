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

;; disk location config
(def dir     "/tmp/small-db")
(def binary  (str dir "/server"))
(def data-dir (str dir "/data"))
(def logfile (str dir "/server.log"))
(def pidfile (str dir "/server.pid"))

;; runtime config
(def sql-port 5001)
(def grpc-port 50001)

;; host binary location
(def host-binary "../build/debug/src/server/server")
(def tools-binary ["../build/debug/src/rocks/rocks_scan"])

(defn small-db
  "Small DB"
  []
  (reify jepsen.db/DB
    (setup! [_ test node]
      (info node "installing small db")
      ;; Copy server binary to VM
      (jepsen.control/exec :mkdir :-p dir)
      (jepsen.control/upload [host-binary] binary)
      (jepsen.control/exec :chmod :+x binary)

      ;; Copy tools binary to VM
      (doseq [tool tools-binary]
        (let [remote-tool (str dir "/" (last (str/split tool #"/")))]
          (jepsen.control/upload [tool] remote-tool)
          (jepsen.control/exec :chmod :+x remote-tool)))

      ;; ;; Copy dynamic libraries to VM
      ;; (copy-dynamic-libs host-binary)

      ;; Start the server with configuration based on node
      (let [[region join-server]
            (cond
              (= node "asia") ["asia" "america:50001"]
              (= node "europe") ["eu" "america:50001"]
              (= node "america") ["us" ""]
              :else ["us" ""])]
        (jepsen.control/exec :mkdir :-p data-dir)
        (jepsen.control.util/start-daemon!
         {:logfile logfile
          :pidfile pidfile
          :chdir dir
          :env {:LD_LIBRARY_PATH "/tmp/lib"}}
         binary
         :--sql-port sql-port
         :--grpc-port grpc-port
         :--data-dir data-dir
         :--region region
         :--join join-server)
        (info "Started small-db server on" node "with SQL port" sql-port "gRPC port" grpc-port "region" region "join" join-server)
        (Thread/sleep 1000000)))

    (teardown! [_ test node]
      (info node "tearing down small db")
      (jepsen.control.util/stop-daemon! pidfile)
      (jepsen.control/exec :rm :-rf dir))))

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
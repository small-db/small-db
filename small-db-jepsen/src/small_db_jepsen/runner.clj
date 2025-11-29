(ns small-db-jepsen.runner
  (:require
   [clojure.java.shell :refer [sh]]
   [clojure.string :as str]
   [clojure.tools.logging :refer [info]]
   jepsen.cli
   jepsen.client
   jepsen.control
   jepsen.control.util
   jepsen.db
   jepsen.os.debian
   jepsen.tests
   [pg.core]))

(defrecord Client [conn]
  jepsen.client/Client
  (open! [this test node]
    (let [config
          {:host node
           :port 5001
           :user "postgres"
           :password "postgres"
           :database "postgres"}]

      (assoc this
             :conn (pg.core/connect config)
             :node node)))

  (setup! [this test]
    (when (= (:node this) "america")
      (info "Creating table on america client")

      (pg.core/query (:conn this) "DROP TABLE IF EXISTS users;")

      (pg.core/query (:conn this) "
          CREATE TABLE users (
              id INT PRIMARY KEY,
              name STRING,
              balance INT,
              country STRING
          ) PARTITION BY LIST (country);")

      (pg.core/query (:conn this) "
          CREATE TABLE users_eu PARTITION OF users FOR
          VALUES IN ('Germany', 'France', 'Italy');")

      (pg.core/query (:conn this) "
          CREATE TABLE users_us PARTITION OF users FOR
          VALUES IN ('USA', 'Canada');")

      (pg.core/query (:conn this) "
          CREATE TABLE users_asia PARTITION OF users FOR
          VALUES IN ('China', 'Japan', 'Korea');")

      (pg.core/query (:conn this) "
          ALTER TABLE users_eu ADD CONSTRAINT check_region CHECK (region = 'eu');")

      (pg.core/query (:conn this) "
          ALTER TABLE users_us ADD CONSTRAINT check_region CHECK (region = 'us');")

      (pg.core/query (:conn this) "
          ALTER TABLE users_asia ADD CONSTRAINT check_region CHECK (region = 'asia');")

      (pg.core/query (:conn this) "
          INSERT INTO users (id, name, balance, country) VALUES
          (1, 'Alice', 1000, 'Germany'),
          (2, 'Bob', 2000, 'USA'),
          (3, 'Charlie', 1500, 'France'),
          (4, 'David', 3000, 'China'),
          (5, 'Eve', 2500, 'Japan');")

      (info "Querying system.tables:")
      (let [tables-result (pg.core/query (:conn this) "SELECT * FROM system.tables;")]
        (doseq [row tables-result]
          (info "Table:" row)))

      (info "Querying system.partitions:")
      (let [partitions-result (pg.core/query (:conn this) "SELECT * FROM system.partitions WHERE table_name = 'users';")]
        (doseq [row partitions-result]
          (info "Partition:" row)))

      (info "Completed table setup and queries on america client"))
    (Thread/sleep 20000))

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))

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

      ;; Copy dynamic libraries to VM
      (copy-dynamic-libs host-binary)

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
        (info "Started small-db server on" node "with SQL port" sql-port "gRPC port" grpc-port "region" region "join" join-server))

      ;; sleep for 10 seconds
      (Thread/sleep 10000))

    (teardown! [_ test node]
      (info node "tearing down small db")
      (jepsen.control.util/stop-daemon! pidfile)
      (jepsen.control/exec :rm :-rf dir))

    jepsen.db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn small-db-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge jepsen.tests/noop-test
         opts
         {:name "small-db"
          :os jepsen.os.debian/os
          :db (small-db)
          :pure-generators true
          :client (Client. nil)}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (jepsen.cli/run! (jepsen.cli/single-test-cmd {:test-fn small-db-test})
                   args))
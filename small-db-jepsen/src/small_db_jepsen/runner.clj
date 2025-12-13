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
   jepsen.generator
   jepsen.os.debian
   jepsen.tests
   [pg.core]))

;; file location inside VM
(def workDir     "/tmp/small-db")
(def binary  (str workDir "/server"))
(def data-dir (str workDir "/data"))
(def logfile (str workDir "/server.log"))
(def pidfile (str workDir "/server.pid"))
(def libDir  "/tmp/lib")

;; runtime ports
(def sql-port 5001)
(def grpc-port 50001)

;; host binary locations
(def host-binary "../build/debug/src/server/server")
(def tools-binary ["../build/debug/src/rocks/rocks_scan"])

(defrecord Client [conn]
  jepsen.client/Client
  (open! [this test node]
    (info "Opening client connection to" node)
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
    ;; TODO: remove hardcoded node name
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
          ALTER TABLE users_asia ADD CONSTRAINT check_region CHECK (region = 'asia');")

      (pg.core/query (:conn this) "
          INSERT INTO users (id, name, balance, country) VALUES
          (1, 'Alice', 1000, 'Germany'),
          (2, 'Bob', 2000, 'USA'),
          (3, 'Charlie', 1500, 'France'),
          (4, 'David', 3000, 'China'),
          (5, 'Eve', 2500, 'Japan');")

      (info "Completed table setup on america client"))

    ;; all clients wait for 20 seconds to ensure setup is complete
    (Thread/sleep 20000))

  (invoke! [this test op]
    (case (:f op)
      :query-system-tables
      (let [result (pg.core/query (:conn this) "SELECT * FROM system.tables;")]
        (info (:node this) "System tables:" result)
        (assoc op :type :ok, :value result))

      :query-system-partitions
      (let [result (pg.core/query (:conn this) "SELECT * FROM system.partitions WHERE table_name = 'users';")]
        (info (:node this) "System partitions:" result)
        (assoc op :type :ok, :value result))

      (assoc op :type :fail, :error :unknown-operation)))

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
        remote-lib-dir libDir]
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
      ;; Copy server binary to VM
      (jepsen.control/exec :mkdir :-p workDir)
      (jepsen.control/upload [host-binary] binary)
      (jepsen.control/exec :chmod :+x binary)

      ;; Copy tools binary to VM
      (doseq [tool tools-binary]
        (let [remote-tool (str workDir "/" (last (str/split tool #"/")))]
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
          :chdir workDir
          :env {:LD_LIBRARY_PATH libDir}}
         binary
         :--sql-port sql-port
         :--grpc-port grpc-port
         :--data-dir data-dir
         :--region region
         :--join join-server)
        (info "Started small-db server on" node "with SQL port" sql-port "gRPC port" grpc-port "region" region "join" join-server))

      ;; wait for server to start
      (Thread/sleep 20000))

    (teardown! [_ test node]
      (info node "tearing down small db")
      (jepsen.control.util/stop-daemon! pidfile)
      (jepsen.control/exec :rm :-rf workDir))

    jepsen.db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn query-test
  "Run simple queries on all nodes."
  [opts]
  (merge jepsen.tests/noop-test
         opts
         {:name "query-test"
          :os jepsen.os.debian/os
          :db (small-db)
          :client (Client. nil)
          :generator (jepsen.generator/phases
                      (jepsen.generator/log "Querying system.tables")
                      (jepsen.generator/once
                       {:type :invoke, :f :query-system-tables})
                      (jepsen.generator/log "Querying system.partitions")
                      (jepsen.generator/once
                       {:type :invoke, :f :query-system-partitions}))}))

(defn second-test
  "a placeholder for a second test."
  [opts]
  (merge jepsen.tests/noop-test
         opts
         {:name "second-test"
          :os jepsen.os.debian/os
          :db (small-db)
          :client (Client. nil)}))

(defn -main
  [& args]
  (jepsen.cli/run! (jepsen.cli/test-all-cmd {:tests-fn (fn [opts]
                                                         [(query-test opts)])})
                   args))
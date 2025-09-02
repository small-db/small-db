(ns jepsen.core
  "Entry point for all Jepsen tests. Coordinates the setup of servers, running
  tests, creating and resolving failures, and interpreting results.

  Jepsen tests a system by running a set of singlethreaded *processes*, each
  representing a single client in the system, and a special *nemesis* process,
  which induces failures across the cluster. Processes choose operations to
  perform based on a *generator*. Each process uses a *client* to apply the
  operation to the distributed system, and records the invocation and
  completion of that operation in the *history* for the test. When the test is
  complete, a *checker* analyzes the history to see if it made sense.

  Jepsen automates the setup and teardown of the environment and distributed
  system by using an *OS* and *client* respectively. See `run!` for details."
  (:refer-clojure :exclude [run!])
  (:require [clojure.java.shell :refer [sh]]
            [clojure.stacktrace :as trace]
            [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.datafy :refer [datafy]]
            [dom-top.core :as dt :refer [assert+]]
            [knossos.op :as op]
            [knossos.history :as history]
            [jepsen.util :as util :refer [with-thread-name
                                          fcatch
                                          real-pmap
                                          relative-time-nanos]]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.control :as control]
            [jepsen.generator :as generator]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.nemesis :as nemesis]
            [jepsen.store :as store]
            [jepsen.control.util :as cu]
            [jepsen.generator [interpreter :as gen.interpreter]]
            [tea-time.core :as tt]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent CyclicBarrier
                                 CountDownLatch
                                 TimeUnit)))

(defn synchronize
  "A synchronization primitive for tests. When invoked, blocks until all nodes
  have arrived at the same point.

  This is often used in IO-heavy DB setup code to ensure all nodes have
  completed some phase of execution before moving on to the next. However, if
  an exception is thrown by one of those threads, the call to `synchronize`
  will deadlock! To avoid this, we include a default timeout of 60 seconds,
  which can be overridden by passing an alternate timeout in seconds."
  ([test]
   (synchronize test 60))
  ([test timeout-s]
   (or (= ::no-barrier (:barrier test))
       (.await ^CyclicBarrier (:barrier test) timeout-s TimeUnit/SECONDS))))

(defn conj-op!
  "Add an operation to a tests's history, and returns the operation."
  [test op]
  (swap! (:history test) conj op)
  op)

(defn primary
  "Given a test, returns the primary node."
  [test]
  (first (:nodes test)))

(defmacro with-resources
  "Takes a four-part binding vector: a symbol to bind resources to, a function
  to start a resource, a function to stop a resource, and a sequence of
  resources. Then takes a body. Starts resources in parallel, evaluates body,
  and ensures all resources are correctly closed in the event of an error."
  [[sym start stop resources] & body]
  ; Start resources in parallel
  `(let [~sym (doall (real-pmap (fcatch ~start) ~resources))]
     (when-let [ex# (some #(when (instance? Exception %) %) ~sym)]
       ; One of the resources threw instead of succeeding; shut down all which
       ; started OK and throw.
       (->> ~sym
            (remove (partial instance? Exception))
            (real-pmap (fcatch ~stop))
            dorun)
       (throw ex#))

     ; Run body
     (try ~@body
       (finally
         ; Clean up resources
         (dorun (real-pmap (fcatch ~stop) ~sym))))))

(defmacro with-os
  "Wraps body in OS setup and teardown."
  [test & body]
  `(try
     (control/on-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (control/on-nodes ~test (partial os/teardown! (:os ~test))))))

(defn snarf-logs!
  "Downloads logs for a test. Updates symlinks."
  [test]
  ; Download logs
  (locking snarf-logs!
    (when (satisfies? db/LogFiles (:db test))
      (info "Snarfing log files")
      (control/on-nodes test
        (fn [test node]
          (let [full-paths (db/log-files (:db test) test node)
                ; A map of full paths to short paths
                paths      (->> full-paths
                                (map #(str/split % #"/"))
                                util/drop-common-proper-prefix
                                (map (partial str/join "/"))
                                (zipmap full-paths))]
            (doseq [[remote local] paths]
              (when (cu/exists? remote)
                (info "downloading" remote "to" local)
                (try
                  (control/download
                    remote
                    (.getCanonicalPath
                      (store/path! test (name node)
                                   ; strip leading /
                                   (str/replace local #"^/" ""))))
                  (catch java.io.IOException e
                    (if (= "Pipe closed" (.getMessage e))
                      (info remote "pipe closed")
                      (throw e)))
                  (catch java.lang.IllegalArgumentException e
                    ; This is a jsch bug where the file is just being
                    ; created
                    (info remote "doesn't exist")))))))))
    (store/update-symlinks! test)))

(defn maybe-snarf-logs!
  "Snarfs logs, swallows and logs all throwables. Why? Because we do this when
  we encounter an error and abort, and we don't want an error here to supercede
  the root cause that made us abort."
  [test]
  (try (snarf-logs! test)
       (catch clojure.lang.ExceptionInfo e
         (warn e (str "Error snarfing logs and updating symlinks\n")
               (with-out-str (pprint (ex-data e)))))
       (catch Throwable t
         (warn t "Error snarfing logs and updating symlinks"))))

(defmacro with-log-snarfing
  "Evaluates body and ensures logs are snarfed afterwards. Will also download
  logs in the event of JVM shutdown, so you can ctrl-c a test and get something
  useful."
  [test & body]
  `(let [^Thread hook# (Thread.
                         (bound-fn []
                           (with-thread-name "Jepsen shutdown hook"
                             (info "Downloading DB logs before JVM shutdown...")
                             (snarf-logs! ~test)
                             (store/update-symlinks! ~test))))]
     (.. (Runtime/getRuntime) (addShutdownHook hook#))
     (try
       (let [res# (do ~@body)]
         (snarf-logs! ~test)
         res#)
       (finally
         (maybe-snarf-logs! ~test)
         (.. (Runtime/getRuntime) (removeShutdownHook hook#))))))

(defmacro with-db
  "Wraps body in DB setup and teardown."
  [test & body]
  `(try
     (with-log-snarfing ~test
       (db/cycle! ~test)
       ~@body)
     (finally
       (when-not (:leave-db-running? ~test)
         (control/on-nodes ~test (partial db/teardown! (:db ~test)))))))

(defmacro with-client+nemesis-setup-teardown
  "Evaluates body, setting up clients and nemesis before, and tearing them down
  at the end of the test."
  [test & body]
  `(let [client#  (:client ~test)
         nemesis# (nemesis/validate (:nemesis ~test))]
    ; Setup
    (let [nf# (future (nemesis/setup! nemesis# ~test))
               clients# (real-pmap (fn [node#]
                                     (with-thread-name
                                       (str "jepsen node " node#)
                                       (let [c# (client/open! client# ~test node#)]
                                         (client/setup! c# ~test)
                                         c#)))
                                   (:nodes ~test))
               nf# @nf#]
      (try
        (dorun clients#)
        ~@body
        (finally
          ; Teardown (and close clients)
          (let [nf# (future (nemesis/teardown! nf# ~test))]
            (dorun (real-pmap (fn [[c# node#]]
                                (with-thread-name
                                  (str "jepsen node " node#))
                                (try (client/teardown! c# ~test)
                                     (finally
                                       (client/close! c# ~test))))
                              (map vector clients# (:nodes ~test))))
            @nf#))))))

(defn run-case!
  "Takes a test, spawns nemesis and clients, runs the generator, and returns
  the history."
  [test]
  (assert+ (:pure-generators test)
           IllegalStateException
           "Jepsen 0.2.0 introduced significant changes to the generator system, and the semantics of your test may have changed. See jepsen.generator's docs for an extensive migration guide, and when you're ready to proceed, set `:pure-generators true` on your test map.")
  (with-client+nemesis-setup-teardown test
    (gen.interpreter/run! test)))

(defn analyze!
  "After running the test and obtaining a history, we perform some
  post-processing on the history, run the checker, and write the test to disk
  again."
  [test]
  (info "Analyzing...")
  (let [; Give each op in the history a monotonically increasing index
        test (assoc test :history (history/index (:history test)))
        ; Run checkers
        test (assoc test :results (checker/check-safe
                                   (:checker test)
                                   test
                                   (:history test)))]
    (info "Analysis complete")
    (when (:name test) (store/save-2! test))
    test))

(defn log-results
  "Logs info about the results of a test to stdout, and returns test."
  [test]
  (info (str
          (with-out-str
            (pprint (:results test)))
          (when (:error (:results test))
            (str "\n\n" (:error (:results test))))
          "\n\n"
          (case (:valid? (:results test))
            false     "Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻"
            :unknown  "Errors occurred during analysis, but no anomalies found. ಠ~ಠ"
            true      "Everything looks good! ヽ(‘ー`)ノ")))
  test)

(defn log-test-start!
  "Logs some basic information at the start of a test: the Git version of the
  working directory, the lein arguments to re-run the test, etc."
  [test]
  (let [git-head (sh "git" "rev-parse" "HEAD")]
    (when (zero? (:exit git-head))
      (let [head      (str/trim-newline (:out git-head))
            clean? (-> (sh "git" "status" "--porcelain=v1")
                       :out
                       str/blank?)]
        (info (str "Test version " head
                   (when-not clean? " (plus uncommitted changes)"))))))
  (when-let [argv (:argv test)]
    (info (str "Command line:\n"
          (->> (:argv test)
               (map control/escape)
               (list* "lein" "run")
               (str/join " ")))))
  (info (str "Running test:\n"
             (util/test->str test))))

(defn run!
  "Runs a test. Tests are maps containing

  :nodes      A sequence of string node names involved in the test
  :concurrency  (optional) How many processes to run concurrently
  :ssh        SSH credential information: a map containing...
    :username           The username to connect with   (root)
    :password           The password to use
    :port               SSH listening port (22)
    :private-key-path   A path to an SSH identity file (~/.ssh/id_rsa)
    :strict-host-key-checking  Whether or not to verify host keys
  :logging    Logging options; see jepsen.store/start-logging!
  :os         The operating system; given by the OS protocol
  :db         The database to configure: given by the DB protocol
  :remote     The remote to use for control actions: given by the Remote protocol
  :client     A client for the database
  :nemesis    A client for failures
  :generator  A generator of operations to apply to the DB
  :checker    Verifies that the history is valid
  :log-files  A list of paths to logfiles/dirs which should be captured at
              the end of the test.
  :nonserializable-keys   A collection of top-level keys in the test which
                          shouldn't be serialized to disk.
  :leave-db-running? Whether to leave the DB running at the end of the test.

  Tests proceed like so:

  1. Setup the operating system

  2. Try to teardown, then setup the database
    - If the DB supports the Primary protocol, also perform the Primary setup
      on the first node.

  3. Create the nemesis

  4. Fork the client into one client for each node

  5. Fork a thread for each client, each of which requests operations from
     the generator until the generator returns nil
    - Each operation is appended to the operation history
    - The client executes the operation and returns a vector of history elements
      - which are appended to the operation history

  6. Capture log files

  7. Teardown the database

  8. Teardown the operating system

  9. When the generator is finished, invoke the checker with the history
    - This generates the final report"
  [test]
  (tt/with-threadpool
    (try
      (with-thread-name "jepsen test runner"
        (let [test (assoc test
                          ; Initialization time
                          :start-time (util/local-time)

                          ; Number of concurrent workers
                          :concurrency (or (:concurrency test)
                                           (count (:nodes test)))

                          ; Synchronization point for nodes
                          :barrier (let [c (count (:nodes test))]
                                     (if (pos? c)
                                       (CyclicBarrier. (count (:nodes test)))
                                       ::no-barrier))

                          ; Currently running histories
                          :active-histories (atom #{}))
              _    (store/start-logging! test)
              _    (log-test-start! test)
              test (control/with-remote (:remote test)
                     (control/with-ssh (:ssh test)
                       (with-resources [sessions
                                        (bound-fn* control/session)
                                        control/disconnect
                                        (:nodes test)]
                         ; Index sessions by node name and add to test
                         (let [test (->> sessions
                                         (map vector (:nodes test))
                                         (into {})
                                         (assoc test :sessions))]
                           ; Setup
                           (with-os test
                             (with-db test
                               (util/with-relative-time
                                 ; Run a single case
                                 (let [test (assoc test :history
                                                   (run-case! test))
                                       ; Remove state
                                       test (dissoc test
                                                    :barrier
                                                    :active-histories
                                                    :sessions)]
                                   ; TODO: move test analysis outside the
                                   ; DB/ssh block.
                                   (info "Run complete, writing")
                                   (when (:name test) (store/save-1! test))
                                   (analyze! test)))))))))]
          (log-results test)))
      (catch Throwable t
        (warn t "Test crashed!")
        (throw t))
    (finally
      (store/stop-logging!)))))

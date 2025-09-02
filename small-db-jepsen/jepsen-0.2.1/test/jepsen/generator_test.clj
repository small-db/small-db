(ns jepsen.generator-test
  (:require [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.independent :as independent]
            [jepsen [util :as util]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [knossos.op :as op]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (io.lacuna.bifurcan Set)))

(gen/init!)

(deftest nil-test
  (is (= [] (gen.test/perfect nil))))

(deftest map-test
  (testing "once"
    (is (= [{:time 0
             :process 0
             :type :invoke
             :f :write}]
           (gen.test/perfect {:f :write}))))

  (testing "concurrent"
    (is (= [{:type :invoke, :process 0, :f :write, :time 0}
            {:type :invoke, :process :nemesis, :f :write, :time 0}
            {:type :invoke, :process 1, :f :write, :time 0}
            {:type :invoke, :process 1, :f :write, :time 10}
            {:type :invoke, :process :nemesis, :f :write, :time 10}
            {:type :invoke, :process 0, :f :write, :time 10}]
           (gen.test/perfect (repeat 6 {:f :write})))))

  (testing "all threads busy"
    (is (= [:pending {:f :write}]
           (gen/op {:f :write} {} (assoc gen.test/default-context
                                         :free-threads (Set.)))))))

(deftest limit-test
  (is (= [{:type :invoke, :process 0,        :time 0, :f :write, :value 1}
          {:type :invoke, :process :nemesis, :time 0, :f :write, :value 1}]
         (->> (repeat {:f :write :value 1})
              (gen/limit 2)
              gen.test/quick))))

(deftest repeat-test
  (is (= [0 0 0]
         (->> (range)
              (map (partial hash-map :value))
              (gen/repeat 3)
              (gen.test/perfect)
              (map :value)))))

(deftest delay-test
  (is (= [{:type :invoke, :process 0, :time 0, :f :write}
          {:type :invoke, :process :nemesis, :time 3, :f :write}
          {:type :invoke, :process 1, :time 6, :f :write}
          ; This would normally execute at 9 and 12, but every thread was busy
          ; for 10 nanos: they start as soon as they can.
          {:type :invoke, :process 0, :time 10, :f :write}
          {:type :invoke, :process :nemesis, :time 13, :f :write}]
          (->> {:f :write}
               repeat
               (gen/delay 3e-9)
               (gen/limit 5)
               gen.test/perfect))))

(deftest seq-test
  (testing "vectors"
    (is (= [1 2 3]
           (->> [{:value 1}
                 {:value 2}
                 {:value 3}]
                gen.test/quick
                (map :value)))))

  (testing "seqs"
    (is (= [1 2 3]
           (->> [{:value 1}
                 {:value 2}
                 {:value 3}]
                gen.test/quick
                (map :value)))))

  (testing "nested"
    (is (= [1 2 3 4 5]
           (->> [[{:value 1}
                  {:value 2}]
                 [[{:value 3}]
                  {:value 4}]
                 {:value 5}]
                gen.test/quick
                (map :value)))))

  (testing "updates propagate to first generator"
    (let [gen (->> [(gen/until-ok (gen/repeat {:f :read}))
                    {:f :done}]
                   (gen/clients))
          types (atom (concat [nil :fail :fail :ok :ok] (repeat :info)))]
      (is (= [[0 :read :invoke]
              [0 :read :invoke]
              ; Everyone fails and retries
              [10 :read :fail]
              [10 :read :invoke]
              [10 :read :fail]
              [10 :read :invoke]
              ; One succeeds and goes on to execute :done
              [20 :read :ok]
              [20 :done :invoke]
              ; The other succeeds and is finished
              [20 :read :ok]
              [30 :done :info]]
             (->> (gen.test/simulate gen.test/default-context gen
                            (fn [ctx op]
                              (-> op (update :time + 10)
                                  (assoc :type (first (swap! types next))))))
                  (map (juxt :time :f :type))))))))

(deftest fn-test
  (testing "returning nil"
    (is (= [] (gen.test/quick (fn [])))))

  (testing "returning a literal map"
    (let [ops (->> (fn [] {:f :write, :value (rand-int 10)})
                   (gen/limit 5)
                   gen.test/perfect)]
      (is (= 5 (count ops)))                      ; limit
      (is (every? #(<= 0 % 10) (map :value ops))) ; legal vals
      (is (< 1 (count (set (map :value ops)))))   ; random vals
      (is (= #{0 1 :nemesis} (set (map :process ops)))))) ; processes assigned

  (testing "returning repeat maps"
    (let [ops (->> #(gen/repeat {:f :write, :value (rand-int 10)})
                   (gen/limit 5)
                   gen.test/perfect)]
      (is (= 5 (count ops)))                      ; limit
      (is (every? #(<= 0 % 10) (map :value ops))) ; legal vals
      (is (= 1 (count (set (map :value ops)))))   ; same vals
      (is (= #{0 1 :nemesis} (set (map :process ops))))))) ; processes assigned

(deftest on-update+promise-test
  ; We only fulfill p once the write has taken place.
  (let [p (promise)]
    (is (= [{:type :invoke, :time 0, :process 0, :f :read}
            {:type :invoke, :time 0, :process 1, :f :write,   :value :x}
            {:type :invoke, :time 0, :process 1, :f :confirm, :value :x}
            {:type :invoke, :time 0, :process 1, :f :hold}
            {:type :invoke, :time 0, :process 1, :f :hold}]
           (->> (gen/any p
                         [{:f :read}
                          {:f :write, :value :x}
                          ; We'll do p at this point, then return to hold.
                          (repeat {:f :hold})])
                ; We don't deliver p until after the write is complete.
                (gen/on-update (fn [this test ctx event]
                                 (when (and (op/ok? event)
                                            (= :write (:f event)))
                                   (deliver p {:f      :confirm
                                               :value  (:value event)}))
                                 this))
                (gen/limit 5)
                (gen.test/quick (assoc gen.test/default-context :free-threads
                              (Set/from [0 1]))))))))


(deftest clojure-delay-test
  (let [eval-ctx (promise)
        d (delay (gen/limit 3
                   (fn [test ctx]
                     ; This is a side effect so we can verify the context is
                     ; being passed in properly.
                     (deliver eval-ctx ctx)
                     {:f :delayed})))
        h (->> (gen/phases {:f :write}
                           {:f :read}
                           d)
               gen/clients
               gen.test/perfect)]
    (is (= [{:f :write, :time 0, :process 0, :type :invoke}
            {:f :read, :time 10, :process 1, :type :invoke}
            {:f :delayed, :time 20, :process 1, :type :invoke}
            {:f :delayed, :time 20, :process 0, :type :invoke}
            {:f :delayed, :time 30, :process 0, :type :invoke}]
           h))
    (is (realized? d))
    (is (= {:time 20
            :free-threads (Set/from [0 1])
            :workers {0 0, 1 1}}
           @eval-ctx))))

(deftest synchronize-test
  (is (= [{:f :a, :process 0, :time 2, :type :invoke}
          {:f :a, :process 1, :time 3, :type :invoke}
          {:f :a, :process :nemesis, :time 5, :type :invoke}
          {:f :b, :process 1, :time 15, :type :invoke}
          {:f :b, :process 0, :time 15, :type :invoke}]
         (->> [(->> (fn [test ctx]
                      (let [p     (first (gen/free-processes ctx))
                            ; This is technically illegal: we should return the
                            ; NEXT event by time. We're relying on the specific
                            ; order we get called here to do this. Fragile hack!
                            delay (case p
                                    0        2
                                    1        1
                                    :nemesis 2)]
                        {:f :a
                         :process p
                         :time (+ (:time ctx) delay)}))
                    (gen/limit 3))
               ; The latest process, the nemesis, should start at time 5 and
               ; finish at 15.
               (gen/synchronize (repeat 2 {:f :b}))]
              gen.test/perfect))))

(deftest clients-test
  (is (= #{0 1}
         (->> {}
              gen/repeat
              (gen/clients)
              (gen/limit 5)
              gen.test/perfect
              (map :process)
              set))))

(deftest phases-test
  (is (= [[:a 0 0]
          [:a 1 0]
          [:b 1 10]
          [:c 0 20]
          [:c 1 20]
          [:c 1 30]]
         (->> (gen/phases (repeat 2 {:f :a})
                          (repeat 1 {:f :b})
                          (repeat 3 {:f :c}))
              gen/clients
              gen.test/perfect
              (map (juxt :f :process :time))))))

(deftest any-test
  ; We take two generators, each of which is restricted to a single process,
  ; and each of which takes time to schedule. When we bind them together with
  ; Any, they can interleave.
  (is (= [[:b 1 0]
          [:a 0 0]
          [:a 0 20]
          [:b 1 20]]
         (->> (gen/any (gen/on #{0} (gen/delay 20e-9 (repeat {:f :a})))
                       (gen/on #{1} (gen/delay 20e-9 (repeat {:f :b}))))
              (gen/limit 4)
              gen.test/perfect
              (map (juxt :f :process :time))))))

(deftest each-thread-test
  (is (= [[0 0 :a]
          [0 1 :a]
          [0 :nemesis :a]
          [10 :nemesis :b]
          [10 1 :b]
          [10 0 :b]]
         ; Each thread now gets to evaluate [a b] independently.
         (->> (gen/each-thread [{:f :a} {:f :b}])
              gen.test/perfect
              (map (juxt :time :process :f)))))

  (testing "collapses when exhausted"
    (is (= nil
           (gen/op (gen/each-thread (gen/limit 0 {:f :read}))
               {}
               gen.test/default-context)))))

(deftest stagger-test
  (let [n           1000
        dt          20
        concurrency (count (:workers gen.test/default-context))
        ops         (->> (range n)
                         (map (fn [x] {:f :write, :value x}))
                         (gen/stagger (util/nanos->secs dt))
                         gen.test/perfect)
        times       (mapv :time ops)
        max-time    (peek times)
        rate        (float (/ n max-time))
        expected-rate (float (/ dt))]
    (is (<= 0.9 (/ rate expected-rate) 1.1))))

(deftest f-map-test
  (is (= [{:type :invoke, :process 0, :time 0, :f :b, :value 2}]
         (->> {:f :a, :value 2}
              (gen/f-map {:a :b})
              gen.test/perfect))))

(deftest filter-test
  (is (= [0 2 4 6 8]
         (->> (range)
              (map (fn [x] {:value x}))
              (gen/limit 10)
              (gen/filter (comp even? :value))
              gen.test/perfect
              (map :value)))))

(deftest ^:logging log-test
  (is (->> (gen/phases (gen/log :first)
                       {:f :a}
                       (gen/log :second)
                       {:f :b})
           gen.test/perfect
           (map :f)
           (= [:a :b]))))

(deftest mix-test
  (let [fs (->> (gen/mix [(repeat 5  {:f :a})
                          (repeat 10 {:f :b})])
                gen.test/perfect
                (map :f))]
    (is (= {:a 5
            :b 10}
           (frequencies fs)))
    (is (not= (concat (repeat 5 :a) (repeat 5 :b)) fs))))

(deftest process-limit-test
  (is (= [[0 0]
          [1 1]
          [3 2]
          [2 3]
          [4 4]]
         (->> (range)
              (map (fn [x] {:value x}))
              (gen/process-limit 5)
              gen/clients
              gen.test/perfect-info
              (map (juxt :process :value))))))

(deftest time-limit-test
  (is (= [[0  :a] [0  :a] [0 :a]
          [10 :a] [10 :a] [10 :a]
          [20 :b] [20 :b] [20 :b]]
         ; We use two time limits in succession to make sure they initialize
         ; their limits appropriately.
         (->> [(gen/time-limit (util/nanos->secs 20) (gen/repeat {:value :a}))
               (gen/time-limit (util/nanos->secs 10) (gen/repeat {:value :b}))]
              gen.test/perfect
              (map (juxt :time :value))))))

(defn integers
  "A sequence of maps with :value 0, 1, 2, ..., and any other kv pairs."
  [& kv-pairs]
  (->> (range)
       (map (fn [x] (apply hash-map :value x kv-pairs)))))

(deftest reserve-test
  ; TODO: can you nest reserves properly? I suspect no.

  (let [as (integers :f :a)
        bs (integers :f :b)
        cs (integers :f :c)]
    (testing "only a default"
      (is (= [{:f :a, :process 0,        :time 0, :type :invoke, :value 0}
              {:f :a, :process :nemesis, :time 0, :type :invoke, :value 1}
              {:f :a, :process 1,        :time 0, :type :invoke, :value 2}]
             (->> (gen/reserve as)
                  (gen/limit 3)
                  gen.test/perfect))))

    (testing "three ranges"
      (is (= [[0 1 :a 0]
              [0 0 :a 1]
              [0 3 :b 0]
              [0 :nemesis :c 0]
              [0 2 :b 1]
              [0 4 :b 2]
              [10 4 :b 3]
              [10 2 :b 4]
              [10 :nemesis :c 1]
              [10 3 :b 5]
              [10 0 :a 2]
              [10 1 :a 3]
              [20 1 :a 4]
              [20 0 :a 5]
              [20 3 :b 6]]
             (->> (gen/reserve 2 as
                               3 bs
                               cs)
                  (gen/limit 15)
                  (gen.test/perfect (gen.test/n+nemesis-context 5))
                  (map (juxt :time :process :f :value))))))))

(deftest independent-sequential-test
  (is (= [[0 0 [:x 0]]
          [0 1 [:x 1]]
          [10 1 [:x 2]]
          [10 0 [:y 0]]
          [20 0 [:y 1]]
          [20 1 [:y 2]]]
         (->> (independent/sequential-generator
                [:x :y]
                (fn [k]
                  (->> (range)
                       (map (partial hash-map :type :invoke, :value))
                       (gen/limit 3))))
              gen/clients
              gen.test/perfect
              (map (juxt :time :process :value))))))

(deftest independent-concurrent-test
  ; All 3 groups can concurrently execute the first 2 values from k0, k1, k2
  (is (= [[0 0 [:k0 :v0]]
          [0 1 [:k0 :v1]]
          [0 4 [:k2 :v0]]
          [0 2 [:k1 :v0]]
          [0 5 [:k2 :v1]]
          [0 3 [:k1 :v1]]

          ; Worker 3 in group 1 finishes k1
          [10 3 [:k1 :v2]]
          ; And worker 5 finishes k2
          [10 5 [:k2 :v2]]
          ; Worker 2 in group 1 starts k3
          [10 2 [:k3 :v0]]
          ; And worker 4 in group 2 starts k4
          [10 4 [:k4 :v0]]
          ; Worker 1 in group 0 finishes k0
          [10 1 [:k0 :v2]]

          ; Worker 0 has no options left; there are no keys remaining for it to
          ; start afresh, and other groups still has generators, so it holds
          ; at :pending. Workers 2 & 3 finish k3, and workers 4 and 5 finish k4.
          ; At the next timeslice, worker 4 in group 2 continues k4.
          [20 4 [:k4 :v1]]
          [20 2 [:k3 :v1]]
          [20 5 [:k4 :v2]]
          [20 3 [:k3 :v2]]]
         (->> (independent/concurrent-generator
                2                     ; 2 threads per group
                [:k0 :k1 :k2 :k3 :k4] ; 5 keys
                (fn [k]
                  (->> [:v0 :v1 :v2] ; Three values per key
                       (map (partial hash-map :type :invoke, :value)))))
              (gen.test/perfect (gen.test/n+nemesis-context 6)) ; 3 groups of 2 threads each
              (map (juxt :time :process :value))))))

(deftest independent-deadlock-case
  (is (= [[0 1 :meow [0 nil]]
          [0 0 :meow [0 nil]]
          [10 1 :meow [1 nil]]
          [10 0 :meow [1 nil]]
          [20 0 :meow [2 nil]]]
          (->> (independent/concurrent-generator
                2
                (range)
                (fn [k] (gen/each-thread {:f :meow})))
              (gen/limit 5)
              gen/clients
              gen.test/perfect
              (map (juxt :time :process :f :value))))))

(deftest at-least-one-ok-test
  ; Our goal here is to ensure that at least one OK operation happens.
  (is (= [[0   0 :invoke]
          [0   1 :invoke]
          [10  1 :fail]
          [10  1 :invoke]
          [10  0 :fail]
          [10  0 :invoke]
          [20  0 :info]
          [20  2 :invoke]
          [20  1 :info]
          [20  3 :invoke]
          [30  3 :ok]
          [30  2 :ok]] ; They complete concurrently, so we get two oks
         (->> {:f :read}
              repeat
              gen/until-ok
              (gen/limit 10)
              gen/clients
              gen.test/imperfect
              (map (juxt :time :process :type))))))

(deftest flip-flop-test
  (is (= [[0 :write 0]
          [1 :read nil]
          [1 :write 1]
          [0 :finalize nil]
          [0 :write 2]]
         (->> (gen/flip-flop (map (fn [x] {:f :write, :value x}) (range))
                             [{:f :read}
                              {:f :finalize}])
              (gen/limit 10)
              gen/clients
              gen.test/perfect
              (map (juxt :process :f :value))))))

(deftest pretty-print-test
  (is (= "(jepsen.generator.Synchronize{\n   :gen {:f :start}}\n jepsen.generator.Synchronize{\n   :gen [1 2 3]}\n jepsen.generator.Synchronize{\n   :gen jepsen.generator.Limit{\n     :remaining 3, :gen {:f :read}}})\n"
         (with-out-str
           (pprint (gen/phases
                     {:f :start}
                     [1 2 3]
                     (->> {:f :read}
                          (gen/limit 3))))))))

(deftest concat-test
  (is (= [:a :b :c :d]
         (->> (gen/concat [{:value :a}
                           {:value :b}]
                          (gen/limit 1 {:value :c})
                          {:value :d})
              gen.test/perfect
              (map :value)))))

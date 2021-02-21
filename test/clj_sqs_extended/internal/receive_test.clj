(ns clj-sqs-extended.internal.receive-test
  (:require [clojure.test :refer [deftest is]]
            [bond.james :as bond]
            [clojure.core.async :as async :refer [chan close! timeout alts!! <!!]]
            [clojure.core.async.impl.protocols :as async-protocols]

            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.internal.receive :as receive]))

(def mock-received-messages [{:receiptHandle "receipt"
                              :body          "foo"}])

(deftest nil-returned-after-loop-was-terminated
  (let [message  mock-received-messages
        out-chan (chan)

        stop-fn (receive/receive-loop
                  @fixtures/test-queue-url
                  out-chan
                  (constantly message)
                  identity
                  {})]
    (is (fn? stop-fn))
    (is (= (:body (first message))
           (:body (<!! out-chan))))

    ;; terminate receive loop and thereby close the out-channel
    (stop-fn)
    (<!! out-chan) ;; take out the last buffered item

    (is (clojure.core.async.impl.protocols/closed? out-chan))
    (is (nil? (<!! out-chan)))))

(defn- mock-receiver-fn [] [:foo])

(deftest receive-to-channel-test
  (bond/with-spy [mock-receiver-fn]
    (let [receiver-chan (receive/receive-to-channel mock-receiver-fn)]
      (is (instance? clojure.core.async.impl.channels.ManyToManyChannel receiver-chan))

      (is (= 1 (-> mock-receiver-fn  bond/calls count)))

      (is (= :foo (<!! receiver-chan)))
      (is (not (async-protocols/closed? receiver-chan)))

      (is (= 2 (-> mock-receiver-fn  bond/calls count))))))

(deftest ^:flaky numerous-simultaneous-receive-loops
  (let [message mock-received-messages
        n       10
        c       (chan)

        ;; setup
        stop-receive-loops
        (doall (for [_ (range n)]
                 (receive/receive-loop
                   @fixtures/test-queue-url
                   c
                   (constantly message)
                   identity
                   {})))]

    (is (= n (count stop-receive-loops)))
    (is (every? fn? stop-receive-loops))

    (is (= (:body (first message))
           (:body (<!! c))))

    ;; teardown
    (doseq [stop-fn stop-receive-loops]
      (stop-fn))
    (close! c))

  ;; gives time for the receive-loop to stop. otherwise, subsequent tests might fail
  (Thread/sleep 500))


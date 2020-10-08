(ns clj-sqs-extended.internal.receive-test
  (:require [clojure.test :refer [deftest is are use-fixtures]]
            [clojure.core.async :as async :refer [chan close! timeout alts!! <!!]]

            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.internal.receive :as receive])
  (:import [java.net.http HttpTimeoutException]
           [java.net
            SocketException
            UnknownHostException]
           [java.lang ReflectiveOperationException]))

(deftest error-is-safe-to-continue?-test
  (are [severity error]
       (= severity (receive/error-is-safe-to-continue? error))
       true (UnknownHostException.)
       true (SocketException.)
       true (HttpTimeoutException. "test")
       false (RuntimeException.)
       false (ReflectiveOperationException.)))

(use-fixtures :once fixtures/with-test-sqs-ext-client)

(deftest nil-returned-after-loop-was-terminated
  (fixtures/with-test-standard-queue
    (let [message  (helpers/random-message-basic)
          out-chan (chan)

          initial-receiving-chan (sqs/receive-to-channel
                                 @fixtures/test-sqs-ext-client
                                 @fixtures/test-queue-url
                                 {:auto-delete true})

        stop-fn (receive/receive-loop
                  @fixtures/test-sqs-ext-client
                  @fixtures/test-queue-url
                  initial-receiving-chan
                  #(sqs/receive-to-channel
                     @fixtures/test-sqs-ext-client
                     @fixtures/test-queue-url
                     {:auto-delete true})
                  out-chan)]
      (is (fn? stop-fn))

      (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                     @fixtures/test-queue-url
                                     message)))
      (is (= message (:body (<!! out-chan))))

      ;; terminate receive loop and thereby close the out-channel
      (stop-fn)

      (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                     @fixtures/test-queue-url
                                     message)))
      (is (clojure.core.async.impl.protocols/closed? out-chan))
      (is (nil? (<!! out-chan))))))

(deftest numerous-simultaneous-receive-loops
  (fixtures/with-test-standard-queue
    (let [message (helpers/random-message-basic)
          n       10
          c       (chan)

          sqs-ext-client
          (sqs/sqs-ext-client fixtures/sqs-ext-config)

          ;; setup
          initial-receiving-chan
          (sqs/receive-to-channel
            sqs-ext-client
            @fixtures/test-queue-url
            {:auto-delete true})

          stop-receive-loops
          (doall (for [_ (range n)]
                   (receive/receive-loop
                     sqs-ext-client
                     @fixtures/test-queue-url
                     initial-receiving-chan
                     #(sqs/receive-to-channel
                        sqs-ext-client
                        @fixtures/test-queue-url
                        {:auto-delete true})
                     c
                     {:auto-delete true})))]

      (is (= n (count stop-receive-loops)))

      (is (sqs/send-message sqs-ext-client
                            @fixtures/test-queue-url
                            message))

      (let [[out _] (alts!! [c (timeout 1000)])]
        (is (= message (:body out))))

      ;; teardown
      (doseq [stop-fn stop-receive-loops]
        (stop-fn))
      (close! c)

      ;; gives time for the receive-loop to stop
      (Thread/sleep 500))))


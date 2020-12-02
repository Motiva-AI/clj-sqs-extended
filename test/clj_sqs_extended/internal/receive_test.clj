(ns clj-sqs-extended.internal.receive-test
  (:require [clojure.test :refer [deftest is are use-fixtures]]
            [clojure.core.async :as async :refer [chan close! timeout alts!! <!!]]

            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.internal.receive :as receive]))

(use-fixtures :once fixtures/with-test-sqs-ext-client)

(deftest nil-returned-after-loop-was-terminated
  (fixtures/with-test-standard-queue
    (let [message  (helpers/random-message-basic)
          out-chan (chan)

          stop-fn (receive/receive-loop
                    @fixtures/test-sqs-ext-client
                    @fixtures/test-queue-url
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
          stop-receive-loops
          (doall (for [_ (range n)]
                   (receive/receive-loop
                     sqs-ext-client
                     @fixtures/test-queue-url
                     c
                     {:auto-delete true}
                     {})))]

      (is (= n (count stop-receive-loops)))
      (is (every? fn? stop-receive-loops))

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

(deftest only-acknowledge-messages-that-handler-is-available-to-process
  (fixtures/with-test-standard-queue
    (let [n        5
          messages (into [] (take n (repeatedly helpers/random-message-basic)))
          c        (chan)

          sqs-ext-client
          (sqs/sqs-ext-client fixtures/sqs-ext-config)

          ;; setup
          stop-receive-loop
          (receive/receive-loop
            sqs-ext-client
            @fixtures/test-queue-url
            c
            {:auto-delete false}
            {:max-number-of-receiving-messages 1
             :wait-time-in-seconds             1})]

      (is (fn? stop-receive-loop))
      (is (= {"ApproximateNumberOfMessages" 0, "ApproximateNumberOfMessagesNotVisible" 0}
             (sqs/queue-attributes sqs-ext-client @fixtures/test-queue-url)))

      (doseq [msg messages]
        (sqs/send-message sqs-ext-client @fixtures/test-queue-url msg))

      (is (= {"ApproximateNumberOfMessages" (- n 2), "ApproximateNumberOfMessagesNotVisible" 2}
             (sqs/queue-attributes sqs-ext-client @fixtures/test-queue-url)))

      (let [[out _] (alts!! [c (timeout 1000)])]
        (is (:body out))
        (is ((:done-fn out))))

      (is (= {"ApproximateNumberOfMessages" (- n 2) , "ApproximateNumberOfMessagesNotVisible" 1}
             (sqs/queue-attributes sqs-ext-client @fixtures/test-queue-url)))

      ;; teardown
      (stop-receive-loop)
      (close! c)

      ;; gives time for the receive-loop to stop
      (Thread/sleep 500))))


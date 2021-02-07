(ns clj-sqs-extended.internal.receive-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [bond.james :as bond]
            [clojure.core.async :as async :refer [chan close! timeout alts!! <!!]]
            [clojure.core.async.impl.protocols :as async-protocols]

            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.internal.receive :as receive]))

(use-fixtures :once fixtures/with-test-sqs-ext-client fixtures/with-test-s3-bucket)
(use-fixtures :each fixtures/with-transient-queue)

(deftest nil-returned-after-loop-was-terminated
  (let [message  (helpers/random-message-basic)
        out-chan (chan)

        stop-fn (receive/receive-loop
                  @fixtures/test-queue-url
                  out-chan
                  (partial sqs/receive-messages
                           @fixtures/test-sqs-ext-client
                           @fixtures/test-queue-url)
                  (partial sqs/delete-message!
                           @fixtures/test-sqs-ext-client
                           @fixtures/test-queue-url)
                  {:auto-delete? true})]
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
    (is (nil? (<!! out-chan)))))

(deftest numerous-simultaneous-receive-loops
  (let [message (helpers/random-message-basic)
        n       10
        c       (chan)

        ;; setup
        stop-receive-loops
        (doall (for [_ (range n)]
                 (receive/receive-loop
                   @fixtures/test-queue-url
                   c
                   (partial sqs/receive-messages
                            @fixtures/test-sqs-ext-client
                            @fixtures/test-queue-url)
                   (partial sqs/delete-message!
                            @fixtures/test-sqs-ext-client
                            @fixtures/test-queue-url)
                   {:auto-delete? true})))]

    (is (= n (count stop-receive-loops)))
    (is (every? fn? stop-receive-loops))

    (is (sqs/send-message @fixtures/test-sqs-ext-client
                          @fixtures/test-queue-url
                          message))

    (let [[out _] (alts!! [c (timeout 1000)])]
      (is (= message (:body out))))

    ;; teardown
    (doseq [stop-fn stop-receive-loops]
      (stop-fn))
    (close! c)

    ;; gives time for the receive-loop to stop
    (Thread/sleep 500)))

(deftest only-acknowledge-messages-that-handler-is-available-to-process
  (let [n        5
        messages (into [] (take n (repeatedly helpers/random-message-basic)))
        c        (chan)

        ;; setup
        stop-receive-loop
        (receive/receive-loop
          @fixtures/test-queue-url
          c
          (partial sqs/receive-messages
                   @fixtures/test-sqs-ext-client
                   @fixtures/test-queue-url
                   {:max-number-of-receiving-messages 1
                    :wait-time-in-seconds             1})
          (partial sqs/delete-message!
                   @fixtures/test-sqs-ext-client
                   @fixtures/test-queue-url)
          {:auto-delete? false})]

    (is (fn? stop-receive-loop))
    (is (= {"ApproximateNumberOfMessages" 0, "ApproximateNumberOfMessagesNotVisible" 0}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))


    (doseq [msg messages]
      (sqs/send-message @fixtures/test-sqs-ext-client @fixtures/test-queue-url msg))

    ;; these expected values require some explanation:
    ;; 1. one message is read off the queue and become NotVisible
    ;; 2. this first message is pushed to the output channel. Now the output
    ;;    channel (default size is 1) is full.
    ;; 3. on the next receive-loop iteration, a second message is read off the queue
    ;; 4. but when receive-to-channel tries to put this second message to the
    ;;    output channel, it is blocked because the output channel is full.
    ;; 5. Thus we expect two messages to be read from the queue with
    ;;    NotVisible values = 2
    (is (= {"ApproximateNumberOfMessages" (- n 2), "ApproximateNumberOfMessagesNotVisible" 2}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))

    (let [[out _] (alts!! [c (timeout 1000)])]
      (is (:body out))
      (is ((:done-fn out))))
    (Thread/sleep 100) ;; wait for message to be deleted

    (is (= {"ApproximateNumberOfMessages" (- n 3) , "ApproximateNumberOfMessagesNotVisible" 2}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))

    ;; teardown
    (stop-receive-loop)
    (close! c)

    ;; gives time for the receive-loop to stop
    (Thread/sleep 500)))

(defn- mock-receiver-fn [] [:foo])

(deftest receive-to-channel-test
  (bond/with-spy [mock-receiver-fn]
    (let [receiver-chan (receive/receive-to-channel mock-receiver-fn)]
      (is (instance? clojure.core.async.impl.channels.ManyToManyChannel receiver-chan))

      (is (= 1 (-> mock-receiver-fn  bond/calls count)))

      (is (= :foo (<!! receiver-chan)))
      (is (not (async-protocols/closed? receiver-chan)))

      (is (= 2 (-> mock-receiver-fn  bond/calls count))))))


(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan close! <!!]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import (com.amazonaws.services.sqs.model QueueDoesNotExistException)))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-messages-basic
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-with-time (helpers/random-message-with-time))
(defonce test-message-large (helpers/random-message-larger-than-256kb))

(deftest send-and-receive-basic-messages-test
  (testing "Basic sending and receiving works"
    (doseq [format [:transit :json]]
      (fixtures/with-test-standard-queue
        (let [out-chan (chan)
              stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (first test-messages-basic)
                                             {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (last test-messages-basic)
                                             {:format format})))
          (is (= (last test-messages-basic) (:body (<!! out-chan))))
          (stop-fn))))))

(deftest send-and-receive-timestamp-message
  (testing "Sending a message including a timestamp works"
    (fixtures/with-test-standard-queue
      (let [out-chan (chan)
            stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                          fixtures/test-standard-queue-name
                                          out-chan)]
        (is (fn? stop-fn))
        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           fixtures/test-standard-queue-name
                                           test-message-with-time)))
        (is (= test-message-with-time (:body (<!! out-chan))))
        (stop-fn)))))

(deftest send-and-receive-large-message
  (testing "Sending and receiving of a 256k+ message works"
    (fixtures/with-test-standard-queue
      (let [out-chan (chan)
            stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                          fixtures/test-standard-queue-name
                                          out-chan)]
        (is (fn? stop-fn))
        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           fixtures/test-standard-queue-name
                                           test-message-large)))
        (is (= test-message-large (:body (<!! out-chan))))
        (stop-fn)))))

(deftest send-nil-body-message
  (testing "Sending a standard message with a nil body works (read: is ignored)"
    (fixtures/with-test-standard-queue
      (is (nil? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                      fixtures/test-standard-queue-name
                                      nil))))))

(deftest send-nil-body-fifo-message
  (testing "Sending a FIFO message with a nil body works (read: is ignored)"
    (fixtures/with-test-fifo-queue
      (is (nil? (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                           fixtures/test-fifo-queue-name
                                           nil
                                           (helpers/random-group-id)))))))

(deftest send-message-to-non-existing-queue-fails
  (testing "Sending a message to a non-existing queue yields proper exception"
    (fixtures/with-test-standard-queue
      (is (thrown? QueueDoesNotExistException
                   (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                         "non-existing-queue"
                                         (first test-messages-basic)))))))

(deftest send-fifo-messages
  (testing "Sending and receiving of FIFO messages works"
    (doseq [format [:transit :json]]
      (fixtures/with-test-fifo-queue
        (helpers/purge-queue @fixtures/test-sqs-ext-client
                             fixtures/test-fifo-queue-name)
        (let [out-chan (chan)
              stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-fifo-queue-name
                                            out-chan
                                            {:format format})]
          (doseq [message test-messages-basic]
            (is (string? (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                                    fixtures/test-fifo-queue-name
                                                    message
                                                    (helpers/random-group-id)
                                                    {:format format}))))
          (doseq [message test-messages-basic]
            (let [received-message (<!! out-chan)]
              (is (= message (:body received-message)))))
          (stop-fn))))))

(deftest done-fn-handle-present-when-auto-delete-false
  (testing "done-fn handle is present in response when auto-delete is false"
    (doseq [format [:transit :json]]
      (fixtures/with-test-standard-queue
        (let [out-chan (chan)
              stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format      format
                                             :auto-delete false})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (first test-messages-basic)
                                             {:format format})))
          (let [received-message (<!! out-chan)]
            (is (= (first test-messages-basic) (:body received-message)))
            (is (contains? received-message :done-fn))
            (is (fn? (:done-fn received-message)))
            ((:done-fn received-message)))
          (stop-fn))))))

(deftest done-fn-handle-present-when-auto-delete-true
  (testing "done-fn handle is NOT present in response when auto-delete is true"
    (doseq [format [:transit :json]]
      (fixtures/with-test-standard-queue
        (let [out-chan (chan)
              stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format      format
                                             :auto-delete true})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (first test-messages-basic)
                                             {:format format})))
          (let [received-message (<!! out-chan)]
            (is (= (first test-messages-basic) (:body received-message))))
          (stop-fn))))))

(deftest nil-returned-after-loop-was-terminated
  (testing "Stopping the listener yields nil response when receiving from the channel again"
    (doseq [format [:transit :json]]
      (fixtures/with-test-standard-queue
        (let [out-chan (chan)
              stop-fn (receive/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (first test-messages-basic)
                                             {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))
          ;; Terminate receive loop and thereby close the out-channel
          (stop-fn)
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (last test-messages-basic)
                                             {:format format})))
          (is (clojure.core.async.impl.protocols/closed? out-chan))
          (is (nil? (<!! out-chan))))))))

(deftest handle-queue-works
  (let [handler-chan (chan)]
    (fixtures/with-test-standard-queue
      (fixtures/with-handle-queue-standard
        handler-chan
        {} ;; default aws options
        {} ;; default queue options

        (testing "handle-queue can send/receive basic message"
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             (first test-messages-basic))))
          (is (= (first test-messages-basic) (:body (<!! handler-chan)))))

        (testing "handle-queue can send/receive message including timestamp"
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             test-message-with-time)))
          (is (= test-message-with-time (:body (<!! handler-chan)))))

        (testing "handle-queue can send/receive large message"
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             test-message-large)))
          (is (= test-message-large (:body (<!! handler-chan)))))))))

(deftest handle-queue-terminates-with-non-existing-queue
  (testing "handle-queue terminates when non-existing queue is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-standard
                handler-chan
                {} ;; default aws options
                {:queue-name "non-existing-queue"})]
          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

(deftest handle-queue-terminates-with-non-existing-bucket
  (testing "handle-queue terminates when non-existing bucket is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-standard
                handler-chan
                {:s3-bucket-name "non-existing-bucket"}
                {} ;; default queue options
                )]
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             test-message-large)))
          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

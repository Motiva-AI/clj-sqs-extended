(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!!]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures])
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
              stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                         fixtures/test-standard-queue-name
                                         (first test-messages-basic)
                                         {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                         fixtures/test-standard-queue-name
                                         (last test-messages-basic)
                                         {:format format})))
          (is (= (last test-messages-basic) (:body (<!! out-chan))))
          (stop-fn))))))

(deftest send-and-receive-timestamp-message
  (testing "Sending a message including a timestamp works"
    (fixtures/with-test-standard-queue
      (let [out-chan (chan)
            stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                          fixtures/test-standard-queue-name
                                          out-chan)]
        (is (fn? stop-fn))
        (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                       fixtures/test-standard-queue-name
                                       test-message-with-time)))
        (is (= test-message-with-time (:body (<!! out-chan))))
        (stop-fn)))))

(deftest send-and-receive-large-message
  (testing "Sending and receiving of a 256k+ message works"
    (fixtures/with-test-standard-queue
      (let [out-chan (chan)
            stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                          fixtures/test-standard-queue-name
                                          out-chan)]
        (is (fn? stop-fn))
        (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                       fixtures/test-standard-queue-name
                                       test-message-large)))
        (is (= test-message-large (:body (<!! out-chan))))
        (stop-fn)))))

(deftest send-message-to-non-existing-queue-fails
  (testing "Sending a message to a non-existing queue yields proper exception"
    (fixtures/with-test-standard-queue
      (is (thrown? QueueDoesNotExistException
                   (sqs/send-message @fixtures/test-sqs-ext-client
                                     "non-existing-queue"
                                     (first test-messages-basic)))))))

(deftest send-fifo-messages
  (testing "Sending and receiving of FIFO messages works"
    (doseq [format [:transit :json]]
      (fixtures/with-test-fifo-queue
        (sqs/purge-queue @fixtures/test-sqs-ext-client
                         fixtures/test-fifo-queue-name)
        (let [out-chan (chan)
              stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-fifo-queue-name
                                            out-chan
                                            {:format format})]
          (doseq [message test-messages-basic]
            (is (string? (sqs/send-fifo-message @fixtures/test-sqs-ext-client
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
              stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format      format
                                             :auto-delete false})]
          (is (fn? stop-fn))
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
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
              stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format      format
                                             :auto-delete true})]
          (is (fn? stop-fn))
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
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
              stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                            fixtures/test-standard-queue-name
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                         fixtures/test-standard-queue-name
                                         (first test-messages-basic)
                                         {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))
          ;; Terminate receive loop and thereby close the out-channel
          (stop-fn)
          (is (string? (sqs/send-message @fixtures/test-sqs-ext-client
                                         fixtures/test-standard-queue-name
                                         (last test-messages-basic)
                                         {:format format})))
          (is (nil? (<!! out-chan))))))))

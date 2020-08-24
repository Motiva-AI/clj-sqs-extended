(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan close! <!! >!!]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs])
  (:import (com.amazonaws.services.sqs.model QueueDoesNotExistException)))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-messages-basic
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-with-time (helpers/random-message-with-time))
(defonce test-message-large (helpers/random-message-larger-than-256kb))

(deftest send-nil-body-message-yields-exception
  (testing "Sending a standard message with a nil body yields exception"
    (fixtures/with-test-standard-queue
      (is (thrown? Exception
                   (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                         fixtures/test-standard-queue-name
                                         nil)))))

  (testing "Sending a FIFO message with a nil body yields exception"
    (fixtures/with-test-fifo-queue
      (is (thrown? Exception
                   (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                              fixtures/test-fifo-queue-name
                                              nil
                                              (helpers/random-group-id)))))))

(deftest send-message-to-non-existing-queue-fails
  (testing "Sending a standard message to a non-existing queue yields proper exception"
    (fixtures/with-test-standard-queue
      (is (thrown? QueueDoesNotExistException
                   (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                         "non-existing-queue"
                                         (first test-messages-basic))))))
  (testing "Sending a FIFO message to a non-existing queue yields proper exception"
    (fixtures/with-test-fifo-queue
      (is (thrown? QueueDoesNotExistException
                   (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                              "non-existing-queue"
                                              (first test-messages-basic)
                                              (helpers/random-group-id)))))))

(deftest handle-queue-sends-and-receives-basic-messages
  (doseq [format [:transit :json]]
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-queue-opts-standard
          handler-chan
          {:format format}

          (testing "handle-queue can send/receive basic message to standard queue"
            (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                               fixtures/test-standard-queue-name
                                               (first test-messages-basic)
                                               {:format format})))
            (is (= (first test-messages-basic) (:body (<!! handler-chan)))))

          (testing "handle-queue can send/receive large message to standard queue"
            (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                               fixtures/test-standard-queue-name
                                               test-message-large
                                               {:format format})))
            (is (= test-message-large (:body (<!! handler-chan))))))))))

(deftest handle-queue-sends-and-receives-timestamped-message
  (testing "handle-queue can send/receive message including timestamp to standard queue"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-standard
          handler-chan

          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             test-message-with-time)))
          (is (= test-message-with-time (:body (<!! handler-chan)))))))))

(deftest handle-queue-sends-and-receives-fifo-messages
  (testing "handle-queue can send/receive basic messages to FIFO queue"
    (doseq [format [:transit :json]]
      (let [handler-chan (chan)]
        (fixtures/with-test-fifo-queue
          (fixtures/with-handle-queue-queue-opts-fifo
            handler-chan
            {:format format}

            (doseq [message test-messages-basic]
              (is (string? (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                                      fixtures/test-fifo-queue-name
                                                      message
                                                      (helpers/random-group-id)
                                                      {:format format}))))
            (doseq [message test-messages-basic]
              (let [received-message (<!! handler-chan)]
                (is (= message (:body received-message)))))))))))

(deftest handle-queue-terminates-with-non-existing-queue
  (testing "handle-queue terminates when non-existing queue is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-queue-opts-standard
                handler-chan
                {:queue-name "non-existing-queue"})]

          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

(deftest handle-queue-terminates-after-restart-count-exceeded
  (testing "handle-queue terminates when the restart-count exceeds the limit"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (with-redefs-fn {#'sqs/receive-message
                         (fn [_ _ _] (ex-info "Bam!" {:cause "wtf"}))}
          #(let [stop-fn (fixtures/with-handle-queue-standard-no-stop
                          handler-chan)]
             (Thread/sleep 3000)
             (let [stats (stop-fn)]
               (is (= (:restart-count stats) 10))))))
      (close! handler-chan))))

(deftest handle-queue-terminates-with-non-existing-bucket
  (testing "handle-queue terminates when non-existing bucket is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-aws-opts-standard
                handler-chan
                {:s3-bucket-name "non-existing-bucket"})]

          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             fixtures/test-standard-queue-name
                                             test-message-large)))
          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

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

(deftest done-fn-handle-present-when-auto-delete-false
  (testing "done-fn handle is present in response when auto-delete is false"
    (doseq [format [:transit]]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (fixtures/with-handle-queue-queue-opts-standard
            handler-chan
            {:format      format
             :auto-delete false}

            (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                               fixtures/test-standard-queue-name
                                               (first test-messages-basic)
                                               {:format format})))
            (let [received-message (<!! handler-chan)]
              (is (= (first test-messages-basic) (:body received-message)))
              (is (contains? received-message :done-fn))
              (is (fn? (:done-fn received-message))))))
        (close! handler-chan)))))

(deftest done-fn-handle-absent-when-auto-delete-true
  (testing "done-fn handle is not present in response when auto-delete is true"
    (doseq [format [:transit :json]]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (fixtures/with-handle-queue-queue-opts-standard
            handler-chan
            {:format      format
             :auto-delete true}

            (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                               fixtures/test-standard-queue-name
                                               (first test-messages-basic)
                                               {:format format})))
            (let [received-message (<!! handler-chan)]
              (is (= (first test-messages-basic) (:body received-message)))
              (is (not (contains? received-message :done-fn))))))
        (close! handler-chan)))))

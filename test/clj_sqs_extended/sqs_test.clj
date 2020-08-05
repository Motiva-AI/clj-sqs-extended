(ns clj-sqs-extended.sqs-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce ^:private test-messages
  (into [] (take 5 (repeatedly helpers/random-message))))

(defonce ^:private test-message-larger-than-256kb
  (helpers/random-string-with-length 300000))

(deftest can-receive-message-when-idle
  (testing "Receive empty response when no message has been send before"
    (fixtures/with-standard-queue
      (doseq [format [:transit :json]]
        (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                @fixtures/test-standard-queue-url
                                                {:format format})]
          (is (= true (empty? response))))))))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (fixtures/with-standard-queue
      (let [test-message (first test-messages)]
        (doseq [format [:transit :json]]
          (log/infof "Message sent. ID: '%s'"
                      (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                            @fixtures/test-standard-queue-url
                                            test-message {:format format}))
          (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                  @fixtures/test-standard-queue-url
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-auto-delete-message
  (testing "Auto-Deleting a single message after receiving it"
    (fixtures/with-standard-queue
      (let [test-message (first test-messages)]
        (sqs-ext/send-message @fixtures/test-sqs-ext-client
                              @fixtures/test-standard-queue-url
                              test-message)
        (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                @fixtures/test-standard-queue-url
                                                {:auto-delete true})]
          (is (= test-message (:body response)))
          (is (= 0 (helpers/get-total-message-amount-in-queue @fixtures/test-sqs-ext-client
                                                              @fixtures/test-standard-queue-url))))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (fixtures/with-fifo-queue
      (doseq [format [:transit :json]]
        (doseq [test-message test-messages]
          (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                     @fixtures/test-fifo-queue-url
                                     test-message
                                     (helpers/random-group-id)
                                     {:format format}))
        (doseq [test-message test-messages]
          (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                  @fixtures/test-fifo-queue-url
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (fixtures/with-standard-queue
      (sqs-ext/send-message @fixtures/test-sqs-ext-client
                            @fixtures/test-standard-queue-url
                            test-message-larger-than-256kb
                            {:format :raw})
      (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-standard-queue-url
                                              {:format :raw})
            desired-length (count test-message-larger-than-256kb)
            response-length (->> (:body response) (count))]
        (is (= desired-length response-length))))))
(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(defonce ext-sqs-client (atom nil))
(defonce standard-queue-url (atom nil))
(defonce fifo-queue-url (atom nil))

(use-fixtures :once (partial fixtures/with-test-ext-sqs-client ext-sqs-client))

(defonce ^:private test-messages
  (into [] (take 5 (repeatedly helpers/random-message))))

(defonce ^:private test-message-larger-than-256kb
  (helpers/random-string-with-length 300000))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (fixtures/with-standard-queue ext-sqs-client standard-queue-url
      (let [test-message (first test-messages)]
        (doseq [format [:transit :json]]
          (sqs-ext/send-message @ext-sqs-client
                                @standard-queue-url
                                test-message {:format format})
          (let [response (sqs-ext/receive-message @ext-sqs-client
                                                  @standard-queue-url
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-auto-delete-message
  (testing "Auto-Deleting a single message after receiving it"
    (fixtures/with-standard-queue ext-sqs-client standard-queue-url
      (let [test-message (first test-messages)]
        (sqs-ext/send-message @ext-sqs-client
                              @standard-queue-url
                              test-message)
        (let [response (sqs-ext/receive-message @ext-sqs-client
                                                @standard-queue-url
                                                {:auto-delete true})]
          (is (= test-message (:body response)))
          (is (= 0 (helpers/get-total-message-amount-in-queue @ext-sqs-client
                                                              @standard-queue-url))))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (fixtures/with-fifo-queue ext-sqs-client fifo-queue-url
      (doseq [format [:transit :json]]
        (doseq [test-message test-messages]
          (sqs-ext/send-fifo-message @ext-sqs-client
                                     @fifo-queue-url
                                     test-message
                                     (helpers/random-group-id)
                                     {:format format}))
        (doseq [test-message test-messages]
          (let [response (sqs-ext/receive-message @ext-sqs-client
                                                  @fifo-queue-url
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (fixtures/with-standard-queue ext-sqs-client standard-queue-url
      (sqs-ext/send-message @ext-sqs-client
                            @standard-queue-url
                            test-message-larger-than-256kb
                            {:format :raw})
      (let [response (sqs-ext/receive-message @ext-sqs-client
                                              @standard-queue-url
                                              {:format :raw})
            desired-length (count test-message-larger-than-256kb)
            response-length (->> (:body response) (count))]
        (is (= desired-length response-length))))))

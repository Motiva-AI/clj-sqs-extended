(ns clj-sqs-extended.sqs-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-messages
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-larger-than-256kb
         (helpers/random-string-with-length 300000))

(deftest can-receive-message-when-idle
  (testing "Receive empty response when no message has been send before"
    (fixtures/with-test-standard-queue
      (let [response (sqs/receive-message @fixtures/test-sqs-ext-client
                                          @fixtures/test-queue-url)]
        (is (empty? response))))))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (fixtures/with-test-standard-queue
      (let [test-message (first test-messages)]
        (doseq [format [:transit :json]]
          (log/infof "Message sent. ID: '%s'"
                     (sqs/send-message @fixtures/test-sqs-ext-client
                                       @fixtures/test-queue-url
                                       test-message {:format format}))
          (let [response (sqs/receive-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url)]
            (is (= test-message (:body response)))))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (fixtures/with-test-fifo-queue
      (doseq [format [:transit :json]]
        (doseq [test-message test-messages]
          (sqs/send-fifo-message @fixtures/test-sqs-ext-client
                                 @fixtures/test-queue-url
                                 test-message
                                 (helpers/random-group-id)
                                 {:format format}))
        (doseq [test-message test-messages]
          (let [response (sqs/receive-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url)]
            (is (= test-message (:body response)))))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (fixtures/with-test-standard-queue
      (sqs/send-message @fixtures/test-sqs-ext-client
                        @fixtures/test-queue-url
                        test-message-larger-than-256kb)
      (let [response (sqs/receive-message @fixtures/test-sqs-ext-client
                                          @fixtures/test-queue-url)]
        (is (= test-message-larger-than-256kb (:body response)))))))

(deftest create-queue-request-attributes-attached-correctly
  (testing "Creating a queue create request with single attributes works as expected"
    (let [test-request (sqs/build-create-queue-request-with-attributes
                          "test-queue-name"
                          {:fifo "true"})]
     (is (= (.getAttributes test-request) {"FifoQueue" "true"}))))

  (testing "Creating a queue create request with multiple attributes works as expected"
    (let [test-request (sqs/build-create-queue-request-with-attributes
                          "test-queue-name"
                          {:kms-master-key-id         "UnbreakableMasterKey"
                           :kms-data-key-reuse-period "60"})]

     (is (= (.getAttributes test-request)
            {"KmsMasterKeyId"               "UnbreakableMasterKey"
             "KmsDataKeyReusePeriodSeconds" "60"})))))

(ns clj-sqs-extended.sqs-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [bond.james :as bond]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]))

(use-fixtures :once fixtures/with-test-sqs-ext-client fixtures/with-test-s3-bucket)
(use-fixtures :each fixtures/with-transient-queue)

(defonce test-messages
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-larger-than-256kb
         (helpers/random-string-with-length 300000))

(deftest can-receive-message-when-idle
  (testing "Receive empty response when no message has been send before"
    (let [response (sqs/receive-messages @fixtures/test-sqs-ext-client
                                         @fixtures/test-queue-url)]
      (is (empty? response)))))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (let [test-message (first test-messages)]
      (doseq [format [:transit :json]]
        (log/infof "Message sent. ID: '%s'"
                   (sqs/send-message @fixtures/test-sqs-ext-client
                                     @fixtures/test-queue-url
                                     test-message
                                     {:format format}))
        (let [response (sqs/receive-messages @fixtures/test-sqs-ext-client
                                             @fixtures/test-queue-url)]
          (is (= [test-message] (map :body response))))))))

(deftest safely-receive-nil-message
  ;; sqs/wait-and-receive-messages-from-sqs returns an empty list when
  ;; WaitTimeSeconds is reached
  ;;
  ;; Reference:
  ;; See WaitTimeSeconds section in
  ;; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_ResponseElements
  (bond/with-stub! [[sqs/wait-and-receive-messages-from-sqs (constantly [])]]
    ;; ensure that this doesn't crash
    (is (empty? (sqs/receive-messages @fixtures/test-sqs-ext-client
                                      @fixtures/test-queue-url)))))

(deftest can-receive-fifo-message
  (doseq [format [:transit :json]]
    (let [test-message (first test-messages)]
      (sqs/send-fifo-message @fixtures/test-sqs-ext-client
                             @fixtures/test-queue-url
                             test-message
                             (helpers/random-group-id)
                             {:format format})

      (let [response (sqs/receive-messages @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url)]
        (is (= [test-message] (map :body response)))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (sqs/send-message @fixtures/test-sqs-ext-client
                      @fixtures/test-queue-url
                      test-message-larger-than-256kb)
    (let [response (sqs/receive-messages @fixtures/test-sqs-ext-client
                                         @fixtures/test-queue-url)]
      (is (= [test-message-larger-than-256kb] (map :body response))))))

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
                           :kms-data-key-reuse-period 60})]

     (is (= (.getAttributes test-request)
            {"KmsMasterKeyId"               "UnbreakableMasterKey"
             "KmsDataKeyReusePeriodSeconds" "60"})))))

(deftest message-without-format-attribute-is-received-correctly
  (testing "A message without the format attribute gets read without error"
    (let [plain-message (helpers/random-string-with-length 32)]
      (.sendMessage @fixtures/test-sqs-ext-client
                    @fixtures/test-queue-url
                    plain-message)
      (let [response (sqs/receive-messages @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url)]
        (is (= plain-message (->> response first :body)))))))


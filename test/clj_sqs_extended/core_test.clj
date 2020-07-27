(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-localstack-environment)

(def ^:private test-message "What hath God wrought?")
(def ^:private test-message-larger-than-256kb (helpers/random-string-with-length 300000))

(deftest ^:unit test-send-receive-identical
  (testing "Sending/Receiving message"
    (let [url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue (fixtures/localstack-sqs) url test-message)
      (let [reply (sqs-ext/receive-messages-on-queue (fixtures/localstack-sqs) url)]
        (is (= 1 (count reply)))
        (is (= test-message (:body (first reply))))))))

(deftest ^:unit delete-single-message
  (testing "Deleting single message"
    (let [url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue (fixtures/localstack-sqs) url test-message)
      (let [messages (sqs-ext/receive-messages-on-queue (fixtures/localstack-sqs) url)]
        (is (= 1 (count messages)))
        (sqs-ext/delete-messages-on-queue (fixtures/localstack-sqs) url messages)))))

(deftest ^:unit send-message-larger-than-256kb
  (testing "Sending message with more than 256kb of data (via S3 bucket)"
    (let [url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue (fixtures/localstack-sqs) url test-message-larger-than-256kb)
      (let [messages (sqs-ext/receive-messages-on-queue (fixtures/localstack-sqs) url)
            desired-length (count test-message-larger-than-256kb)
            reply-length (->> (first messages) :body (count))]
        (is (= 1 (count messages)))
        (is (= desired-length reply-length))))))

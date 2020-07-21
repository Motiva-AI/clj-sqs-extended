(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.tools :as tools]
            [clj-sqs-extended.test-fixtures :refer [with-localstack-environment
                                                    localstack-sqs
                                                    test-queue]]))


(use-fixtures :once with-localstack-environment)

(def ^:private test-message "What hath God wrought?")
(def ^:private test-message-large (tools/random-string-with-length 300000))

(deftest ^:unit test-send-receive-identical
  (testing "Sending/Receiving message"
    (let [url (.getQueueUrl (test-queue))]
      (sqs-ext/send-message-on-queue (localstack-sqs) url test-message)
      (let [reply (sqs-ext/receive-messages-on-queue (localstack-sqs) url)]
        (is (= 1 (count reply)))
        (is (= test-message (.getBody (first reply))))))))

(deftest ^:unit delete-single-message
  (testing "Deleting single message"
    (let [url (.getQueueUrl (test-queue))]
      (sqs-ext/send-message-on-queue (localstack-sqs) url test-message)
      (let [messages (sqs-ext/receive-messages-on-queue (localstack-sqs) url)]
        (is (= 1 (count messages)))
        (sqs-ext/delete-messages-on-queue (localstack-sqs) url messages)))))

(deftest ^:unit send-large-message
  (testing "Sending message with more than 256k of data via S3 bucket"
    (let [url (.getQueueUrl (test-queue))]
      (sqs-ext/send-message-on-queue (localstack-sqs) url test-message-large)
      (let [messages (sqs-ext/receive-messages-on-queue (localstack-sqs) url)
            desired-length (count test-message-large)
            reply-length (->> (first messages) (.getBody) (count))]
        (is (= 1 (count messages)))
        (is (= desired-length reply-length))
        ))))

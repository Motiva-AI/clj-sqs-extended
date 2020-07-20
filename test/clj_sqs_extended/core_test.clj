(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-fixtures :refer [with-localstack-environment
                                                    localstack-sqs
                                                    test-queue]]))


(use-fixtures :once with-localstack-environment)

(def ^:private test-message "What hath God wrought?")


(deftest ^:unit test-send-receive-message
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
        (sqs-ext/delete-messages-on-queue (localstack-sqs) url messages)))))

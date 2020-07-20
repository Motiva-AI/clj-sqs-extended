(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]))

; TODO: Use fixtures instead of the helpers.


(def ^:private localstack (helpers/localstack-client))

(def ^:private test-message "What hath God wrought?")


(deftest ^:unit test-create-queue
  (testing "Creating queue"
    (let [result (sqs-ext/create-queue localstack)]
      (is (= com.amazonaws.services.sqs.model.CreateQueueResult
             (class result))))))

(deftest ^:unit test-send-receive-message
  (testing "Sending/Receiving message"
    (let [queue (sqs-ext/create-queue localstack)
          url (.getQueueUrl queue)]
      (sqs-ext/send-message-on-queue localstack url test-message)
      (let [reply (sqs-ext/receive-messages-on-queue localstack url)]
        (is (= 1 (count reply)))
        (is (= test-message (.getBody (first reply))))))))

(deftest ^:unit delete-message
  (testing "Deleting messages"
    (let [queue (sqs-ext/create-queue localstack)
          url (.getQueueUrl queue)]
      (sqs-ext/send-message-on-queue localstack url test-message)
      (let [messages (sqs-ext/receive-messages-on-queue localstack url)]
        (sqs-ext/delete-messages-on-queue localstack url messages)))))

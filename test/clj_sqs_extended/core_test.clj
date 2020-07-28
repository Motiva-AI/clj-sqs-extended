(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-localstack-environment)

(def ^:private test-message-1 "Design is to take things apart in such a way that they can be put back together.")
(def ^:private test-message-2 "State. You're doing it wrong.")
(def ^:private test-message-larger-than-256kb (helpers/random-string-with-length 300000))

(deftest ^:unit can-receive-message
  (testing "Received message body is identical to send message body"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue client url test-message-1)
      (let [messages (sqs-ext/receive-messages-on-queue client url)]
        (is (= 1 (count messages)))
        (is (= test-message-1 (:body (first messages))))))))

(deftest ^:unit can-auto-delete-message
  (testing "Auto-Deleting a single message from queue after receiving it"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/purge-queue client url)
      (sqs-ext/send-message-on-queue client url test-message-1)
      (let [messages (sqs-ext/receive-messages-on-queue
                       client
                       url
                       {:auto-delete true})]
        (is (= 1 (count messages)))
        (is (= test-message-1 (:body (first messages))))
        (is (= 0 (helpers/get-total-message-amount-in-queue client url)))))))

(deftest ^:unit can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-fifo-queue-url)]
      (sqs-ext/send-message-on-queue client url test-message-1)
      (sqs-ext/send-message-on-queue client url test-message-2)
      (let [messages (sqs-ext/receive-messages-on-queue client url)]
        (is (= 1 (count messages)))
        (is (= test-message-1 (:body (first messages)))))
      (let [messages (sqs-ext/receive-messages-on-queue client url)]
        (is (= 1 (count messages)))
        (is (= test-message-2 (:body (first messages))))))))

(deftest ^:unit can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket)"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue client url test-message-larger-than-256kb)
      (let [messages (sqs-ext/receive-messages-on-queue client url)
            desired-length (count test-message-larger-than-256kb)
            response-length (->> (first messages) :body (count))]
        (is (= 1 (count messages)))
        (is (= desired-length response-length))))))

(deftest ^:unit can-receive-batch-of-messages
  (testing "Receiving multiple messages at once"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue client url test-message-1)
      (sqs-ext/send-message-on-queue client url test-message-2)
      (let [messages (sqs-ext/receive-messages-on-queue
                       client
                       url
                       {:timeout 10 :max-messages 10})]
        (is (= 2 (count messages)))))))

(deftest ^:unit can-receive-again-after-visibility-timeout-expired
  (testing "Receive a message again correctly after (fake) processing problems"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/send-message-on-queue client url test-message-1)
      (let [messages (sqs-ext/receive-messages-on-queue client url {:visibility-timeout 1})]
        (is (= 1 (count messages)))
        (is (= test-message-1 (:body (first messages)))))
      (Thread/sleep 2000)
      (let [messages (sqs-ext/receive-messages-on-queue client url)]
        (is (= 1 (count messages)))
        (is (= test-message-1 (:body (first messages))))))))


(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-localstack-environment)

(def ^:private test-message-1
  {:quote "State. You're doing it wrong."
   :by "Rich Hickey"})

(def ^:private test-message-2
  {:quote "Nothing says 'Screw You!' like a DSL."
   :by "Stuart Halloway"})

(def ^:private test-message-larger-than-256kb
  (helpers/random-string-with-length 300000))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (doseq [format [:transit :json]]
        (sqs-ext/send-message client url test-message-1 {:format format})
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message-1 (:body message))))))))

(deftest can-auto-delete-message
  (testing "Auto-Deleting a single message after receiving it"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/purge-queue client url)
      (sqs-ext/send-message client url test-message-1)
      (let [message (sqs-ext/receive-message
                      client
                      url
                      {:auto-delete true})]
        (is (= test-message-1 (:body message)))
        (is (= 0 (helpers/get-total-message-amount-in-queue client url)))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-fifo-queue-url)]
      (doseq [format [:transit :json]]
        (let [group-id-1 (str format "-1")
              group-id-2 (str format "-2")]
          (sqs-ext/send-fifo-message client url test-message-1 group-id-1 {:format format})
          (sqs-ext/send-fifo-message client url test-message-2 group-id-2 {:format format}))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message-1 (:body message))))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message-2 (:body message))))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (sqs-ext/send-message client url test-message-larger-than-256kb {:format :raw})
      (let [message (sqs-ext/receive-message client url {:format :raw})
            desired-length (count test-message-larger-than-256kb)
            response-length (->> (:body message) (count))]
        (is (= desired-length response-length))))))

(deftest can-receive-again-after-visibility-timeout-expired
  (testing "Receive a message again correctly after (fake) processing problems"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)]
      (doseq [format [:transit :json]]
        (sqs-ext/purge-queue client url)
        (sqs-ext/send-message client url test-message-1 {:format format})
        (let [message (sqs-ext/receive-message client url {:format format
                                                           :visibility-timeout 1})]
          (is (= test-message-1 (:body message))))
        (Thread/sleep 2000)
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message-1 (:body message))))))))

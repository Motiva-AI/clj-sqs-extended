(ns clj-sqs-extended.core-test
  "Basic tests for the primary API of `clj-extended-sqs`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-localstack-environment)

(def ^:private test-messages
  [{:quote "State. You're doing it wrong."
    :by    "Rich Hickey"}
   {:quote "Nothing says 'Screw You!' like a DSL."
    :by    "Stuart Halloway"}
   {:quote "I don't care if it works on your machine! We are not shipping your machine!"
    :by    "Vidiu Platon"}
   {:quote "If you can't beat your computer at chess, try kickboxing."
    :by    "Claudio E. Montero"}
   {:quote "I would rather be without a state than without a voice."
    :by    "Edward Snowden"}])

(def ^:private test-message-larger-than-256kb
  (helpers/random-string-with-length 300000))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)
          test-message (first test-messages)]
      (doseq [format [:transit :json]]
        (sqs-ext/send-message client url test-message {:format format})
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message (:body message))))))))

(deftest can-auto-delete-message
  (testing "Auto-Deleting a single message after receiving it"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-queue-url)
          test-message (rand-nth test-messages)]
      (sqs-ext/purge-queue client url)
      (sqs-ext/send-message client url test-message)
      (let [message (sqs-ext/receive-message
                      client
                      url
                      {:auto-delete true})]
        (is (= test-message (:body message)))
        (is (= 0 (helpers/get-total-message-amount-in-queue client url)))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (let [client (fixtures/localstack-sqs)
          url (fixtures/test-fifo-queue-url)]
      (doseq [format [:transit :json]]
        (sqs-ext/purge-queue client url)
        (doseq [message test-messages]
          (sqs-ext/send-fifo-message client
                                     url
                                     message
                                     (helpers/random-string-with-length 5)
                                     {:format format}))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= (nth test-messages 0) (:body message))))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= (nth test-messages 1) (:body message))))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= (nth test-messages 2) (:body message))))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= (nth test-messages 3) (:body message))))
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= (nth test-messages 4) (:body message))))))))

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
          url (fixtures/test-queue-url)
          test-message (rand-nth test-messages)]
      (doseq [format [:transit :json]]
        (sqs-ext/purge-queue client url)
        (sqs-ext/send-message client url test-message {:format format})
        (let [message (sqs-ext/receive-message client url {:format format
                                                           :visibility-timeout 1})]
          (is (= test-message (:body message))))
        (Thread/sleep 2000)
        (let [message (sqs-ext/receive-message client url {:format format})]
          (is (= test-message (:body message))))))))

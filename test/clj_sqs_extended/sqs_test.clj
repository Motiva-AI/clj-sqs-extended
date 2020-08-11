(ns clj-sqs-extended.sqs-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-messages
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-larger-than-256kb
         (helpers/random-string-with-length 300000))

(deftest can-receive-message-when-idle
  (testing "Receive empty response when no message has been send before"
    (fixtures/with-test-standard-queue
      (doseq [format [:transit :json]]
        (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                fixtures/test-standard-queue-name
                                                {:format format})]
          (is (= true (empty? response))))))))

(deftest can-receive-message
  (testing "Sending/Receiving basic maps"
    (fixtures/with-test-standard-queue
      (let [test-message (first test-messages)]
        (doseq [format [:transit :json]]
          (log/infof "Message sent. ID: '%s'"
                     (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           fixtures/test-standard-queue-name
                                           test-message {:format format}))
          (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                  fixtures/test-standard-queue-name
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-receive-fifo-messages
  (testing "Receiving multiple messages from FIFO queue in correct order"
    (fixtures/with-test-fifo-queue
      (doseq [format [:transit :json]]
        (doseq [test-message test-messages]
          (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                     fixtures/test-fifo-queue-name
                                     test-message
                                     (helpers/random-group-id)
                                     {:format format}))
        (doseq [test-message test-messages]
          (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                                  fixtures/test-fifo-queue-name
                                                  {:format format})]
            (is (= test-message (:body response)))))))))

(deftest can-send-message-larger-than-256kb
  (testing "Sending a message with more than 256kb of data (via S3 bucket) in raw format"
    (fixtures/with-test-standard-queue
      (sqs-ext/send-message @fixtures/test-sqs-ext-client
                            fixtures/test-standard-queue-name
                            test-message-larger-than-256kb
                            {:format :raw})
      (let [response (sqs-ext/receive-message @fixtures/test-sqs-ext-client
                                              fixtures/test-standard-queue-name
                                              {:format :raw})
            desired-length (count test-message-larger-than-256kb)
            response-length (->> (:body response) (count))]
        (is (= desired-length response-length))))))

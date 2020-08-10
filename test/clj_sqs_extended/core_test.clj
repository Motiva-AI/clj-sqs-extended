(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!!]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.test-fixtures :as fixtures]))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-message-basic-1 (helpers/random-message-basic))
(defonce test-message-basic-2 (helpers/random-message-basic))
(defonce test-message-with-time (helpers/random-message-with-time))
(defonce test-message-large (helpers/random-message-larger-than-256kb))

(deftest send-and-receive-basic-messages-test
  (doseq [format [:transit :json]]
    (fixtures/with-test-standard-queue
      (let [out-chan (chan)
            stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                          fixtures/test-standard-queue-name
                                          out-chan
                                          {:format format})]
        (is (fn? stop-fn))
        (is (sqs/send-message @fixtures/test-sqs-ext-client
                              fixtures/test-standard-queue-name
                              test-message-basic-1
                              {:format format}))
        (is (= test-message-basic-1 (:body (<!! out-chan))))
        (is (sqs/send-message @fixtures/test-sqs-ext-client
                              fixtures/test-standard-queue-name
                              test-message-basic-2
                              {:format format}))
        (is (= test-message-basic-2 (:body (<!! out-chan))))
        (stop-fn)))))

(deftest send-and-receive-timestamp-message
  (fixtures/with-test-standard-queue
    (let [out-chan (chan)
          stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                        fixtures/test-standard-queue-name
                                        out-chan)]
      (is (fn? stop-fn))
      (is (sqs/send-message @fixtures/test-sqs-ext-client
                            fixtures/test-standard-queue-name
                            test-message-with-time))
      (is (= test-message-with-time (:body (<!! out-chan))))
      (stop-fn))))

(deftest send-and-receive-large-message
  (fixtures/with-test-standard-queue
    (let [out-chan (chan)
          stop-fn (sqs-ext/receive-loop @fixtures/test-sqs-ext-client
                                        fixtures/test-standard-queue-name
                                        out-chan)]
      (is (fn? stop-fn))
      (is (sqs/send-message @fixtures/test-sqs-ext-client
                            fixtures/test-standard-queue-name
                            test-message-large))
      (is (= test-message-large (:body (<!! out-chan))))
      (stop-fn))))


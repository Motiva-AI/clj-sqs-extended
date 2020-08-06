(ns clj-sqs-extended.test-fixtures
  (:require [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.utils :as utils]
            [clj-sqs-extended.test-helpers :as helpers]))


(defonce test-sqs-ext-client (atom nil))
(defonce test-standard-queue-name (helpers/random-queue-name))
(defonce test-fifo-queue-name (helpers/random-queue-name {:suffix ".fifo"}))

(defn wrap-standard-queue
  [f]
  (sqs-ext/create-standard-queue @test-sqs-ext-client
                                 test-standard-queue-name)
  (f)
  (sqs-ext/delete-queue @test-sqs-ext-client
                        test-standard-queue-name))

(defmacro with-standard-queue [& body]
  `(wrap-standard-queue (fn [] ~@body)))

(defn wrap-fifo-queue
  [f]
  (sqs-ext/create-fifo-queue @test-sqs-ext-client
                             test-fifo-queue-name)
  (f)
  (sqs-ext/delete-queue @test-sqs-ext-client
                        test-fifo-queue-name))

(defmacro with-fifo-queue [& body]
  `(wrap-fifo-queue (fn [] ~@body)))

(defn with-test-sqs-ext-client
  [f]
  (let [localstack-endpoint (utils/configure-endpoint)
        localstack-creds (utils/configure-credentials)
        s3-client (s3/s3-client localstack-endpoint
                                localstack-creds)
        bucket-name (s3/create-bucket s3-client
                                      (helpers/random-bucket-name))]
    (reset! test-sqs-ext-client (sqs-ext/sqs-ext-client bucket-name
                                                        localstack-endpoint
                                                        localstack-creds))
    (f)
    (s3/purge-bucket s3-client
                     bucket-name)))
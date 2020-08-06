(ns clj-sqs-extended.test-fixtures
  (:require [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.utils :as utils]
            [clj-sqs-extended.test-helpers :as helpers]))


(defonce test-sqs-ext-client (atom nil))

(def test-standard-queue-url (atom nil))
(def test-fifo-queue-url (atom nil))

(defn wrap-standard-queue
  [f]
  (let [queue (sqs-ext/create-standard-queue
                @test-sqs-ext-client
                (helpers/random-queue-name "queue-" ".standard"))]
    (reset! test-standard-queue-url (.getQueueUrl queue))
    (f)
    (sqs-ext/delete-queue @test-sqs-ext-client @test-standard-queue-url)))

(defmacro with-standard-queue [& body]
  `(wrap-standard-queue (fn [] ~@body)))

(defn wrap-fifo-queue
  [f]
  (let [queue (sqs-ext/create-fifo-queue
                @test-sqs-ext-client
                (helpers/random-queue-name "queue-" ".fifo"))]
    (reset! test-fifo-queue-url (.getQueueUrl queue))
    (f)
    (sqs-ext/delete-queue @test-sqs-ext-client @test-fifo-queue-url)))

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
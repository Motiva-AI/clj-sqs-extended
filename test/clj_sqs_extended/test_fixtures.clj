(ns clj-sqs-extended.test-fixtures
  (:require [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.tools :as tools]
            [clj-sqs-extended.test-helpers :as helpers]))


(defonce ^:private ext-sqs-client (atom nil))
(defonce ^:private queue (atom nil))
(defonce ^:private fifo-queue (atom nil))

(defn localstack-sqs
  "Tests should call this function to get an initialized SQS client to use for
   calling API requests on."
  []
  @ext-sqs-client)

(defn test-queue-url
  []
  (.getQueueUrl @queue))

(defn test-fifo-queue-url
  []
  (.getQueueUrl @fifo-queue))

(defn with-localstack-environment
  "Provides a complete set of S3/SQS localstack infrastructure for testing."
  [f]
  (let [queue-name (helpers/random-queue-name "queue-" ".standard")
        fifo-queue-name (helpers/random-queue-name "queue-" ".fifo")
        bucket-name (helpers/random-bucket-name)
        localstack-endpoint (helpers/configure-endpoint
                              "http://localhost:4566"
                              "us-east-2")
        localstack-creds (helpers/configure-credentials
                           "localstack"
                           "localstack")
        s3-client (tools/s3-client localstack-endpoint
                                   localstack-creds)
        bucket (tools/create-bucket s3-client
                                    bucket-name)]
    (reset! ext-sqs-client (sqs-ext/ext-sqs-client bucket
                                                   localstack-endpoint
                                                   localstack-creds))
    (reset! queue (sqs-ext/create-standard-queue @ext-sqs-client
                                                 queue-name))
    (reset! fifo-queue (sqs-ext/create-fifo-queue @ext-sqs-client
                                                  fifo-queue-name))
    (f)
    (sqs-ext/delete-queue @ext-sqs-client
                          (test-queue-url))
    (sqs-ext/delete-queue @ext-sqs-client
                          (test-fifo-queue-url))
    (tools/purge-bucket s3-client
                        bucket-name)))

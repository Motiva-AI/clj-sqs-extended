(ns clj-sqs-extended.test-fixtures
  (:require [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.tools :as tools]
            [clj-sqs-extended.test-helpers :as helpers]))


(defonce ^:private s3-client (atom nil))
(defonce ^:private sqs-client (atom nil))
(defonce ^:private bucket (atom nil))
(defonce ^:private queue (atom nil))
(defonce ^:private fifo-queue (atom nil))

(defn localstack-s3
  "Tests should call this function to get an initialized S3 client to use for
   calling API requests on."
  []
  @s3-client)

(defn localstack-sqs
  "Tests should call this function to get an initialized SQS client to use for
   calling API requests on."
  []
  @sqs-client)

(defn test-bucket
  "Tests should call this function to get a bucket ready for testing."
  []
  @bucket)

(defn test-queue-url
  []
  (.getQueueUrl @queue))

(defn test-fifo-queue-url
  []
  (.getQueueUrl @fifo-queue))

(defn with-localstack-environment
  "Provides a complete set of S3/SQS localstack infrastructure for testing."
  [f]
  (let [test-queue-name (tools/random-queue-name)
        test-fifo-queue-name (tools/random-queue-name)
        test-bucket-name (tools/random-bucket-name)
        localstack-endpoint (helpers/configure-endpoint
                              "http://localhost:4566"
                              "us-east-2")
        localstack-creds (helpers/configure-credentials
                           "localstack"
                           "localstack")]
    (reset! s3-client (sqs-ext/s3-client localstack-endpoint
                                         localstack-creds))
    (reset! bucket (sqs-ext/create-bucket @s3-client
                                          test-bucket-name))
    (reset! sqs-client (sqs-ext/sqs-client @s3-client
                                           @bucket
                                           localstack-endpoint
                                           localstack-creds))
    (reset! queue (sqs-ext/create-queue @sqs-client
                                        test-queue-name))
    (reset! fifo-queue (sqs-ext/create-queue @sqs-client
                                             test-fifo-queue-name))
    (f)
    (sqs-ext/delete-queue @sqs-client
                          (test-queue-url))
    (sqs-ext/delete-queue @sqs-client
                          (test-fifo-queue-url))
    (sqs-ext/purge-bucket @s3-client
                          test-bucket-name)))

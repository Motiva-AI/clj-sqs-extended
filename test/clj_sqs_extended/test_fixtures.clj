(ns clj-sqs-extended.test-fixtures
  "Provides some helper functions to provide a convenient testing environment
   connected to the Localstack backend."
  (:require [clj-sqs-extended.core :as sqs]
            [clj-sqs-extended.tools :as tools]))


(def ^:private s3-client (atom nil))
(def ^:private sqs-client (atom nil))
(def ^:private bucket (atom nil))
(def ^:private queue (atom nil))

(def test-queue-name (tools/random-queue-name))

(def test-bucket-name (tools/random-bucket-name))

(def localstack-endpoint (sqs/configure-endpoint
                          "http://localhost:4566"
                          "us-east-2"))

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

(defn test-queue
  "Tests should call this function to get a queue ready for testing."
  []
  @queue)

(defn with-localstack-environment
  "Provides a complete set of S3/SQS localstack infrastructure for testing."
  [f]
  (reset! s3-client (sqs/s3-client localstack-endpoint))
  (reset! bucket (sqs/create-bucket @s3-client test-bucket-name))
  (reset! sqs-client (sqs/sqs-client @s3-client
                                     @bucket
                                     localstack-endpoint))
  (reset! queue (sqs/create-queue @sqs-client test-queue-name))
  (f)
  (sqs/delete-queue @sqs-client (.getQueueUrl @queue))
  (sqs/purge-bucket @s3-client test-bucket-name))

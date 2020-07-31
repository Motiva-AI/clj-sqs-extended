(ns clj-sqs-extended.test-fixtures
  (:require [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.test-helpers :as helpers]))


(defn wrap-standard-queue
  [ext-sqs-client url f]
  (let [queue (sqs-ext/create-standard-queue
                @ext-sqs-client
                (helpers/random-queue-name "queue-" ".standard"))]
    (reset! url (.getQueueUrl queue))
    (f)
    (sqs-ext/delete-queue @ext-sqs-client @url)))

(defmacro with-standard-queue
  [ext-sqs-client url & body]
  `(wrap-standard-queue ~ext-sqs-client ~url (fn [] ~@body)))

(defn wrap-fifo-queue
  [ext-sqs-client url f]
  (let [queue (sqs-ext/create-fifo-queue
                @ext-sqs-client
                (helpers/random-queue-name "queue-" ".fifo"))]
    (reset! url (.getQueueUrl queue))
    (f)
    (sqs-ext/delete-queue @ext-sqs-client @url)))

(defmacro with-fifo-queue
  [ext-sqs-client url & body]
  `(wrap-fifo-queue ~ext-sqs-client ~url (fn [] ~@body)))

(defn with-test-ext-sqs-client
  [ext-sqs-client f]
  (let [bucket-name (helpers/random-bucket-name)
        localstack-endpoint (helpers/configure-endpoint
                              "http://localhost:4566"
                              "us-east-2")
        localstack-creds (helpers/configure-credentials
                           "localstack"
                           "localstack")
        s3-client (s3/s3-client localstack-endpoint
                                localstack-creds)
        bucket (s3/create-bucket s3-client
                                 bucket-name)]
    (reset! ext-sqs-client (sqs-ext/ext-sqs-client bucket
                                                   localstack-endpoint
                                                   localstack-creds))
    (f)
    (s3/purge-bucket s3-client
                     bucket-name)))

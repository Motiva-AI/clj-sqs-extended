(ns clj-sqs-extended.test-fixtures
  (:require [environ.core :refer [env]]
            [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.aws :as aws]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.test-helpers :as helpers]))


(def aws-config {:access-key   (env :aws-access-key-id)
                 :secret-key   (env :aws-secret-access-key)
                 :endpoint-url (env :aws-sqs-endpoint-url)
                 :region       (env :aws-sqs-region)})

(defonce test-sqs-ext-client (atom nil))
(defonce test-standard-queue-name (helpers/random-queue-name))
(defonce test-fifo-queue-name (helpers/random-queue-name {:suffix ".fifo"}))

(defn wrap-standard-queue
  [f]
  (sqs-ext/create-standard-queue @test-sqs-ext-client
                                 test-standard-queue-name)
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs-ext/delete-queue @test-sqs-ext-client
                        test-standard-queue-name))

(defmacro with-test-standard-queue [& body]
  `(wrap-standard-queue (fn [] ~@body)))

(defn wrap-fifo-queue
  [f]
  (sqs-ext/create-fifo-queue @test-sqs-ext-client
                             test-fifo-queue-name)
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs-ext/delete-queue @test-sqs-ext-client
                        test-fifo-queue-name))

(defmacro with-test-fifo-queue [& body]
  `(wrap-fifo-queue (fn [] ~@body)))

(defn with-test-sqs-ext-client
  [f]
  (let [localstack-endpoint (aws/configure-endpoint aws-config)
        localstack-creds (aws/configure-credentials aws-config)
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

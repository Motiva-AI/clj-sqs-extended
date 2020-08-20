(ns clj-sqs-extended.core-no-fixtures-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import [com.amazonaws
            AmazonServiceException
            SdkClientException]))


(def ^:private aws-down-config {:access-key   (env :aws-access-key-id)
                                :secret-key   (env :aws-secret-access-key)
                                :endpoint-url "https://currently-unreachable-aws-endpoint.com"
                                :region       (env :aws-sqs-region)})

(def ^:private aws-config {:access-key   (env :aws-access-key-id)
                           :secret-key   (env :aws-secret-access-key)
                           :endpoint-url (env :aws-sqs-endpoint-url)
                           :region       (env :aws-sqs-region)})

(deftest unreachable-endpoint-yields-proper-exception
  (testing "Trying to connect to an unreachable endpoint yields a proper exception"
    (let [unreachable-client (sqs/sqs-ext-client aws-down-config
                                                 "test-bucket")]
      ;; WATCHOUT: Apparently the SDK does not verify the endpoint when the client
      ;;           is instantiated so we need to fire off a request in order to trigger
      ;;           the desired exception:
      (is (thrown? SdkClientException
                   (sqs-ext/send-message unreachable-client
                                         "test-queue"
                                         {:data "here-be-dragons"}))))))

(deftest cannot-send-to-non-existing-bucket
  (testing "Sending a large message using a non-existing S3 bucket yields proper exception"
    (let [sqs-ext-client (sqs/sqs-ext-client aws-config
                                             "non-existing-bucket")]
      ;; WATCHOUT: Apparently the SDK does not verify the existance of the used S3
      ;;           bucket when the client is in instantiated so we need to send a
      ;;           large message that would require the bucket to be used in order
      ;;           to trigger the desired exception:
      (sqs/create-standard-queue sqs-ext-client
                                 "test-queue")
      (is (thrown? AmazonServiceException
                   ;; WATCHOUT: Inside the SDK there is an additional AmazonS3Exception
                   ;;           being thrown that Apparently is impossible to catch here?!
                   (sqs-ext/send-message sqs-ext-client
                                         "test-queue"
                                         (helpers/random-message-larger-than-256kb))))
      (sqs/delete-queue sqs-ext-client
                        "test-queue"))))

;; WATCHOUT: Interesting! Apparently the queue remembers the S3 bucket
;;           where it needs to look for a stored message so this test has
;;           no meaning? TODO: Remove (!?)
(deftest cannot-receive-from-non-existing-bucket
  (testing "Receiving a large message from a non-existing S3 bucket yields proper exception"
    (let [s3-client (s3/s3-client aws-config)
          queue-name (helpers/random-queue-name)
          bucket-name (s3/create-bucket s3-client
                                        (helpers/random-bucket-name))
          good-sqs-ext-client (sqs/sqs-ext-client aws-config
                                                  bucket-name)
          test-message-large (helpers/random-message-larger-than-256kb)]
      ;; Send a large message and leave it in the queue ...
      (sqs/create-standard-queue good-sqs-ext-client
                                 queue-name)
      (sqs-ext/send-message good-sqs-ext-client
                            queue-name
                            test-message-large)

      ;; Now setup receiving with the correct queue, but non-existing bucket ...
      (let [bad-sqs-ext-client (sqs/sqs-ext-client aws-config
                                                   "non-existing-bucket")
            out-chan (chan)
            stop-fn (receive/receive-loop bad-sqs-ext-client
                                          queue-name
                                          out-chan)]
        (is (fn? stop-fn))
        (is (= test-message-large (:body (<!! out-chan))))
        (stop-fn))

      (sqs/purge-queue good-sqs-ext-client
                       queue-name)
      (s3/purge-bucket s3-client
                       bucket-name))))

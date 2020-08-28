(ns clj-sqs-extended.core-no-fixtures-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import [com.amazonaws
            AmazonServiceException
            SdkClientException]))


(def ^:private aws-down-config {:access-key     (env :access-key)
                                :secret-key (env :secret-key)
                                :s3-endpoint   "https://currently-unreachable-aws-endpoint.com"
                                :sqs-endpoint  "https://currently-unreachable-aws-endpoint.com"
                                :region            (env :region)})

(def ^:private aws-up-config {:access-key     (env :access-key)
                              :secret-key (env :secret-key)
                              :s3-endpoint   (env :s3-endpoint)
                              :sqs-endpoint  (env :sqs-endpoint)
                              :region            (env :region)})

(deftest unreachable-endpoint-yields-proper-exception
  (testing "Trying to connect to an unreachable endpoint yields a proper exception"
    (let [unreachable-client (sqs/sqs-ext-client aws-down-config
                                                 "test-bucket")]
      ;; WATCHOUT: Apparently the SDK does not verify the endpoint when the client
      ;;           is instantiated so we need to fire off a message:
      (is (thrown? SdkClientException
                   (sqs-ext/send-message unreachable-client
                                         "test-queue"
                                         {:data "here-be-dragons"}))))))

(deftest cannot-send-to-non-existing-bucket
  (testing "Sending a large message using a non-existing S3 bucket yields proper exception"
    (let [sqs-ext-client (sqs/sqs-ext-client aws-up-config
                                             "non-existing-bucket")]
      ;; WATCHOUT: Apparently the SDK does not verify the existance of the used S3
      ;;           bucket when the client is in instantiated so we need to send a
      ;;           large message that would require the bucket to be used:
      (sqs/create-standard-queue sqs-ext-client
                                 "test-queue")
      (is (thrown? AmazonServiceException
                   (sqs-ext/send-message sqs-ext-client
                                         "test-queue"
                                         (helpers/random-message-larger-than-256kb))))
      (sqs/delete-queue sqs-ext-client
                        "test-queue"))))

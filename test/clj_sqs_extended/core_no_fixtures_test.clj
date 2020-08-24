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


(def ^:private aws-down-config {:aws-access-key-id     (env :aws-access-key-id)
                                :aws-secret-access-key (env :aws-secret-access-key)
                                :aws-s3-endpoint-url   "https://currently-unreachable-aws-endpoint.com"
                                :aws-sqs-endpoint-url  "https://currently-unreachable-aws-endpoint.com"
                                :aws-region            (env :aws-region)})

(def ^:private aws-up-config {:aws-access-key-id     (env :aws-access-key-id)
                              :aws-secret-access-key (env :aws-secret-access-key)
                              :aws-s3-endpoint-url   (env :aws-s3-endpoint-url)
                              :aws-sqs-endpoint-url  (env :aws-sqs-endpoint-url)
                              :aws-region            (env :aws-region)})

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

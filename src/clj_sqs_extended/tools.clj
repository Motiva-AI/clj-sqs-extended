(ns clj-sqs-extended.tools
  "Provides the core functionalities of the wrapped library."
  (:require [clojure.tools.logging :as log])
  (:import [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.s3.model
              ListVersionsRequest
              BucketLifecycleConfiguration
              BucketLifecycleConfiguration$Rule]))

(defn configure-endpoint
  [url region]
  (AwsClientBuilder$EndpointConfiguration. url region))

(defn configure-bucket-lifecycle
  "Creates a bucket lifecylce configuration with the passed status and expiration in days."
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

(defn peak-message
  "Dumps some information about the passed SQS message on the screen for verification."
  [message]
  (log/infof
    "ID: %s\nReceipt handle: %s\nMessage body (first 5 characters): %s"
    (.getMessageId message)
    (.getReceiptHandle message)
    (subs (.getBody message) 0 5)))

(defn wipe-bucket
  "Deletes the passed bucket including all meta-information (summaries, versions) via
   the passed S3 interface."
  [s3 bucket-name]
  (letfn [(delete-object-summaries [s]
            (.deleteObject s3 bucket-name (.getKey s)))
          (delete-object-versions [v]
            (.deleteVersion s3 bucket-name (.getKey v) (.getVersionId v)))]
    (loop [objects (.listObjects s3 bucket-name)]
      (map delete-object-summaries (.getObjectSummaries objects))
      (when (.isTruncated objects)
        (recur (.listNextBatchOfObjects objects))))
    (let [version-request (-> (ListVersionsRequest.) (.withBucketName bucket-name))
          version-list (.listVersions s3 version-request)]
      (map delete-object-versions (.getVersionSummaries version-list)))
    (.deleteBucket s3 bucket-name)))

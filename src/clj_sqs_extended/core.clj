(ns clj-sqs-extended.core
  "Provides the core functionalities of the wrapped library."
  (:require [clj-sqs-extended.tools :as tools]
            [clojure.tools.logging :as log])
  (:import [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model ListVersionsRequest]
           [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazon.sqs.javamessaging
              AmazonSQSExtendedClient
              ExtendedClientConfiguration]
           [com.amazonaws.services.sqs.model
              ReceiveMessageRequest
              DeleteMessageRequest]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.s3.model
              BucketLifecycleConfiguration
              BucketLifecycleConfiguration$Rule]))


(defn configure-endpoint
  "Creates an endpoint configuration for the passed url and region."
  [url region]
  (AwsClientBuilder$EndpointConfiguration. url region))

(defn configure-bucket-lifecycle
  "Creates a bucket lifecylce configuration with the passed status and expiration in days."
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

(defn s3-client
  "Initializes a new S3 client with the passed settings."
  [configuration]
  (let [builder (-> (AmazonS3ClientBuilder/standard)
                    (.withPathStyleAccessEnabled true))
        builder (if configuration (.withEndpointConfiguration builder configuration) builder)]
    (.build builder)))

(defn sqs-client
  "Initializes a new SQS extended client with the passed settings."
  [s3-client bucket configuration]
  (let [sqs-config (-> (ExtendedClientConfiguration.)
                       (.withLargePayloadSupportEnabled s3-client bucket))
        builder (AmazonSQSClientBuilder/standard)
        builder (if configuration (.withEndpointConfiguration builder configuration) builder)]
    (AmazonSQSExtendedClient. (.build builder) sqs-config)))

(defn create-bucket
  "Creates a bucket with passed settings. Uses a random name and a lifecycle of 14 days if
   no other settings are provided."
  ([s3-client]
   (create-bucket s3-client (tools/random-bucket-name)))
  ([s3-client name]
   (create-bucket s3-client name (configure-bucket-lifecycle "Enabled" 14)))
  ([s3-client name lifecycle]
   (doto s3-client
         (.createBucket name)
         (.setBucketLifecycleConfiguration name lifecycle))
   name))

(defn purge-bucket
  "Deletes the passed bucket including all meta-information (summaries, versions) via
   the passed S3 interface."
  [s3-client bucket-name]
  (letfn [(delete-objects [objects]
                          (doseq [o objects]
                            (let [key (.getKey o)]
                              (log/infof "Deleting object '%s'." key)
                              (.deleteObject s3-client bucket-name key))))
          (delete-object-versions [versions]
                                  (doseq [v versions]
                                    (let [key (.getKey v)
                                          id (.getVersionId v)]
                                      (log/infof "Deleting version with id '%s' of '%s'." id key)
                                      (.deleteVersion s3-client bucket-name key id))))]
    (loop [objects (.listObjects s3-client bucket-name)]
      (delete-objects (.getObjectSummaries objects))
      (when (.isTruncated objects)
        (recur (.listNextBatchOfObjects objects))))
    (let [version-request (-> (ListVersionsRequest.) (.withBucketName bucket-name))
          versions (->> (.listVersions s3-client version-request) (.getVersionSummaries))]
      (delete-object-versions versions))
    (.deleteBucket s3-client bucket-name)))

(defn create-queue
  "Creates a new queue with the passed name via the passed sqs client interface."
  ([sqs-client]
   (create-queue sqs-client (tools/random-queue-name)))
  ([sqs-client name]
   (.createQueue sqs-client name)))

(defn delete-queue
  "Deletes the queue at the passed URL via the passed sqs client interface."
  [sqs-client url]
  (.deleteQueue sqs-client url))

(defn send-message-on-queue
  "Sends the passed message data on the url of the provided sqs-client."
  [sqs-client queue-url message]
  (.sendMessage sqs-client queue-url message))

(defn receive-messages-on-queue
  "Receives messages at the passed queue url via the provided SQS interface."
  [sqs-client url]
  (let [request (doto (ReceiveMessageRequest. url)
                      (.setWaitTimeSeconds (int 10))
                      (.setMaxNumberOfMessages (int 10)))
        result (.receiveMessage sqs-client request)]
    (.getMessages result)))

(defn delete-messages-on-queue
  "Deletes the batch of passed messages in the passed queue URL via the provided
   SQS interface."
  [sqs-client url batch]
  (doseq [message batch]
    (.deleteMessage sqs-client (DeleteMessageRequest. url (.getReceiptHandle message)))))

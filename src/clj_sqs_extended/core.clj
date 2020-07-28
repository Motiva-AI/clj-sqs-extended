(ns clj-sqs-extended.core
  "Provides the core functionalities of the wrapped library."
  (:require [clj-sqs-extended.tools :as tools])
  (:import [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model ListVersionsRequest]
           [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazon.sqs.javamessaging
            AmazonSQSExtendedClient
            ExtendedClientConfiguration]
           [com.amazonaws.services.sqs.model
            CreateQueueRequest
            ReceiveMessageRequest
            DeleteMessageRequest
            PurgeQueueRequest]))


; TODO: Move into some internal ns.
(defn s3-client
  [endpoint credentials]
  (let [builder (-> (AmazonS3ClientBuilder/standard)
                    (.withPathStyleAccessEnabled true))
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if credentials (.withCredentials builder credentials) builder)]
    (.build builder)))

(defn sqs-client
  [s3-client bucket endpoint credentials]
  (let [sqs-config (-> (ExtendedClientConfiguration.)
                       (.withLargePayloadSupportEnabled s3-client bucket))
        builder (AmazonSQSClientBuilder/standard)
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if credentials (.withCredentials builder credentials) builder)]
    (AmazonSQSExtendedClient. (.build builder) sqs-config)))

(defn create-bucket
  ([s3-client]
   (create-bucket s3-client (tools/random-bucket-name)))
  ([s3-client name]
   (create-bucket s3-client name (tools/configure-bucket-lifecycle "Enabled" 14)))
  ([s3-client name lifecycle]
   (doto s3-client
     (.createBucket name)
     (.setBucketLifecycleConfiguration name lifecycle))
   name))

(defn purge-bucket
  [s3-client bucket-name]
  (letfn [(delete-objects [objects]
            (doseq [o objects]
              (let [key (.getKey o)]
                (.deleteObject s3-client bucket-name key))))
          (delete-object-versions [versions]
            (doseq [v versions]
              (let [key (.getKey v)
                    id (.getVersionId v)]
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
  ([sqs-client]
   (create-queue sqs-client (tools/random-queue-name) {}))
  ([sqs-client name]
   (create-queue sqs-client name {}))
  ([sqs-client name
    {:keys [fifo
            visibility-timeout
            kms-master-key-id
            kms-data-key-reuse-period]
     :as   opts}]
   (let [request (CreateQueueRequest. name)]
     (when fifo
       (doto request (.addAttributesEntry
                       "FifoQueue" "true")))
     (when visibility-timeout
       (doto request (.addAttributesEntry
                       "VisibilityTimeout" (str visibility-timeout))))
     (when kms-master-key-id
       (doto request (.addAttributesEntry
                       "KmsMasterKeyId" kms-master-key-id)))
     (when kms-data-key-reuse-period
       (doto request (.addAttributesEntry
                       "KmsDataKeyReusePeriodSeconds" kms-data-key-reuse-period)))
     (.createQueue sqs-client request))))

(defn purge-queue
  [sqs-client url]
  (let [request (PurgeQueueRequest. url)]
    (.purgeQueue sqs-client request)))

(defn delete-queue
  [sqs-client url]
  (.deleteQueue sqs-client url))

(defn send-message-on-queue
  [sqs-client queue-url message]
  (.sendMessage sqs-client queue-url message))

(defn delete-messages-on-queue
  [sqs-client url batch]
  (doseq [message batch]
    (let [request (DeleteMessageRequest. url (:receiptHandle message))]
      (.deleteMessage sqs-client request))))

(defn receive-messages-on-queue
  ([sqs-client url]
   (receive-messages-on-queue sqs-client url {}))
  ([sqs-client url
    {:keys [wait-time
            max-messages
            visibility-timeout
            auto-delete]
     :or   {wait-time    0
            max-messages 1}
     :as   opts}]
   (let [request (doto (ReceiveMessageRequest. url)
                   (.setWaitTimeSeconds (int wait-time))
                   (.setMaxNumberOfMessages (int max-messages)))
         result (.receiveMessage sqs-client request)
         messages (map #(-> (bean %) (select-keys [:messageId :receiptHandle :body]))
                       (.getMessages result))]
     (when visibility-timeout
       (doseq [m messages]
         (.changeMessageVisibility sqs-client
                                   url
                                   (:receiptHandle m)
                                   (int visibility-timeout))))
     (when auto-delete
       (delete-messages-on-queue sqs-client url messages))
     messages)))
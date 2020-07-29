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
            SendMessageRequest
            ReceiveMessageRequest
            DeleteMessageRequest
            PurgeQueueRequest]))


; TODO: Move into some internal tooling ns.
(defn s3-client
  [endpoint credentials]
  (let [builder (-> (AmazonS3ClientBuilder/standard)
                    (.withPathStyleAccessEnabled true))
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if credentials (.withCredentials builder credentials) builder)]
    (.build builder)))

; TODO: s3-client to local binding with same credentials
(defn sqs-client
  [s3-client bucket endpoint credentials]
  (let [sqs-config (-> (ExtendedClientConfiguration.)
                       (.withLargePayloadSupportEnabled s3-client bucket))
        builder (AmazonSQSClientBuilder/standard)
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if credentials (.withCredentials builder credentials) builder)]
    (AmazonSQSExtendedClient. (.build builder) sqs-config)))

; TODO: Move into some internal tooling ns.
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

; TODO: Move into some internal tooling ns.
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

(defn- create-queue
  ([sqs-client name]
   (create-queue sqs-client name {}))
  ([sqs-client name
    {:keys [fifo
            kms-master-key-id
            kms-data-key-reuse-period]
     :as   opts}]
   (let [request (CreateQueueRequest. name)]
     (when fifo
       (doto request (.addAttributesEntry
                       "FifoQueue" "true")))
     (when kms-master-key-id
       (doto request (.addAttributesEntry
                       "KmsMasterKeyId" kms-master-key-id)))
     (when kms-data-key-reuse-period
       (doto request (.addAttributesEntry
                       "KmsDataKeyReusePeriodSeconds" kms-data-key-reuse-period)))
     (.createQueue sqs-client request))))

(defn create-standard-queue
  ([sqs-client name]
   (create-queue sqs-client name {}))
  ([sqs-client name
    {:keys [kms-master-key-id
            kms-data-key-reuse-period]
     :as   opts}]
   (create-queue sqs-client name opts)))

(defn create-fifo-queue
  ([sqs-client name]
   (create-queue sqs-client name {:fifo true}))
  ([sqs-client name
    {:keys [fifo
            kms-master-key-id
            kms-data-key-reuse-period]
     :or   {fifo true}
     :as   opts}]
   (create-queue sqs-client name opts)))

(defn purge-queue
  [sqs-client url]
  (let [request (PurgeQueueRequest. url)]
    (.purgeQueue sqs-client request)))

(defn delete-queue
  [sqs-client url]
  (.deleteQueue sqs-client url))

(defn send-message
  [sqs-client url message]
  (.sendMessage sqs-client url message))

(defn send-fifo-message
  [sqs-client url message group-id]
  (let [request (SendMessageRequest. url message)]
    ; WATCHOUT: The group ID is mandatory when sending fifo messages.
    (doto request (.setMessageGroupId group-id))
    (.sendMessage sqs-client request)))

(defn delete-message
  [sqs-client url message]
  (let [request (DeleteMessageRequest. url (:receiptHandle message))]
    (.deleteMessage sqs-client request)))

(defn receive-message
  ([sqs-client url]
   (receive-message sqs-client url {}))
  ([sqs-client url
    {:keys [wait-time
            visibility-timeout
            auto-delete]
     :or   {wait-time 0}
     :as   opts}]
   (letfn [(extract-relevant-keys [message]
             (-> (bean message)
                 (select-keys [:messageId :receiptHandle :body])))]
     (let [request (doto (ReceiveMessageRequest. url)
                     (.setWaitTimeSeconds (int wait-time))
                     (.setMaxNumberOfMessages (int 1)))
           result (.receiveMessage sqs-client request)
           message (->> (.getMessages result) (first) (extract-relevant-keys))]
       (when visibility-timeout
         (.changeMessageVisibility sqs-client
                                   url
                                   (:receiptHandle message)
                                   (int visibility-timeout)))
       (when auto-delete
         (delete-message sqs-client url message))
       message))))

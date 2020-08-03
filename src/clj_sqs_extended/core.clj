(ns clj-sqs-extended.core
  (:require [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.serdes :as serdes])
  (:import [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazon.sqs.javamessaging
            AmazonSQSExtendedClient
            ExtendedClientConfiguration]
           [com.amazonaws.services.sqs.model
            CreateQueueRequest
            SendMessageRequest
            ReceiveMessageRequest
            DeleteMessageRequest
            PurgeQueueRequest]))


(defn ext-sqs-client
  [s3-bucket-name endpoint credentials]
  (let [s3-client (s3/s3-client endpoint credentials)
        sqs-config (-> (ExtendedClientConfiguration.)
                       (.withLargePayloadSupportEnabled s3-client s3-bucket-name))
        builder (AmazonSQSClientBuilder/standard)
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if credentials (.withCredentials builder credentials) builder)]
    (AmazonSQSExtendedClient. (.build builder) sqs-config)))

(defn- create-queue
  ([sqs-client name]
   (create-queue sqs-client name {}))

  ([sqs-client name
    {:keys [fifo
            kms-master-key-id
            kms-data-key-reuse-period]}]
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
  (->> (PurgeQueueRequest. url)
       (.purgeQueue sqs-client)))

(defn delete-queue
  [sqs-client url]
  (.deleteQueue sqs-client url))

(defn send-message
  ([sqs-client url message]
   (send-message sqs-client url message {}))

  ([sqs-client url message
    {:keys [format]
     :or   {format :transit}}]
   (->> (serdes/serialize message format)
        (SendMessageRequest. url)
        (.sendMessage sqs-client)
        (.getMessageId))))

(defn send-fifo-message
  ([sqs-client url message group-id]
   (send-fifo-message sqs-client url message group-id {}))

  ([sqs-client url message group-id
    {:keys [format
            deduplication-id]
     :or   {format :transit}}]
   (let [request (->> (serdes/serialize message format)
                      (SendMessageRequest. url))]
     ;; WATCHOUT: The group ID is mandatory when sending fifo messages.
     (doto request (.setMessageGroupId group-id))
     (when deduplication-id
       ;; WATCHOUT: Refer to https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/SendMessageRequest.html#setMessageDeduplicationId-java.lang.String-
       (doto request (.setMessageDeduplicationId deduplication-id)))
     (.sendMessage sqs-client request))))

(defn delete-message
  [sqs-client url message]
  (->> (DeleteMessageRequest. url (:receiptHandle message))
       (.deleteMessage sqs-client)))

(defn receive-message
  ([sqs-client url]
   (receive-message sqs-client url {}))

  ([sqs-client url
    {:keys [format
            wait-time
            auto-delete]
     :or   {format :transit
            wait-time 0}}]
   (letfn [(extract-relevant-keys [message]
             (-> (bean message)
                 (select-keys [:messageId :receiptHandle :body])))]
     (let [request (doto (ReceiveMessageRequest. url)
                     (.setWaitTimeSeconds (int wait-time))
                     ;; WATCHOUT: This is a design choice to read one message at a time from the queue
                     (.setMaxNumberOfMessages (int 1)))
           response (.receiveMessage sqs-client request)
           message (->> (.getMessages response) (first) (extract-relevant-keys))
           payload (serdes/deserialize (:body message) format)]
       (when auto-delete
         (delete-message sqs-client url message))
       (assoc message :body payload)))))

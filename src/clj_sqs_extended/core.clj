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
  ([sqs-client url body]
   (send-message sqs-client url body {}))
  ([sqs-client url body
    {:keys [format]
     :or   {format :transit}
     :as   opts}]
   (let [payload (serdes/serialize body format)]
     (.sendMessage sqs-client url payload))))

(defn send-fifo-message
  ([sqs-client url body group-id]
   (send-fifo-message sqs-client url body group-id {}))
  ([sqs-client url body group-id
    {:keys [format]
     :or   {format :transit}
     :as   opts}]
   (let [payload (serdes/serialize body format)
         request (SendMessageRequest. url payload)]
     ; WATCHOUT: The group ID is mandatory when sending fifo messages.
     (doto request (.setMessageGroupId group-id))
     (.sendMessage sqs-client request))))

(defn delete-message
  [sqs-client url handle]
  (let [request (DeleteMessageRequest. url (:receiptHandle handle))]
    (.deleteMessage sqs-client request)))

(defn receive-message
  ([sqs-client url]
   (receive-message sqs-client url {}))
  ([sqs-client url
    {:keys [format
            wait-time
            visibility-timeout
            auto-delete]
     :or   {format :transit
            wait-time 0}
     :as   opts}]
   (letfn [(extract-relevant-keys [message]
             (-> (bean message)
                 (select-keys [:messageId :receiptHandle :body])))]
     (let [request (doto (ReceiveMessageRequest. url)
                     (.setWaitTimeSeconds (int wait-time))
                     (.setMaxNumberOfMessages (int 1)))
           response (.receiveMessage sqs-client request)
           message (->> (.getMessages response) (first) (extract-relevant-keys))
           payload (serdes/deserialize (:body message) format)]
       (when visibility-timeout
         (.changeMessageVisibility sqs-client
                                   url
                                   (:receiptHandle message)
                                   (int visibility-timeout)))
       (when auto-delete
         (delete-message sqs-client url message))
       (assoc message :body payload)))))

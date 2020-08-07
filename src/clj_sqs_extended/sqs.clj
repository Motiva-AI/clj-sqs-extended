(ns clj-sqs-extended.sqs
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
            DeleteMessageRequest]))


(defn sqs-ext-client
  ;; XXXFI: This shall build an extended client, therefore the bucket is mandatory.
  [s3-bucket-name endpoint creds]
  (let [s3-client (s3/s3-client endpoint creds)
        sqs-config (-> (ExtendedClientConfiguration.)
                       (.withLargePayloadSupportEnabled s3-client s3-bucket-name))
        builder (AmazonSQSClientBuilder/standard)
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if creds (.withCredentials builder creds) builder)]
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

(defn queue-name-to-url
  [sqs-client name]
  ;; WATCHOUT: Yes, there will be a GetQueueUrlResult returned for which getQueueUrl
  ;;           has to be called again.
  (->> (.getQueueUrl sqs-client name)
       (.getQueueUrl)))

(defn delete-queue
  [sqs-client name]
  (let [url (queue-name-to-url sqs-client name)]
    (.deleteQueue sqs-client url)))

(defn send-message
  ([sqs-client queue-name message]
   (send-message sqs-client queue-name message {}))

  ([sqs-client queue-name message
    {:keys [format]
     :or   {format :transit}}]
   (let [url (queue-name-to-url sqs-client queue-name)]
     (->> (serdes/serialize message format)
          (SendMessageRequest. url)
          (.sendMessage sqs-client)
          (.getMessageId)))))

(defn send-fifo-message
  ([sqs-client queue-name message group-id]
   (send-fifo-message sqs-client queue-name message group-id {}))

  ([sqs-client queue-name message group-id
    {:keys [format
            deduplication-id]
     :or   {format :transit}}]
   (let [url (queue-name-to-url sqs-client queue-name)
         request (->> (serdes/serialize message format)
                      (SendMessageRequest. url))]
     ;; WATCHOUT: The group ID is mandatory when sending fifo messages.
     (doto request (.setMessageGroupId group-id))
     (when deduplication-id
       ;; WATCHOUT: Refer to https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/SendMessageRequest.html#setMessageDeduplicationId-java.lang.String-
       (doto request (.setMessageDeduplicationId deduplication-id)))
     (.sendMessage sqs-client request))))

(defn delete-message
  [sqs-client queue-name message]
  (let [url (queue-name-to-url sqs-client queue-name)]
    (->> (DeleteMessageRequest. url (:receiptHandle message))
         (.deleteMessage sqs-client))))

(defn receive-message
  ([sqs-client queue-name]
   (receive-message sqs-client queue-name {}))

  ([sqs-client queue-name
    {:keys [format
            wait-time
            auto-delete]
     :or   {format    :transit
            wait-time 0}}]
   (letfn [(extract-relevant-keys [message]
             (if message
               (-> (bean message)
                   (select-keys [:messageId :receiptHandle :body]))
               {}))]
     (let [url (queue-name-to-url sqs-client queue-name)
           request (doto (ReceiveMessageRequest. url)
                     (.setWaitTimeSeconds (int wait-time))
                     ;; WATCHOUT: This is a design choice to read one message at a time from the queue
                     (.setMaxNumberOfMessages (int 1)))
           response (.receiveMessage sqs-client request)
           message (->> (.getMessages response) (first) (extract-relevant-keys))]
       (when auto-delete
         (delete-message sqs-client queue-name message))
       (if-let [payload (serdes/deserialize (:body message) format)]
         (assoc message :body payload)
         message)))))

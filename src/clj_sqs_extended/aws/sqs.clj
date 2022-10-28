(ns clj-sqs-extended.aws.sqs
  (:require [clj-sqs-extended.aws.configuration :as aws]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.internal.serdes :as serdes])
  (:import [com.amazon.sqs.javamessaging
            AmazonSQSExtendedClient
            ExtendedClientConfiguration]
           [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazonaws.services.sqs.model
            CreateQueueRequest
            DeleteMessageRequest
            MessageAttributeValue
            PurgeQueueRequest
            ReceiveMessageRequest
            SendMessageRequest
            GetQueueAttributesRequest]))


;; WATCHOUT: Refer to https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
(def ^:private clj-sqs-ext-format-attribute "clj-sqs-extended.serdes-format")

(defn- build-message-format-attribute-value
  [format]
  (doto (MessageAttributeValue.)
        (.withDataType "String")
        (.withStringValue (str (name format)))))

(defn sqs-ext-client
  "Takes a map of configuration and returns a client object.

   Arguments:

   A map of the following optional keys used for accessing AWS services:
     access-key     - AWS access key ID
     secret-key     - AWS secret access key
     s3-endpoint    - AWS S3 endpoint (protocol://service-code.region-code.amazonaws.com)
     s3-bucket-name - AWS S3 bucket to use to store messages larger than 256kb (optional)
     sqs-endpoint   - AWS SQS endpoint (protocol://service-code.region-code.amazonaws.com)
     region         - AWS region
  "
  [{:keys [access-key
           secret-key
           s3-endpoint
           s3-bucket-name
           sqs-endpoint
           region
           cleanup-s3-payload?]
    :or {cleanup-s3-payload? true}
    :as   sqs-ext-config}]
  (let [endpoint (aws/configure-sqs-endpoint sqs-ext-config)
        creds (aws/configure-credentials sqs-ext-config)
        s3-client (when s3-bucket-name
                    (s3/s3-client sqs-ext-config))
        sqs-config (cond-> (ExtendedClientConfiguration.)
                     s3-client (.withPayloadSupportEnabled
                                 s3-client
                                 s3-bucket-name
                                 cleanup-s3-payload?))
        builder (AmazonSQSClientBuilder/standard)
        builder (if region (.setRegion builder region) builder)
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if creds (.withCredentials builder creds) builder)]
    (AmazonSQSExtendedClient. (.build builder) sqs-config)))

(defn build-create-queue-request-with-attributes
  [name
   {:keys [fifo
           visibility-timeout-in-seconds
           kms-master-key-id
           kms-data-key-reuse-period]}]
  (cond-> (CreateQueueRequest. name)
    fifo
    (.addAttributesEntry "FifoQueue"
                         "true")

    visibility-timeout-in-seconds
    (.addAttributesEntry "VisibilityTimeout"
                         (str visibility-timeout-in-seconds))

    kms-master-key-id
    (.addAttributesEntry "KmsMasterKeyId"
                         (str kms-master-key-id))

    kms-data-key-reuse-period
    (.addAttributesEntry "KmsDataKeyReusePeriodSeconds"
                         (str kms-data-key-reuse-period))))

(defn- create-queue
  ([sqs-client name]
   (create-queue sqs-client name {}))

  ([sqs-client name attributes]
   (->> (build-create-queue-request-with-attributes name attributes)
        (.createQueue sqs-client)
        (.getQueueUrl))))

(defn create-standard-queue!
  ([sqs-client queue-name]
   (create-queue sqs-client queue-name {}))

  ([sqs-client
    queue-name
    {:keys [visibility-timeout-in-seconds
            kms-master-key-id
            kms-data-key-reuse-period]
     :as   opts}]
   (create-queue sqs-client queue-name opts)))

(defn create-fifo-queue!
  ([sqs-client queue-name]
   (create-queue sqs-client queue-name {}))

  ([sqs-client queue-name
    {:keys [visibility-timeout-in-seconds
            kms-master-key-id
            kms-data-key-reuse-period]
     :as   opts}]
   (create-queue sqs-client queue-name (assoc opts :fifo true))))

(defn delete-queue!
  [sqs-client queue-url]
  (.deleteQueue sqs-client queue-url))

(defn purge-queue!
  [sqs-client queue-url]
  (->> (PurgeQueueRequest. queue-url)
       (.purgeQueue sqs-client)))

(defn- map-vals [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn queue-attributes
  ([sqs-client queue-url]
   (->> (queue-attributes
          sqs-client
          queue-url
          ["ApproximateNumberOfMessages"
           "ApproximateNumberOfMessagesNotVisible"])
        (map-vals #(Integer/parseInt %))))

  ([sqs-client queue-url attribute-names]
   (->> (GetQueueAttributesRequest. queue-url attribute-names)
        (.getQueueAttributes sqs-client)
        (.getAttributes))))

(defn send-message
  "Send a message to a standard queue.

  Argments:
    sqs-client - The extended sqs-client via which the request shall be sent
    queue-url  - the queue's URL
    message    - The actual message/data to be sent

  Options:
    format     - The format (currently :json or :transit) to serialize the outgoing
                 message with (default: :transit)"
  ([sqs-client queue-url message]
   (send-message sqs-client queue-url message {}))

  ([sqs-client queue-url message
    {:keys [format]
     :or   {format :transit}}]
   (let [request (->> (serdes/serialize message format)
                      (SendMessageRequest. queue-url))]
     (doto request (.addMessageAttributesEntry clj-sqs-ext-format-attribute
                                               (build-message-format-attribute-value format)))
     (->> request
          (.sendMessage sqs-client)
          (.getMessageId)))))

(defn send-fifo-message
  "Send a message to a FIFO queue.

  Argments:
    sqs-client       - The extended sqs-client via which the request shall be sent
    queue-url        - the queue's URL
    message          - The actual message/data to be sent
    message-group-id - A value that will be parsed as string to specify the
                       group that this message belongs to. Messages belonging
                       to the same group are guaranteed FIFO

  Options:
    format           - The format (currently :json or :transit) to serialize the outgoing
                       message with (default: :transit)
    deduplication-id - A string used for deduplication of sent messages"
  ([sqs-client queue-url message group-id]
   (send-fifo-message sqs-client queue-url message group-id {}))

  ([sqs-client queue-url message group-id
    {:keys [format
            deduplication-id]
     :or   {format :transit}}]
   (let [request (->> (serdes/serialize message format)
                      (SendMessageRequest. queue-url))]
     ;; WATCHOUT: group ID is mandatory when sending fifo messages
     (doto request (.setMessageGroupId (str group-id))
                   (.addMessageAttributesEntry clj-sqs-ext-format-attribute
                                               (build-message-format-attribute-value format)))
     (when deduplication-id
       ;; Refer to https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/SendMessageRequest.html#setMessageDeduplicationId-java.lang.String-
       (doto request (.setMessageDeduplicationId (str deduplication-id))))
     (->> request
          (.sendMessage sqs-client)
          (.getMessageId)))))

(defn delete-message!
  [sqs-client queue-url message-or-receipt-handle]
  (let [receipt-handle (if (map? message-or-receipt-handle)
                         (:receiptHandle message-or-receipt-handle)
                         message-or-receipt-handle)]
    (->> receipt-handle
         (DeleteMessageRequest. queue-url)
         (.deleteMessage sqs-client))))

(defn- get-serdes-format-attribute
  [message]
  (some-> message
          (.getMessageAttributes)
          (get clj-sqs-ext-format-attribute)
          (.getStringValue)
          (keyword)))

(defn- extract-relevant-keys-from-message
  [message]
  (some-> message
          (bean)
          (select-keys [:messageId :receiptHandle :body])))

(defn- receive-messages-request
  [queue-url wait-time-in-seconds max-number-of-receiving-messages]
  (doto (ReceiveMessageRequest. queue-url)
      (.setWaitTimeSeconds (int wait-time-in-seconds))
      ;; this below is to satisfy some quirk with SQS for our custom serdes-format attribute to be received
      (.setMessageAttributeNames [clj-sqs-ext-format-attribute])
      (.setMaxNumberOfMessages (when max-number-of-receiving-messages (int max-number-of-receiving-messages)))))

(defn wait-and-receive-messages-from-sqs
  [sqs-client queue-url
   {wait-time-in-seconds             :wait-time-in-seconds
    max-number-of-receiving-messages :max-number-of-receiving-messages}]
  (->> (receive-messages-request queue-url wait-time-in-seconds max-number-of-receiving-messages)
       (.receiveMessage sqs-client)
       (.getMessages)))

(defn- parse-message
  [raw-message]
  (let [coll   (extract-relevant-keys-from-message raw-message)
        format (get-serdes-format-attribute raw-message)]
    (if (seq coll)
      (assoc coll :format format)
      coll)))

(defn- deserialize-message-if-formatted
  [{message-body   :body
    message-format :format
    :as message}]
  (if message-format
    (->> (serdes/deserialize message-body message-format)
         (assoc message :body))

    message))

(defn receive-messages
  ([sqs-client queue-url]
   (receive-messages sqs-client queue-url {}))

  ([sqs-client queue-url
    {:keys [wait-time-in-seconds
            max-number-of-receiving-messages]
     ;; Defaults to maximum long polling
     ;; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
     :or   {wait-time-in-seconds 20}}]
   (try
     (->> (wait-and-receive-messages-from-sqs
            sqs-client
            queue-url
            {:wait-time-in-seconds             wait-time-in-seconds
             :max-number-of-receiving-messages max-number-of-receiving-messages})
          (map parse-message)
          (map deserialize-message-if-formatted))
     (catch Throwable ex
       ;; returns exception
       ex))))


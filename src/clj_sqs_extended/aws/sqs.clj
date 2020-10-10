(ns clj-sqs-extended.aws.sqs
  (:require [clojure.core.async :as async :refer [chan go >! <!]]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clj-sqs-extended.aws.configuration :as aws]
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
            SendMessageRequest]))


;; WATCHOUT: Refer to https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
(def ^:private clj-sqs-ext-format-attribute "clj-sqs-extended.serdes-format")

(defn- build-message-format-attribute-value
  [format]
  (doto (MessageAttributeValue.)
        (.withDataType "String")
        (.withStringValue (str (name format)))))

(defn sqs-ext-client
  [sqs-ext-config]
  (let [endpoint (aws/configure-sqs-endpoint sqs-ext-config)
        creds (aws/configure-credentials sqs-ext-config)
        s3-client (when (:s3-bucket-name sqs-ext-config)
                    (s3/s3-client sqs-ext-config))
        sqs-config (cond-> (ExtendedClientConfiguration.)
                      s3-client (.withPayloadSupportEnabled s3-client
                                                            (:s3-bucket-name sqs-ext-config)))
        builder (AmazonSQSClientBuilder/standard)
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
  [sqs-client queue-url message]
  (->> (DeleteMessageRequest. queue-url (:receiptHandle message))
       (.deleteMessage sqs-client)))

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

(def ^:private max-number-of-receiving-messages (int 10))

(defn- receive-messages-request
  [queue-url wait-time-in-seconds]
  (doto (ReceiveMessageRequest. queue-url)
      (.setWaitTimeSeconds (int wait-time-in-seconds))
      ;; this below is to satisfy some quirk with SQS for our custom serdes-format attribute to be received
      (.setMessageAttributeNames [clj-sqs-ext-format-attribute])
      (.setMaxNumberOfMessages max-number-of-receiving-messages)))

(defn wait-and-receive-messages-from-sqs
  [sqs-client queue-url wait-time-in-seconds]
  (->> (receive-messages-request queue-url wait-time-in-seconds)
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
    {:keys [wait-time-in-seconds]
     ;; Defaults to maximum long polling
     ;; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
     :or   {wait-time-in-seconds 20}}]
   (go
     (try
       (->> (wait-and-receive-messages-from-sqs
              sqs-client
              queue-url
              wait-time-in-seconds)
            (map parse-message)
            (map deserialize-message-if-formatted))
       (catch Throwable ex
         ;; returns exception
         ex)))))

(defn- throw-err [x]
  (when (instance? Exception x)
    (throw x))
  x)

(defmacro <?
  "Same as <! but throws error if an error is returned from channel"
  [channel]
  `(throw-err (clojure.core.async/<! ~channel)))

(defn receive-to-channel
  [sqs-client queue-url opts]
  (let [ch (chan max-number-of-receiving-messages)]
    (go
      (try
        (loop []
          (let [messages (<? (receive-messages sqs-client queue-url opts))]
            (when-not (empty? messages)
              (<! (async/onto-chan ch messages false))))

          (when-not (async-protocols/closed? ch)
            (recur)))
        (catch Throwable e
          (>! ch e)))

      ;; close channel when exiting loop
      (async/close! ch))

    ch))


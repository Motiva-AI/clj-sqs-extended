(ns clj-sqs-extended.aws.sqs
  (:require [clojure.core.async :refer [chan go-loop <! >!]]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clj-sqs-extended.aws.configuration :as aws]
            [clj-sqs-extended.aws.s3 :as s3]
            [clojure.tools.logging :as log]
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


(def ^:private clj-sqs-ext-format-attribute "serialized-with")

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
    format     - The format (currently :json or :transit) to serialize outgoing
                 messages with (optional, default: :transit)"
  ([sqs-client queue-url message]
   (send-message sqs-client queue-url message {}))

  ([sqs-client queue-url message
    {:keys [format]
     :or   {format :transit}}]
   (let [request (->> (serdes/serialize message format)
                      (SendMessageRequest. queue-url))
         message-attribute-format (doto (MessageAttributeValue.)
                                        (.withDataType "String")
                                        (.withStringValue (str (name format))))]
     (doto request (.addMessageAttributesEntry clj-sqs-ext-format-attribute
                                               message-attribute-format))
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
    format           - The format (currently :json or :transit) to serialize outgoing
                       messages with (optional, default: :transit)
    deduplication-id - A string used for deduplication of sent messages"
  ([sqs-client queue-url message group-id]
   (send-fifo-message sqs-client queue-url message group-id {}))

  ([sqs-client queue-url message group-id
    {:keys [format
            deduplication-id]
     :or   {format :transit}}]
   (let [request (->> (serdes/serialize message format)
                      (SendMessageRequest. queue-url))
         message-attribute-format (doto (MessageAttributeValue.)
                                        (.withDataType "String")
                                        (.withStringValue (str (name format))))]
     ;; group ID is mandatory when sending fifo messages
     (doto request (.setMessageGroupId (str group-id))
                   (.addMessageAttributesEntry clj-sqs-ext-format-attribute
                                               message-attribute-format))
     (when deduplication-id
       ;; Refer to https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/SendMessageRequest.html#setMessageDeduplicationId-java.lang.String-
       (doto request (.setMessageDeduplicationId deduplication-id)))
     (->> request
          (.sendMessage sqs-client)
          (.getMessageId)))))

(defn delete-message!
  [sqs-client queue-url message]
  (->> (DeleteMessageRequest. queue-url (:receiptHandle message))
       (.deleteMessage sqs-client)))

(defn receive-message
  ([sqs-client queue-url]
   (receive-message sqs-client queue-url {}))

  ([sqs-client queue-url
    {:keys [wait-time]
     :or   {wait-time 0}}]
   (letfn [(get-format-attribute [message]
                                 (when message
                                   (let [attributes (.getMessageAttributes message)
                                         format (get attributes clj-sqs-ext-format-attribute)]
                                     (keyword (.getStringValue format)))))

           (extract-relevant-keys [message]
             (if message
               (-> (bean message)
                   (select-keys [:messageId :receiptHandle :body]))
               {}))]
     (let [request (doto (ReceiveMessageRequest. queue-url)
                     (.setWaitTimeSeconds (int wait-time))
                     ;; WATCHOUT: This is a design choice to read one message at a time from the queue
                     (.setMaxNumberOfMessages (int 1))
                     (.setAttributeNames ["All"]))
           response (.receiveMessage sqs-client request)
           sqs-message (first (.getMessages response))
           message (extract-relevant-keys sqs-message)
           format (get-format-attribute sqs-message)]
       (if-let [payload (serdes/deserialize (:body message) format)]
         (assoc message :body payload)
         message)))))

(defn- receive-to-channel
  [sqs-client queue-url opts]
  (let [ch (chan)]
    (go-loop []
      (try
        (let [message (receive-message sqs-client queue-url opts)]
          (>! ch message))
        (catch Throwable e
          (>! ch e)))
      (when-not (async-protocols/closed? ch)
        (recur)))
    ch))

(defn- multiplex
  [chs]
  (let [ch (chan)]
    (doseq [c chs]
      (go-loop []
        (let [v (<! c)]
          (>! ch v)
          (when (some? v)
            (recur)))))
    ch))

(defn receive-message-channeled
  ([sqs-client queue-url]
   (receive-message-channeled sqs-client queue-url {}))

  ([sqs-client queue-url
    {:keys [num-consumers]
     :or   {num-consumers 1}
     :as   opts}]
   (if (= num-consumers 1)
     (receive-to-channel sqs-client queue-url opts)
     (multiplex
       (loop [chs []
              n num-consumers]
         (if (= n 0)
           chs
           (let [ch (receive-to-channel sqs-client queue-url opts)]
             (recur (conj chs ch) (dec n)))))))))

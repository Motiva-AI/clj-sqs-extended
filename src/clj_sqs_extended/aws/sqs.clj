(ns clj-sqs-extended.aws.sqs
  (:require [clojure.core.async :refer [chan go-loop <! >!]]
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
            PurgeQueueRequest
            ReceiveMessageRequest
            SendMessageRequest]))


(defn sqs-ext-client
  [aws-config & [s3-bucket-name]]
  (let [endpoint (aws/configure-sqs-endpoint aws-config)
        creds (aws/configure-credentials aws-config)
        s3-client (when (some? s3-bucket-name) (s3/s3-client aws-config))
        sqs-config (if s3-client
                     (-> (ExtendedClientConfiguration.)
                         (.withPayloadSupportEnabled s3-client s3-bucket-name))
                     (ExtendedClientConfiguration.))
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

(defn purge-queue
  [sqs-client name]
  (let [url (queue-name-to-url sqs-client name)]
    (->> (PurgeQueueRequest. url)
         (.purgeQueue sqs-client))))

(defn send-message
  "Send a message to a standard queue.

  Argments:
    sqs-client       - The extended sqs-client via which the request shall be sent
    queue-name       - A string containing the name of the queue where the message shall be sent to
    message          - The actual message/data to be sent

  Options:
    format           - The format (currently :json or :transit) to serialize outgoing
                       messages with (optional, default: :transit)"
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
  "Send a message to a FIFO queue.

  Argments:
    sqs-client       - The extended sqs-client via which the request shall be sent
    queue-name       - A string containing the name of the queue where the message shall be sent to
    message          - The actual message/data to be sent
    message-group-id - A string that specifies the group that this message belongs to.
                       Messages belonging to the same group are guaranteed FIFO

  Options:
    format           - The format (currently :json or :transit) to serialize outgoing
                       messages with (optional, default: :transit)
    deduplication-id - A string used for deduplication of sent messages"
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
     (->> request
          (.sendMessage sqs-client)
          (.getMessageId)))))

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
            wait-time]
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
       (if-let [payload (serdes/deserialize (:body message) format)]
         (assoc message :body payload)
         message)))))

(defn- receive-to-channel
  [sqs-client queue-name opts]
  (let [ch (chan)]
    (go-loop []
      (try
        (let [message (receive-message sqs-client queue-name opts)]
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
  ([sqs-client queue-name]
   (receive-message-channeled sqs-client queue-name {}))

  ([sqs-client queue-name
    {:keys [num-consumers
            format]
     :or   {num-consumers 1
            format        :transit}
     :as   opts}]
   (if (= num-consumers 1)
     (receive-to-channel sqs-client queue-name opts)
     (multiplex
       (loop [chs []
              n num-consumers]
         (if (= n 0)
           chs
           (let [ch (receive-to-channel sqs-client queue-name opts)]
             (recur (conj chs ch) (dec n)))))))))

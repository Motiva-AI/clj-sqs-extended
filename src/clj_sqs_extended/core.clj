(ns clj-sqs-extended.core
  "Provides the core functionalities of the wrapped library."
  (:import [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazon.sqs.javamessaging
              AmazonSQSExtendedClient
              ExtendedClientConfiguration]
           [com.amazonaws.services.sqs.model ReceiveMessageRequest]))


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

(defn create-queue
  "Creates a new queue with the passed name via the passed sqs client interface."
  [sqs-client name]
  (.createQueue sqs-client name))

(defn send-message-on-queue
  "Sends the passed message data on the url of the provided sqs-client."
  [sqs-client queue-url message]
  (.sendMessage sqs-client queue-url message))

(defn receive-messages-on-queue
  "Receives messages at the passed queue url via the provided sqs interface."
  [sqs queue-url]
  (let [request (doto (ReceiveMessageRequest. queue-url)
                      (.setWaitTimeSeconds (int 10))
                      (.setMaxNumberOfMessages (int 10)))
        result (.receiveMessage sqs request)]
    (.getMessages result)))

(defn delete-messages-on-queue
  [sqs url batch]
  (doseq [message batch]
    (.deleteMessage sqs (url (.getReceiptHandle message)))))

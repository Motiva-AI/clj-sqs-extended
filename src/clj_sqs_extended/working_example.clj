(ns clj-sqs-extended.working-example
  "Clojure version of the AWS working example found at:
   https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html"
  (:require [clojure.tools.logging :as log]
            [tick.alpha.api :as t])
  (:import [java.util UUID]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model
              BucketLifecycleConfiguration
              BucketLifecycleConfiguration$Rule
              ListVersionsRequest]
           [com.amazonaws.services.sqs AmazonSQSClientBuilder]
           [com.amazonaws.services.sqs.model
              CreateQueueRequest
              SendMessageRequest
              DeleteMessageRequest
              ReceiveMessageRequest]
           [com.amazon.sqs.javamessaging
              AmazonSQSExtendedClient
              ExtendedClientConfiguration]))


(defn- random-bucket-name
  "Creates a random name for the test bucket."
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn- random-queue-name
  "Creates a random name for a dummy queue."
  []
  (str "queue-"
       (UUID/randomUUID)))

(defn- random-junk
  "Creates some random string data to send as an example."
  [length]
  (apply str (take length (repeatedly #(char (+ 97 (rand 26)))))))

(defn- bucket-lifecycle-configuration
  "Creates a bucket lifecylce configuration with the passed status and expiration in days."
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

(defn- create-queue
  "Creates a new queue with the passed name via the passed sqs client interface."
  [sqs-client name]
  (let [queue (.createQueue sqs-client (CreateQueueRequest. name))]
    (.getQueueUrl queue)))

(defn- receive-queue-messages
  "Receives next batch of messages at the passed queue url via the passed sqs interface."
  [sqs queue-url]
  (let [result (.receiveMessage sqs (ReceiveMessageRequest. queue-url))]
    (.getMessages result)))

(defn- dump-message
  "Dumps some information about the passed message on the screen for verification."
  [message]
  (log/infof
    "ID: %s\nReceipt handle: %s\nMessage body (first 5 characters): %s"
    (.getMessageId message)
    (.getReceiptHandle message)
    (subs (.getBody message) 0 5)))

(defn wipe-bucket
  "Deletes the passed bucket including all meta-information (summaries, versions) via
   the passed S3 interface."
  [s3 bucket-name]
  (letfn [(delete-object-summaries [s]
                                   (.deleteObject s3 bucket-name (.getKey s))
                                   (log/info "Deleted a summary object."))
          (delete-object-versions [v]
                           (.deleteVersion s3 bucket-name (.getKey v) (.getVersionId v))
                           (log/info "Deleted a version object."))]
    (loop [objects (.listObjects s3 bucket-name)]
      (map delete-object-summaries (.getObjectSummaries objects))
      (when (.isTruncated objects)
        (recur (.listNextBatchOfObjects objects))))
    (let [version-request (-> (ListVersionsRequest.) (.withBucketName bucket-name))
          version-list (.listVersions s3 version-request)]
      (map delete-object-versions (.getVersionSummaries version-list)))
    (.deleteBucket s3 bucket-name)))

(defn- s3-client
  "Initializes a new S3 client with the passed endpoint URL and AWS region."
  [endpoint-url region]
  (let [localstack-endpoint (AwsClientBuilder$EndpointConfiguration. endpoint-url region)]
    (-> (AmazonS3ClientBuilder/standard)
        (.withEndpointConfiguration localstack-endpoint)
        (.withPathStyleAccessEnabled true)
        (.build))))

(defn- extended-sqs-client
  "Initializes a new SQS extended client with large-payload support enabled for the passed bucket
   via the passed S3 client."
  [s3-client bucket]
  (let [config (-> (ExtendedClientConfiguration.)
                   (.withLargePayloadSupportEnabled s3-client bucket))]
    (AmazonSQSExtendedClient.
     (-> (AmazonSQSClientBuilder/standard)
         (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration. "http://localhost:4566/" "ap-northeast-1"))
         (.build))
     config)))

(defn working-example
  "Does everything the Java version does."
  []
  (let [bucket-name (random-bucket-name)
        queue-name (random-queue-name)
        message-data (random-junk 300000)
        s3 (s3-client "http://localhost:4566" "ap-northeast-1")
        sqs (extended-sqs-client s3 bucket-name)
        lifecycle (bucket-lifecycle-configuration "Enabled" 14)]
    (doto s3
          (.createBucket bucket-name)
          (.setBucketLifecycleConfiguration bucket-name lifecycle))
    (log/infof "Bucket '%s' created and configured." bucket-name)
    (let [queue-url (create-queue sqs queue-name)]
      (log/infof "Created queue at '%s'" queue-url)
      (.sendMessage sqs (SendMessageRequest. queue-url message-data))
      (log/info "Sent message.")
      (let [messages (receive-queue-messages sqs queue-url)]
        (log/infof "%d message(s) received:" (count messages))
        (doseq [m messages] (dump-message m))
        (let [receipt-handle (.getReceiptHandle (first messages))]
          (.deleteMessage sqs (DeleteMessageRequest. queue-url receipt-handle))
          (log/info "All messages deleted."))))
    (wipe-bucket s3 bucket-name)
    (log/info "Bucket with entire contents deleted.")))


; REPL section
(comment

 ; (re)run the main function
 (working-example))

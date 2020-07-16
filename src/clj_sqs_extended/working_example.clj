(ns clj-sqs-extended.working-example
  "Clojure version of the AWS working example found at:
   https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html"
  (:require [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.tools :as aws-tools])
  (:import [java.util UUID]))


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

(defn working-example
  "Does everything the Java version does."
  [bucket-name queue-name]
  (let [endpoint (aws-tools/configure-endpoint "http://localhost:4566" "ap-northeast-1")
        s3 (sqs-ext/s3-client endpoint)
        sqs (sqs-ext/sqs-client s3 bucket-name endpoint)
        lifecycle (aws-tools/configure-bucket-lifecycle "Enabled" 14)]
    (doto s3
          (.createBucket bucket-name)
          (.setBucketLifecycleConfiguration bucket-name lifecycle))
    (log/infof "Bucket '%s' created and configured." bucket-name)
    (let [queue (sqs-ext/create-queue sqs queue-name)
          queue-url (.getQueueUrl queue)]
      (log/infof "Created queue at '%s'" queue-url)
      (sqs-ext/send-message-on-queue sqs queue-url (random-junk 30))
      (log/info "Sent message.")
      (sqs-ext/send-message-on-queue sqs queue-url (random-junk 30))
      (log/info "Sent another message.")
      (let [messages (sqs-ext/receive-messages-on-queue sqs queue-url)]
        (log/infof "%d message(s) received:" (count messages))
        (doseq [m messages] (aws-tools/peak-message m))
        (sqs-ext/delete-messages-on-queue sqs queue-url messages)))
    (aws-tools/wipe-bucket s3 bucket-name)
    (log/info "Bucket with entire contents deleted.")))


; REPL section
(comment

 ; (re)run the main function
 (working-example
  (random-bucket-name)
  (random-queue-name)))

(ns clj-sqs-extended.test-helpers
  (:require [tick.alpha.api :as t])
  (:import [java.util UUID]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.auth
            BasicAWSCredentials
            AWSStaticCredentialsProvider]))


(defn configure-endpoint
  "Creates an AWS endpoint configuration for the passed url and region."
  [url region]
  (AwsClientBuilder$EndpointConfiguration. url region))

(defn configure-credentials
  [access-key secret-key]
  (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret-key)))

(defn random-bucket-name
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn random-queue-name
  [prefix suffix]
  (str prefix
       (UUID/randomUUID)
       suffix))

(defn random-group-id
  []
  (str (UUID/randomUUID)))

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 40 (rand 86))))
       (take length)
       (apply str)))

(defn get-total-message-amount-in-queue
  [sqs-client url]
  (let [requested-attributes ["ApproximateNumberOfMessages"
                              "ApproximateNumberOfMessagesNotVisible"
                              "ApproximateNumberOfMessagesDelayed"]
        result (.getQueueAttributes sqs-client url requested-attributes)]
    (->> (.getAttributes result)
         (vals)
         (map read-string)
         (reduce +))))


(ns clj-sqs-extended.test-helpers
  (:import [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
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

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 40 (rand 86))))
       (take length)
       (apply str)))

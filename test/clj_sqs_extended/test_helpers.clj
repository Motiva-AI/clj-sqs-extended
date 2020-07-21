(ns clj-sqs-extended.test-helpers
  (:import [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]))


(defn configure-endpoint
  "Creates an endpoint configuration for the passed url and region."
  [url region]
  (AwsClientBuilder$EndpointConfiguration. url region))

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 40 (rand 86))))
       (take length)
       (apply str)))

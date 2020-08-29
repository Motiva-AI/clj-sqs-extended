(ns clj-sqs-extended.aws.configuration
  (:import [com.amazonaws.auth
            AWSStaticCredentialsProvider
            BasicAWSCredentials
            DefaultAWSCredentialsProviderChain]
           [com.amazonaws.client.builder
            AwsClientBuilder$EndpointConfiguration]))


(defn configure-sqs-endpoint
  [{:keys [sqs-endpoint region]}]
  ;; WATCHOUT: A specific endpoint is optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the default endpoint instead.
  (when (and (some? sqs-endpoint) (some? region))
    (AwsClientBuilder$EndpointConfiguration. sqs-endpoint region)))

(defn configure-s3-endpoint
  [{:keys [s3-endpoint region]}]
  ;; WATCHOUT: A specific endpoint is optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the default endpoint instead.
  (when (and (some? s3-endpoint) (some? region))
    (AwsClientBuilder$EndpointConfiguration. s3-endpoint region)))

(defn configure-credentials
  [{:keys [access-key secret-key]}]
  (if (and (some? access-key ) (some? secret-key))
    (->> (BasicAWSCredentials. access-key  secret-key)
         (AWSStaticCredentialsProvider.))
    (DefaultAWSCredentialsProviderChain.)))

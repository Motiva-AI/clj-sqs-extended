(ns clj-sqs-extended.aws.configuration
  (:import [com.amazonaws.auth
            AWSStaticCredentialsProvider
            BasicAWSCredentials
            DefaultAWSCredentialsProviderChain]
           [com.amazonaws.client.builder
            AwsClientBuilder$EndpointConfiguration]))


(defn configure-sqs-endpoint
  [{:keys [aws-sqs-endpoint-url aws-region]}]
  ;; WATCHOUT: A specific endpoint is optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the default endpoint instead.
  (when (and (some? aws-sqs-endpoint-url) (some? aws-region))
    (AwsClientBuilder$EndpointConfiguration. aws-sqs-endpoint-url aws-region)))

(defn configure-s3-endpoint
  [{:keys [aws-s3-endpoint-url aws-region]}]
  ;; WATCHOUT: A specific endpoint is optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the default endpoint instead.
  (when (and (some? aws-s3-endpoint-url) (some? aws-region))
    (AwsClientBuilder$EndpointConfiguration. aws-s3-endpoint-url aws-region)))

(defn configure-credentials
  [{:keys [aws-access-key-id aws-secret-access-key]}]
  (if (and (some? aws-access-key-id ) (some? aws-secret-access-key))
    (->> (BasicAWSCredentials. aws-access-key-id  aws-secret-access-key)
         (AWSStaticCredentialsProvider.))
    (DefaultAWSCredentialsProviderChain.)))

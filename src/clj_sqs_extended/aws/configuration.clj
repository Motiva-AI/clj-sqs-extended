(ns clj-sqs-extended.aws.configuration
  (:import [com.amazonaws.auth
            AWSStaticCredentialsProvider
            BasicAWSCredentials]
           [com.amazonaws.client.builder
            AwsClientBuilder$EndpointConfiguration]))


(defn configure-endpoint
  [{:keys [endpoint-url region]}]
  ;; WATCHOUT: A specific endpoint is optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the default endpoint instead.
  (when (and (some? endpoint-url) (some? region))
    (AwsClientBuilder$EndpointConfiguration. endpoint-url region)))

(defn configure-credentials
  [{:keys [access-key secret-key]}]
  ;; WATCHOUT: Credentials are optional in this API, so if the necessary
  ;;           information does not get passed here, this will return nil
  ;;           and the API will use the standard methods for finding the
  ;;           credentials to use instead.
  (when (and (some? access-key) (some? secret-key))
    (->> (BasicAWSCredentials. access-key secret-key)
         (AWSStaticCredentialsProvider.))))

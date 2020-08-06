(ns clj-sqs-extended.aws
  (:import [com.amazonaws.client.builder
            AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.auth
            BasicAWSCredentials
            AWSStaticCredentialsProvider]))


(defn configure-endpoint
  "Creates an AWS endpoint configuration for the passed url and region.

   If no arguments are passed this will create settings to use with a localstack
   environment running on this machine."
  ([]
   (configure-endpoint {}))

  ([{:keys [endpoint-url
            region]
     :or   {endpoint-url "http://localhost:4566"
            region       "us-east-2"}}]
   (AwsClientBuilder$EndpointConfiguration. endpoint-url region)))

(defn configure-credentials
  "Creates basic credentials for the passed keys.

  If no arguments are passed this will create settings to use with a localstack
  environment running on this machine."
  ([]
   (configure-credentials {}))

  ([{:keys [access-key
            secret-key]
     :or   {access-key "default"
            secret-key "default"}}]
    (->> (BasicAWSCredentials. access-key secret-key)
         (AWSStaticCredentialsProvider.))))

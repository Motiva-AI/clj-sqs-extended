(ns clj-sqs-extended.test-helpers
  "Provides some helper functions to provide a convenient testing environment
   connected to the Localstack backend."
  (:require [clj-sqs-extended.core :as sqs]))


; TODO: Fixtures would be better maybe?

(defn localstack-client
  []
  (let [localstack (sqs/configure-endpoint
                    "http://localhost:4566"
                    "us-east-2")
        s3 (sqs/s3-client localstack)
        dummy-bucket (sqs/create-bucket s3)]
    (sqs/sqs-client s3 dummy-bucket localstack)))

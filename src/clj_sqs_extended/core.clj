(ns clj-sqs-extended.core
  (:require [clj-sqs-extended.sqs :as sqs-ext]))


(defn handle-queue
  "Set up a loop that listens to a queue and process incoming messages.

     Arguments:

     aws-creds - A map of the following credential keys, used for interacting with SQS:
       access-key   - AWS access key ID
       secret-key   - AWS secret access key
       sqs-endpoint - SQS queue endpoint - usually an HTTPS based URL
       region       - AWS region

     queue-config - A map of the configuration for this queue
       queue-url     - required: URL of the queue
       s3-bucket-arn - optional: ARN (or whatever URI needed to point to an existing S3,
                                      TBD what minimal setting we need to pass in here)
       num-handler-threads - optional: how many threads to run (defaults: 4)
       auto-delete   - optional: boolean, if true, immediately delete the message,
                       if false, forward a `done` function and leave the
                       message intact. (defaults: true)

     handler-fn - a function which will be passed the incoming message. If
                  auto-delete is false, a second argument will be passed a `done`
                  function to call when finished processing.

    Returns:
    a kill function - call the function to terminate the loop.)"
  [aws-creds queue-config handler-fn]
  (println "I don't do much yet."))
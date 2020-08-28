(ns clj-sqs-extended.core
  (:require [clojure.core.async :refer [chan <! >! <!! thread]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.aws.sqs :as sqs]))


;; Conveniance declarations
(def sqs-ext-client sqs/sqs-ext-client)
(def create-standard-queue sqs/create-standard-queue)
(def create-fifo-queue sqs/create-fifo-queue)
(def send-message sqs/send-message)
(def send-fifo-message sqs/send-fifo-message)


(defn- launch-handler-threads
  [num-handler-threads receive-chan auto-delete handler-fn]
  (dotimes [_ num-handler-threads]
    (thread
      (loop []
        (when-let [message (<!! receive-chan)]
          (try
            (if auto-delete
              (handler-fn message)
              (handler-fn message (:done-fn message)))
            (catch Throwable error
              (log/error error "Handler function threw an error!")))
          (recur))))))

(defn handle-queue
  "Setup a loop that listens to a queue and processes all incoming messages.

  Arguments:
    aws-creds - A map of the following optional keys used for accessing AWS services:
      access-key     - AWS access key ID
      secret-key - AWS secret access key
      s3-endpoint   - AWS S3 endpoint (protocol://service-code.region-code.amazonaws.com)
      sqs-endpoint  - AWS SQS endpoint (protocol://service-code.region-code.amazonaws.com)
      region            - AWS region

    queue-opts - A map for the configuration settings of the queue to handle:
      queue-name            - A string containing the name of the queue to handle (required)
      s3-bucket-name        - A string containing the name of an existing S3 bucket to use to store
                              large messages (required)
      num-handler-threads   - Number of how many threads to run for handling message receival
                              (optional, default: 4)
      restart-limit         - If a potentially non-fatal error occurs while handling the queue,
                              it will try this many times to restart in order to hopefully recover
                              from the occured error (optional, default: 6)
      restart-delay-seconds - If the loop restarts, restart will be delayed by this many seconds
                              (optional, default: 10)
      auto-delete           - A boolean where
                                true: will immediately delete the message

                                false: will provide a `done-fn` key inside the returned messages and
                                       the message will be left untouched by this API
                                (optional, defaults: true)
      format                - The format (currently :json or :transit) to serialize outgoing messages
                              with (optional, default: :transit)

    handler-fn - A function to which new incoming messages will be passed to (required)

                 NOTE: If auto-delete above is set to false, a `done-fn` function will be passed as
                       second argument which should be called to delete the message when processing
                       has been finished.

  Returns:
    A stop function - Call this function to terminate the loop and receive some final
                      information about the finished handling process (hopefully handy for
                      debugging)."
  [{:keys [access-key
           secret-key
           s3-endpoint
           sqs-endpoint
           region]
    :or   {access-key     "default"
           secret-key "default"
           s3-endpoint   "http://localhost:4566"
           sqs-endpoint  "http://localhost:4566"
           region            "us-east-2"}
    :as   aws-creds}
   {:keys [queue-name
           s3-bucket-name
           num-handler-threads
           restart-limit
           restart-delay-seconds
           auto-delete
           format]
    :or   {num-handler-threads   4
           restart-limit         6
           restart-delay-seconds 10
           auto-delete           true
           format                :transit}}
   handler-fn]
  (let [sqs-ext-client (sqs/sqs-ext-client aws-creds s3-bucket-name)
        receive-chan (chan)
        stop-fn (receive/receive-loop sqs-ext-client
                                      queue-name
                                      receive-chan
                                      {:auto-delete           auto-delete
                                       :restart-limit         restart-limit
                                       :restart-delay-seconds restart-delay-seconds
                                       :format                format})]
    (log/infof (str "Now handling queue '%s' with:\n"
                    "  bucket: %s\n"
                    "  num-handler-threads: %d\n"
                    "  restart-limit: %d\n"
                    "  restart-delay-seconds: %d\n"
                    "  auto-delete: %s\n"
                    "  format: %s")
               queue-name
               s3-bucket-name
               num-handler-threads
               restart-limit
               restart-delay-seconds
               auto-delete
               format)
    (launch-handler-threads num-handler-threads
                            receive-chan
                            auto-delete
                            handler-fn)
    stop-fn))

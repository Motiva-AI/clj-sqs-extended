(ns clj-sqs-extended.core
  (:require [clojure.core.async :refer [go-loop chan <!]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.internal.core :refer [provide-with-auto-client-from-config]]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.aws.sqs :as sqs]))


;; Conveniance declarations
(provide-with-auto-client-from-config create-standard-queue! sqs/create-standard-queue!)
(provide-with-auto-client-from-config create-fifo-queue!     sqs/create-fifo-queue!)
(provide-with-auto-client-from-config purge-queue!           sqs/purge-queue!)
(provide-with-auto-client-from-config delete-queue!          sqs/delete-queue!)
(provide-with-auto-client-from-config send-message           sqs/send-message)
(provide-with-auto-client-from-config send-fifo-message      sqs/send-fifo-message)
(provide-with-auto-client-from-config delete-message!        sqs/delete-message!)

(provide-with-auto-client-from-config receive-loop receive/receive-loop)

(defn- launch-handler-threads
  [number-of-handler-threads handler-chan auto-delete handler-fn]
  (dotimes [_ number-of-handler-threads]
    (go-loop []
      (when-let [{message-body :body
                  done-fn      :done-fn}
                 (<! handler-chan)]
        (try
          (if auto-delete
            (handler-fn message-body)
            (handler-fn message-body done-fn))
          (catch Throwable error
            (log/error error "Handler function threw an error!")))
        (recur)))))

(defn handle-queue
  "Setup a loop that listens to a queue and processes all incoming messages.

  Arguments:
    sqs-ext-config - A map of the following optional keys used for accessing AWS services:
      access-key     - AWS access key ID
      secret-key     - AWS secret access key
      s3-endpoint    - AWS S3 endpoint (protocol://service-code.region-code.amazonaws.com)
      s3-bucket-name - AWS S3 bucket to use to store messages larger than 256kb (optional)
      sqs-endpoint   - AWS SQS endpoint (protocol://service-code.region-code.amazonaws.com)
      region         - AWS region

    handler-opts - A map for the queue handling settings:
      queue-url                 - A string containing the unique URL of the queue to handle (required)
      number-of-handler-threads - Number of how many threads to run for handling message receival
                                  (optional, default: 4)
      restart-limit             - If a potentially non-fatal error occurs while handling the queue,
                                  it will try this many times to restart in order to hopefully recover
                                  from the occured error (optional, default: 6)
      restart-delay-seconds     - If the loop restarts, restart will be delayed by this many seconds
                                  (optional, default: 10)
      auto-delete               - A boolean where
                                  true: will immediately delete the message
                                  false: will provide a `done-fn` key inside the returned messages and
                                         the message will be left untouched by this API
                                  (optional, defaults: true)

    handler-fn - A function to which new incoming messages will be passed to (required)

                 NOTE: If auto-delete above is set to false, a `done-fn` function will be passed as
                       second argument which should be called to delete the message when processing
                       has been finished.

  Returns:
    A stop function - Call this function to terminate the loop and receive a map of final
                      information about the finished handling process (hopefully handy for
                      debugging). That map includes:

                      {:iteration                  The total count of iterations the loop was executed
                       :restart-count 0            Number of times the loop was restarted
                       :started-at    (t/now)      A tick instant timestamp when the loop was started
                       :last-iteration-started-at  A tick instant timestamp when the loop began last
                       :stopped-at                 A tick instant timestamp when the loop was stopped
                       :last-loop-duration-in-seconds} Last loop's running time in seconds"
  [{:keys [access-key
           secret-key
           s3-endpoint
           s3-bucket-name
           sqs-endpoint
           region]
    :as   sqs-ext-config}
   {:keys [queue-url
           number-of-handler-threads
           restart-limit
           restart-delay-seconds
           auto-delete]
    :or   {number-of-handler-threads 4
           restart-limit             6
           restart-delay-seconds     10
           auto-delete               true}}
   handler-fn]
  (let [sqs-ext-client (sqs/sqs-ext-client sqs-ext-config)
        handler-chan (chan)
        stop-fn (receive/receive-loop sqs-ext-client
                                      queue-url
                                      handler-chan
                                      {:auto-delete           auto-delete
                                       :restart-limit         restart-limit
                                       :restart-delay-seconds restart-delay-seconds})]
    (log/infof (str "Handling queue '%s' with bucket [%s], "
                    "number-of-handler-threads [%d], "
                    "restart-limit [%d], "
                    "restart-delay-seconds [%d], "
                    "auto-delete [%s].")
               queue-url
               s3-bucket-name
               number-of-handler-threads
               restart-limit
               restart-delay-seconds
               auto-delete)
    (launch-handler-threads number-of-handler-threads
                            handler-chan
                            auto-delete
                            handler-fn)
    stop-fn))

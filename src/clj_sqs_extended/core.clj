(ns clj-sqs-extended.core
  (:require [clojure.core.async :refer [chan <!! thread]]
            [clojure.tools.logging :as log]

            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.aws.sqs :as sqs]))

(def config
  {:access-key     "default"
   :secret-key     "default"
   :s3-endpoint    "http://localhost:4566"
   :s3-bucket-name "example-bucket"
   :sqs-endpoint   "http://localhost:4566"
   :region         "us-east-2"})

;; Conveniance declarations
(def sqs-ext-client    sqs/sqs-ext-client)
(def send-message      sqs/send-message)
(def send-fifo-message sqs/send-fifo-message)

(defn- launch-handler-threads
  [number-of-handler-threads handler-chan auto-delete handler-fn]
  (dotimes [_ number-of-handler-threads]
    (thread
      (loop []
        (when-let [{message-body :body
                    done-fn      :done-fn}
                   (<!! handler-chan)]
          (try
            (if auto-delete
              (handler-fn message-body)
              (handler-fn message-body done-fn))
            (catch Throwable error
              (log/error error (.getMessage error))))
          (recur))))))

(defn handle-queue
  "Setup a loop that listens to a queue and processes all incoming messages.

  Arguments:
    sqs-ext-client - A client object from calling (sqs-ext-client config)
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
  [sqs-ext-client
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
  (let [handler-chan (chan)
        stop-fn (receive/receive-loop
                  queue-url
                  handler-chan
                  (partial sqs/receive-messages sqs-ext-client queue-url)
                  (partial sqs/delete-message! sqs-ext-client queue-url)
                  {:auto-delete? auto-delete
                   :restart-opts {:restart-limit         restart-limit
                                  :restart-delay-seconds restart-delay-seconds}})]
    (log/infof (str "Handling queue '%s' with: "
                    "number-of-handler-threads [%d], "
                    "restart-limit [%d], "
                    "restart-delay-seconds [%d], "
                    "auto-delete [%s].")
               queue-url
               number-of-handler-threads
               restart-limit
               restart-delay-seconds
               auto-delete)
    (launch-handler-threads number-of-handler-threads
                            handler-chan
                            auto-delete
                            handler-fn)
    stop-fn))


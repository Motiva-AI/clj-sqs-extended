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
            (catch Throwable e
              (log/error e "Handler function threw an error!")))
          (recur))))))

(defn handle-queue
  [{:keys [access-key
           secret-key
           endpoint-url
           region]
    :or   {access-key   "default"
           secret-key   "default"
           endpoint-url "http://localhost:4566"
           region       "us-east-2"}
    :as   aws-creds}
   {:keys [queue-name
           s3-bucket-name
           num-handler-threads
           restart-limit
           auto-delete
           format]
    :or   {num-handler-threads 4
           restart-limit       10
           auto-delete         true
           format              :transit}}
   handler-fn]
  (let [sqs-ext-client (sqs/sqs-ext-client aws-creds s3-bucket-name)
        receive-chan (chan)
        stop-fn (receive/receive-loop sqs-ext-client
                                      queue-name
                                      receive-chan
                                      {:auto-delete   auto-delete
                                       :restart-limit restart-limit
                                       :format        format})]
    (log/infof (str "Now handling queue '%s' with:\n"
                    "  num-handler-threads: %d\n"
                    "  auto-delete: %s\n"
                    "  format: %s")
               queue-name num-handler-threads auto-delete format)
    (launch-handler-threads num-handler-threads
                            receive-chan
                            auto-delete
                            handler-fn)
    stop-fn))

(ns clj-sqs-extended.core
  (:require [clojure.core.async :refer [chan <! >! <!! thread]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.aws :as aws]
            [clj-sqs-extended.internal :as internal]
            [clj-sqs-extended.sqs :as sqs]))


;; Conveniance declarations
(def sqs-ext-client sqs/sqs-ext-client)

(def create-standard-queue sqs/create-standard-queue)
(def create-fifo-queue sqs/create-fifo-queue)

(def send-message sqs/send-message)
(def send-fifo-message sqs/send-fifo-message)


(defn handle-queue
  ([{:keys [access-key
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
            auto-delete
            format]
     :or   {num-handler-threads 4
            auto-delete         true
            format              :transit}}
    handler-fn]
   (let [endpoint (aws/configure-endpoint aws-creds)
         creds (aws/configure-credentials aws-creds)
         sqs-ext-client (sqs/sqs-ext-client s3-bucket-name
                                            endpoint
                                            creds)
         receive-chan (chan)
         stop-fn (internal/receive-loop sqs-ext-client
                                        queue-name
                                        receive-chan
                                        {:auto-delete auto-delete
                                         :format      format})]
     (log/infof (str "Starting receive loop for queue '%s' with:\n"
                     "  num-handler-threads: %d\n"
                     "  auto-delete: %s")
                queue-name num-handler-threads auto-delete)
     (dotimes [_ num-handler-threads]
       (thread
         (loop []
           (when-let [message (<!! receive-chan)]
             (try
               (if auto-delete
                 (handler-fn message)
                 (handler-fn message (:done-fn message)))
               (catch Throwable t
                 (log/error t "SQS handler function threw an error.")))
             (recur)))))
     stop-fn)))

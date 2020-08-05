(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-utils]
            [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import (java.util.concurrent CountDownLatch)))


; TODO: This needs to moved somewhere (handle-queue?!) else!
(defonce sqs-ext-client (atom nil))
(defonce queue-url (atom nil))

(defn wrap-sqs-ext-client
  [f]
  (let [bucket-name (helpers/random-bucket-name)
        localstack-endpoint (helpers/configure-endpoint
                              "http://localhost:4566"
                              "us-east-2")
        localstack-creds (helpers/configure-credentials
                           "localstack"
                           "localstack")
        s3-client (s3/s3-client localstack-endpoint
                                localstack-creds)
        bucket (s3/create-bucket s3-client
                                 bucket-name)]
    (reset! sqs-ext-client (sqs-ext/sqs-ext-client bucket
                                                   localstack-endpoint
                                                   localstack-creds))
    (let [queue (sqs-ext/create-standard-queue
                  @sqs-ext-client
                  (helpers/random-queue-name "queue-" ".standard"))]
      (reset! queue-url (.getQueueUrl queue))
      (f))))

(defmacro with-sqs-ext-client [& body]
  `(wrap-sqs-ext-client (fn [] ~@body)))

(defn dispatch-action-service
  ([message]
   (log/infof "I got %s which was auto-deleted." (:body message)))
  ([message done-fn]
   (done-fn)
   (log/infof "I got %s which I just deleted myself." (:body message))))

(defn start-action-service-queue-listener
  []
  (sqs-utils/handle-queue
    @sqs-ext-client
    @queue-url
    dispatch-action-service))

(defn start-queue-listeners
  []
  (let [stop-fns [(start-action-service-queue-listener)]]
    (fn []
      (doseq [f stop-fns]
        (f)))))

(defn start-worker
  []
  (let [sigterm (CountDownLatch. 1)]
    (log/info "Starting queue workers...")
    (let [stop-listeners (start-queue-listeners)]
      ;; (Thread/sleep 5000)
      ;; (stop-listeners)
      (.await sigterm))))

(with-sqs-ext-client
  (future (start-worker))
  (dotimes [_ 10]
    (log/infof "Message with ID '%s' sent."
               (sqs-ext/send-message @sqs-ext-client
                                     @queue-url
                                     (helpers/random-message)))))
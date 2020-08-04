(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-utils]
            [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import (java.util.concurrent CountDownLatch)))


(defn- create-client
  []
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
                                 bucket-name)
        sqs-client (sqs-ext/sqs-ext-client bucket
                                           localstack-endpoint
                                           localstack-creds)
        queue (sqs-ext/create-standard-queue sqs-client
                                             (helpers/random-queue-name "queue-" ".standard"))]
    [sqs-client (.getQueueUrl queue)]))

(def the-client (create-client))

(defn dispatch-action-service
  ([message]
   (log/infof "I got %s" (get-in message [:body :foo])))
  ([message done-fn]
   (log/infof "I got %s" (get-in message [:body :foo]))
   (done-fn)))

(defn start-action-service-queue-listener
  []
  (sqs-utils/handle-queue
    (first the-client)
    (last the-client)
    dispatch-action-service))

(defn start-queue-listeners
  []
  (let [start-fns [(start-action-service-queue-listener)]]
    (fn []
      (doseq [f start-fns]
        (f)))))

(defn start-worker
  []
  (let [sigterm (CountDownLatch. 1)]
    (log/info "Starting queue workers...")
    (start-queue-listeners)
    (.await sigterm)))

(future (start-worker))

(comment
  (sqs-ext/send-message (first the-client) (last the-client) {:foo "potatoes"}))
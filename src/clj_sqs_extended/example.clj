(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.sqs :as sqs]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.utils :as sqs-utils])
  (:import (java.util.concurrent CountDownLatch)))


(def ^:private bucket-name "sqs-ext-bucket")
(def ^:private queue-name "sqs-ext-queue")

(defonce ^:private queue-url (atom nil))

(def ^:private sqs-ext-client
  (sqs/sqs-ext-client bucket-name
                      (sqs-utils/configure-endpoint)
                      (sqs-utils/configure-credentials)))

(defn- dispatch-action-service
  ([message]
   (log/infof "I got %s which was auto-deleted." (:body message)))
  ([message done-fn]
   (done-fn)
   (log/infof "I got %s which I just deleted myself." (:body message))))

(defn- start-action-service-queue-listener
  []
  (sqs-ext/handle-queue
    @queue-url
    dispatch-action-service
    {:bucket-name bucket-name}))

(defn- start-queue-listeners
  []
  (let [stop-fns [(start-action-service-queue-listener)]]
    (fn []
      (doseq [f stop-fns]
        (f)))))

(defn- start-worker
  []
  (let [sigterm (CountDownLatch. 1)]
    (log/info "Starting queue workers...")
    (let [stop-listeners (start-queue-listeners)]
      (.await sigterm)
      (stop-listeners))))

(defn- update-queue-url
  "Run this once before the example in case the queue is already present."
  [sqs-client]
  (reset! queue-url (sqs/get-queue-url sqs-client queue-name)))

(defn- create-example-queue
  "If the queue has not yet been setup outside of this example,
   run this once to create it."
  []
  (sqs/create-standard-queue sqs-ext-client queue-name))

(defn- create-example-bucket
  "If the bucket has not yet been setup outside of this example,
   run this once to create it."
  []
  (s3/create-bucket (s3/s3-client) bucket-name))

(defn- run-example
  []
  (future (start-worker))
  (log/infof "Message with ID '%s' sent."
             (sqs/send-message sqs-ext-client
                               @queue-url
                               {:foo "potatoes"})))

(comment
  (create-example-bucket)
  (create-example-queue)
  (update-queue-url sqs-ext-client)
  (run-example)
  )
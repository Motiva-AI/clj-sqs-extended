(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.aws :as aws]
            [clj-sqs-extended.sqs :as sqs]
            [clj-sqs-extended.s3 :as s3])
  (:import (java.util.concurrent CountDownLatch)))


(def ^:private bucket-name "sqs-ext-bucket")
(def ^:private queue-name "sqs-ext-queue")

(def ^:private sqs-ext-client
  (sqs/sqs-ext-client bucket-name
                      (aws/configure-endpoint)
                      (aws/configure-credentials)))

(defn- dispatch-action-service
  ([message]
   (log/infof "I got %s which was auto-deleted." (:body message)))
  ([message done-fn]
   (done-fn)
   (log/infof "I got %s which I just deleted myself." (:body message))))

(defn- start-action-service-queue-listener
  []
  (sqs-ext/handle-queue
    queue-name
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

(defn- run-example
  []
  (s3/create-bucket (s3/s3-client) bucket-name)
  (sqs/create-standard-queue sqs-ext-client queue-name)
  (future (start-worker))
  (log/infof "Message with ID '%s' sent."
             (sqs/send-message sqs-ext-client
                               queue-name
                               {:foo "potatoes"})))

(comment
  (run-example))
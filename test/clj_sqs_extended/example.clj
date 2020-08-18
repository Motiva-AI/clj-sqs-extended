(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.s3 :as s3]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import (java.util.concurrent CountDownLatch)))


(def ^:private aws-config {:access-key   "default"
                           :secret-key   "default"
                           :endpoint-url "http://localhost:4566"
                           :region       "us-east-2"})

(def ^:private queue-config {:queue-name          "example-queue"
                             :s3-bucket-name      "example-bucket"
                             :num-handler-threads 1
                             :auto-delete         true})

(defn- dispatch-action-service
  ([message]
   (log/infof "I got %s." (:body message)))
  ([message done-fn]
   (done-fn)
   (log/infof "I got %s." (:body message))))

(defn- start-action-service-queue-listener
  []
  (sqs-ext/handle-queue aws-config
                        queue-config
                        dispatch-action-service))

(defn- start-queue-listeners
  []
  (let [stop-fns [(start-action-service-queue-listener)]]
    (fn []
      (doseq [f stop-fns]
        (f)))))

(defn- start-worker
  []
  (let [sigterm (CountDownLatch. 1)]
    (log/info "Starting queue workers ...")
    (let [stop-listeners (start-queue-listeners)]
      (.await sigterm)
      (stop-listeners))))

(defn- run-example
  []
  (let [sqs-ext-client (sqs-ext/sqs-ext-client aws-config
                                               (:s3-bucket-name queue-config))
        s3-client (s3/s3-client aws-config)]
    (s3/create-bucket s3-client
                      (:s3-bucket-name queue-config))
    (sqs-ext/create-standard-queue sqs-ext-client
                                   (:queue-name queue-config))
    (future (start-worker))
    (let [message (helpers/random-message-larger-than-256kb)]
      (log/infof "I sent %s with ID '%s'."
                 message
                 (sqs-ext/send-message sqs-ext-client
                                       (:queue-name queue-config)
                                       message)))
    (s3/purge-bucket s3-client
                     (:s3-bucket-name queue-config))))

(comment
  (run-example))


(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.aws :as aws]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.s3 :as s3])
  (:import (java.util.concurrent CountDownLatch)))


(def ^:private aws-config {:access-key   "default"
                           :secret-key   "default"
                           :endpoint-url "http://localhost:4566"
                           :region       "us-east-2"})

(def ^:private queue-config {:queue-name          "sqs-ext-queue"
                             :s3-bucket-name      "sqs-ext-bucket"
                             :num-handler-threads 1
                             :auto-delete         true})

(def ^:private sqs-ext-client
  (sqs-ext/sqs-ext-client (:s3-bucket-name queue-config)
                          (aws/configure-endpoint aws-config)
                          (aws/configure-credentials aws-config)))

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
    (log/info "Starting queue workers...")
    (let [stop-listeners (start-queue-listeners)]
      (.await sigterm)
      (stop-listeners))))

(defn- run-example
  []
  (s3/create-bucket (s3/s3-client)
                    (:s3-bucket-name queue-config))
  (sqs-ext/create-standard-queue sqs-ext-client
                                 (:queue-name queue-config))
  (future (start-worker))
  (let [message {:foo "potatoes"}]
    (log/infof "I sent %s with ID '%s'."
               message
               (sqs-ext/send-message sqs-ext-client
                                     (:queue-name queue-config)
                                     message))))

(comment
  (run-example))

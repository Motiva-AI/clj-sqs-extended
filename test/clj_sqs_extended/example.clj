(ns clj-sqs-extended.example
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import (java.util.concurrent CountDownLatch)))


(def ^:private aws-config {:aws-access-key-id     "your-access-key"
                           :aws-secret-access-key "your-secret-key"
                           :aws-s3-endpoint-url   "https://s3.us-east-2.amazonaws.com"
                           :aws-sqs-endpoint-url  "https://sqs.us-east-2.amazonaws.com"
                           :aws-region            "us-east-2"})

(def ^:private queue-config {:queue-name          "some-unique-queue-name-to-use"
                             :s3-bucket-name      "some-unique-bucket-name-to-use"
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
                                               (:s3-bucket-name queue-config))]

    ;; Start processing all received messages ...
    (future (start-worker))

    ;; Send a large test message that requires S3 usage to store ...
    (let [message (helpers/random-message-larger-than-256kb)]
      (log/infof "Sent message with ID '%s'."
                 (sqs-ext/send-message sqs-ext-client
                                       (:queue-name queue-config)
                                       message)))))

(comment
  (run-example))


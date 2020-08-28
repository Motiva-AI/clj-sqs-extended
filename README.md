# clj-sqs-extended

[![CircleCI](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master.svg?style=svg)](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master) [![Clojars Project](https://img.shields.io/clojars/v/motiva/clj-sqs-extended.svg)](https://clojars.org/motiva/clj-sqs-extended)

This library is a clojure wrapper for the [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib).

## Example

Spin up some services via Docker on your localhost,

```
$ make devel
```


```clj
(require '[clojure.tools.logging :as log]
         '[clj-sqs-extended.aws.s3 :as s3]
         '[clj-sqs-extended.core :as sqs-ext])

(import '[java.util.concurrent CountDownLatch])


(def aws-config
 {:access-key   "your-access-key"
  :secret-key   "your-secret-key"
  :s3-endpoint  "https://localhost:4566"
  :sqs-endpoint "https://localhost:4566"
  :region       "us-east-2"})

(def queue-config
 {:queue-name                "name-of-existing-queue-to-use"
  :s3-bucket-name            "name-of-existing-bucket-to-use"
  :number-of-handler-threads 1})

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 32 (rand 94))))
       (take length)
       (apply str)))

(defn random-message-larger-than-256kb
  []
  {:id      (rand-int 65535)
   :payload (random-string-with-length 300000)})

(defn dispatch-action-service
  ([message]
   (log/infof "I got '%s'..."
              (subs (get-in message [:body :payload]) 0 32)))
  ([message done-fn]
   (log/infof "I got '%s'..."
              (subs (get-in message [:body :payload]) 0 32))
   (done-fn)))

(defn start-action-service-queue-listener
  []
  (sqs-ext/handle-queue aws-config
                        queue-config
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
    (log/info "Starting queue workers ...")
    (let [stop-listeners (start-queue-listeners)]
      (.await sigterm)
      (stop-listeners))))

(defn run-example
  []
  (let [s3-client (s3/s3-client aws-config)]
    (s3/create-bucket s3-client (:s3-bucket-name
                                 queue-config))
    (let [sqs-ext-client (sqs-ext/sqs-ext-client aws-config
                                                 (:s3-bucket-name queue-config))]
      (sqs-ext/create-standard-queue sqs-ext-client
                                     (:queue-name queue-config))

      ;; Start processing all received messages ...
      (future (start-worker))

      ;; Send a large test message that requires S3 usage to store ...
      (let [message (random-message-larger-than-256kb)]
        (log/infof "Sent message with ID '%s'."
                   (sqs-ext/send-message sqs-ext-client
                                         (:queue-name queue-config)
                                         message))))))
```

## Development

### Requirements

- [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)

### Testing

Tests are run inside a CircleCI Docker container on localhost.

```
$ make test
```

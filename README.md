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
 '[clj-sqs-extended.core :as sqs-ext]
 '[clj-sqs-extended.test-helpers :as helpers])

(import '[java.util.concurrent CountDownLatch])


(def aws-config
 {:aws-access-key-id     "your-access-key"
  :aws-secret-access-key "your-secret-key"
  :aws-s3-endpoint-url   "https://localhost:4566"
  :aws-sqs-endpoint-url  "https://localhost:4566"
  :aws-region            "us-east-2"})

(def queue-config
 {:queue-name                "name-of-existing-queue-to-use"
  :s3-bucket-name            "name-of-existing-bucket-to-use"
  :number-of-handler-threads 1})

(defn dispatch-action-service
  ([message]
   (log/infof "I got %s." (:body message)))
  ([message done-fn]
   (log/infof "I got %s." (:body message))
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
```

## Development

### Requirements

- [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)

### Testing

Tests are run inside a CircleCI Docker container on localhost.

```
$ make test
```

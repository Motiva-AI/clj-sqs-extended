# clj-sqs-extended

[![CircleCI](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master.svg?style=svg)](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master)

```clojure
[motiva/clj-sqs-extended "0.1.0-SNAPSHOT"]
```

<br/>

This is a clojure wrapper for the [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib).

<br/>
  
## [Example](https://github.com/Motiva-AI/clj-sqs-extended/blob/master/test/clj_sqs_extended/example.clj)

In this example we define a very simple handling function ```dispatch-action-service``` for new messages
which just prints out their content. Using ```handle-queue``` with our AWS and queue
settings we will receive incoming messages from our queue directly at our handling
function.

Worker threads get started in the background and will continue running until
CTRL-C terminated. Upon termination we stop the process loop via the ```stop-fn```
handle which we received back from ```handle-queue``` when we initiated the
handling.

As a proof of concept, we sent a single large message that gets printed out by
our handler.
 
```clj
(ns clj-sqs-extended.example
  (:require [clojure.tools.logging :as log]
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
                             :num-handler-threads 1})

(defn- dispatch-action-service
  ([message]
   (log/infof "I got %s." (:body message)))
  ([message done-fn]
   (log/infof "I got %s." (:body message))
   (done-fn)))

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
```

<br/>

## Usage

The core API provides ```handle-queue``` to setup a loop that listens to a queue and
processes all incoming messages. It takes a map of optional keys used for accessing
the AWS services, a map for the configuration settings of the queue to handle and
a function to which new incoming messages will be passed. ```handle-queue``` returns
a stop function to terminate the loop and receive some final information about the
finished handling process (hopefully handy for debugging).

```create-standard-queue``` and ```create-fifo-queue``` are provided to setup
queues programmatically, although this will mostly be done directly via the
AWS management services.
 
```send-message``` and ```send-fifo-message``` can be used to send a message to
either a standard or a FIFO queue via the passed ```sqs-ext-client``` instance. 
Both functions only require the name of the queue and a message payload. The format
for serialisation (currently either :json or :transit) as well as a
deduplication-id can be optionally set in the options of those functions.

<br/>

## Development

### Requirements

- [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)

### Testing

You can spin up a localstack endpoint on your machine which will provide
mockup SQS and S3 services to be used while developing. Set the endpoints
inside the aws-creds options for handle-queue to: "https://localhost:4566"

```
$ make devel
```

Tests are run inside a CircleCI Docker container on localhost.

```
$ make test
```


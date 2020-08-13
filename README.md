# clj-sqs-extended

## API Highlight

Create a worker for consuming an SQS queue (Paul: I'm not sure about this API for handle-queue yet):

```
sqs-utils.core> (doc handle-queue)
-------------------------
sqs-utils.core/handle-queue
[aws-creds queue-config handler-fn]

  Set up a loop that listens to a queue and process incoming messages.

   Arguments:

   aws-config - A map of the following settings used for interacting with AWS SQS:
     access-key   - AWS access key ID
     secret-key   - AWS secret access key
     endpoint-url - AWS endpoint (usually an HTTPS based URL)
     region       - AWS region

   queue-config - A map of the configuration for this queue
     queue-name     - required: name of the queue
     s3-bucket-name - required: name of an existing S3 bucket)
     num-handler-threads - optional: how many threads to run (defaults: 4)
     auto-delete   - optional: boolean, if true, immediately delete the message,
                     if false, forward a `done` function and leave the
                     message intact. (defaults: true)

   handler-fn - a function which will be passed the incoming message. If
                auto-delete is false, a second argument will be passed a `done`
                function to call when finished processing.

  Returns:
  a kill function - call the function to terminate the loop.
=> nil
```

Send messages to a queue:

```
sqs-utils.core> (doc send-message)
-------------------------
sqs-utils.core/send-message
[sqs-client queue-name message]
  Send a message to a standard queue.
=> Message ID as String

sqs-utils.core> (doc send-fifo-message)
-------------------------
sqs-utils.core/send-fifo-message
[sqs-client queue-name message group-id
  {:keys [format
          deduplication-id]
   :or   {format :transit}}]
  Send a message to a FIFO queue.

  Argments:
  message-group-id - a tag that specifies the group that this message
  belongs to. Messages belonging to the same group are guaranteed FIFO.

  Options:
  format - format to serialize outgoing message with (:json :transit)
  deduplication-id - token used for deduplication of sent messages
=> nil
```

## Example


```clj
(defn dispatch-action-service
 [{foo :foo} done-fn]
 (println (format "I got %s" foo))
 (done-fn))

(defn start-action-service-queue-listener
 []
 (sqs-utils/handle-queue
  (queue/aws-creds)
  (queue/queue-config :action-service)
  dispatch-action-service))

(defn start-queue-listeners
  "Start all the queue listener loops. Returns a function which stops them all."
  []
  (let [stop-fns [(start-action-service-queue-listener)]]
    (fn []
      (doseq [stop-fn stop-fns]
        (stop-fn)))))

(defn start-worker
  "Starts queue workers and blocks indefinitely"
  []
  (let [sigterm (java.util.concurrent.CountDownLatch. 1)]
    (log/info "Starting queue workers...")
    (start-queue-listeners)

    ;; blocks main from exiting
    (.await sigterm)))

;; start this in the background
(future (start-worker))

(send-message (queue/aws-creds) (queue/queue-url :action-service) {:foo "potatoes"})
=> nil
"I got potatoes"
```

## Development

### Requirements

- [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)

### Testing

Tests are run inside a CircleCI Docker container on localhost.

```
$ make test
```


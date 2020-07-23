# clj-sqs-extended

## Running the tests

Tests can be run locally the same way as they will be run via CircleCI.
Please install the [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)
tool for your OS of choice and configure it according to the manual.

Then spin up the entire infrastructure and run the tests with:

```
$> circleci local execute --job build
```

## API Highlight

Create a worker for consuming an SQS queue (Paul: I'm not sure about this API for handle-queue yet):

```
sqs-utils.core> (doc handle-queue)
-------------------------
sqs-utils.core/handle-queue
[aws-creds queue-config handler-fn]

  Set up a loop that listens to a queue and process incoming messages.

   Arguments:

   aws-creds - A map of the following credential keys, used for interacting with SQS:
     access-key   - AWS access key ID
     secret-key   - AWS secret access key
     sqs-endpoint - SQS queue endpoint - usually an HTTPS based URL (Paul: this seems redundant to queue-url)
     region       - AWS region

   queue-config - A map of the configuration for this queue
     queue-url     - required: URL of the queue
     s3-bucket-arn - optional: ARN (or whatever URI needed to point to an existing S3, TBD what minimal setting we need to pass in here)
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
[aws-creds queue-url payload]
  Send a message to a standard queue.
=> nil (Paul: could we return the message ID? or whatever ID that the library returns for the message?)
sqs-utils.core> (doc send-fifo-message)
-------------------------
sqs-utils.core/send-fifo-message
[aws-creds queue-url payload {message-group-id :message-group-id, deduplication-id :deduplication-id, :as options}]
  Send a message to a FIFO queue.

  Argments:
  message-group-id - a tag that specifies the group that this message
  belongs to. Messages belonging to the same group
  are guaranteed FIFO

  Options:
  deduplication-id -  token used for deduplication of sent messages
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

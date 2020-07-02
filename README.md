# clj-sqs-extended

## Usage

Create a worker for consuming an SQS queue (Paul: I'm not sure about this API for handle-queue yet):

```
sqs-utils.core> (doc handle-queue)
-------------------------
sqs-utils.core/handle-queue
([sqs-config queue-url handler-fn {:keys [num-handler-threads auto-delete], :or {num-handler-threads 4, auto-delete true}, :as opts}]
 [sqs-config queue-url handler-fn])
  Set up a loop that listens to a queue and process incoming messages.

   Arguments:
   sqs-config  - A map of the following keys, used for interacting with SQS:
      access-key - AWS access key ID
      secret-key - AWS secret access key
      endpoint   - SQS queue endpoint - usually an HTTPS based URL
      region     - AWS region
      s3-bucket  - ARN (or whatever URI needed to point to an existing S3, TBD what minimal setting we need to pass in here)
   queue-url  - URL of the queue
   handler-fn - a function which will be passed the incoming message. If
                auto-delete is false, a second argument will be passed a `done`
                function to call when finished processing.
   opts       - an optional map containing the following keys:
      num-handler-threads - how many threads to run (defaults: 4)

      auto-delete        - boolean, if true, immediately delete the message,
                           if false, forward a `done` function and leave the
                           message intact. (defaults: true)

  Returns:
  a kill function - call the function to terminate the loop.
=> nil
```

Send messages to a queue:

```
sqs-utils.core> (doc send-message)
-------------------------
sqs-utils.core/send-message
[sqs-config queue-url payload]
  Send a message to a standard queue.
=> nil
sqs-utils.core> (doc send-fifo-message)
-------------------------
sqs-utils.core/send-fifo-message
[sqs-config queue-url payload {message-group-id :message-group-id, deduplication-id :deduplication-id, :as options}]
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


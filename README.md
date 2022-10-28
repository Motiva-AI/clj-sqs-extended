# clj-sqs-extended

[![CircleCI](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master.svg?style=svg)](https://circleci.com/gh/Motiva-AI/clj-sqs-extended/tree/master) [![Clojars Project](https://img.shields.io/clojars/v/motiva/clj-sqs-extended.svg)](https://clojars.org/motiva/clj-sqs-extended)

This library is a clojure wrapper for the [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib).

## Example

Spin up some services via Docker on your localhost to try the following:

```
$ make devel
```

```clj
(require '[clj-sqs-extended.core :as sqs-ext]
         '[clj-sqs-extended.aws.sqs :as sqs])

(def config
  {:access-key     "default"
   :secret-key     "default"
   :s3-endpoint    "http://localhost:4566"
   :s3-bucket-name "example-bucket"
   :sqs-endpoint   "http://localhost:4566"
   :region         "us-west-2"})
(def client (sqs-ext/sqs-ext-client config))
(def queue-url (sqs/create-standard-queue! client "my-test-queue"))
; "http://localstack:4566/000000000000/my-test-queue"

(sqs-ext/send-message client queue-url "hello world!")
; "61b077d1-63f9-cd52-b2eb-1078d66ed1a4"

(sqs/receive-messages client queue-url)
; ({:body "hello world!"
;   :format :transit
;   :messageId "61b077d1-63f9-cd52-b2eb-1078d66ed1a4"
;   :receiptHandle "myztiahpogtvdzmjnjqdgezxppupfabbwqckgymjsqyxsclfbajceqrmeuheuqcyuupppmqtryibpkuuoedhrqicnqtkcrsajycnlorutgtgzwykwoqkbkocrpmwedcnafhqetnejdfkwwkcmkrohkldahtzpiavhvyohpccrgssklvoyosricawi"})

;; remove the message from the queue
(->> (sqs/receive-messages client queue-url)
     (first)
     (sqs/delete-message! client queue-url))

;; what about sending a message beyond the SQS size limit of 256kb?

(require 'clj-sqs-extended.test-helpers)
(def a-large-message (clj-sqs-extended.test-helpers/random-message-larger-than-256kb))

(sqs-ext/send-message client queue-url a-large-message)
; com.amazonaws.services.s3.model.AmazonS3Exception

(require '[clj-sqs-extended.aws.s3 :as s3])

(s3/create-bucket! (s3/s3-client config) (:s3-bucket-name config))
; "example-bucket"

(sqs-ext/send-message client queue-url a-large-message)
; 8a0c6d21-f98f-5464-0c8d-91da53ed04a8

; (sqs/receive-messages client queue-url)
; this will print a-large-message and flood your screen...

```


## Development

### Requirements

- [circleci-cli-tool](https://circleci.com/docs/2.0/local-cli/)

### Testing

Tests are run inside a CircleCI Docker container on localhost.

```
$ make test
```

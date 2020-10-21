(ns clj-sqs-extended.integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [environ.core :refer [env]]
            [clojure.core.async :refer [chan <!! >!!]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.test-helpers :as helpers]))

(defn sqs-ext-config []
  {:access-key     (env :integration-access-key)
   :secret-key     (env :integration-secret-key)
   :sqs-endpoint   "https://sqs.us-west-2.amazonaws.com"
   :s3-endpoint    "https://s3.us-west-2.amazonaws.com"
   :s3-bucket-name (env :integration-test-s3-bucket-name)
   :region         "us-west-2"})

(def standard-queue-url (env :integration-test-standard-queue-url))

(defn wrap-purge-integration-queues
  [f]
  ;; From AWS: Only one PurgeQueue operation is allowed every 60 seconds
  (sqs-ext/purge-queue! (sqs-ext-config) standard-queue-url)
  (f))

(use-fixtures :once wrap-purge-integration-queues)

(defn roundtrip-message-test [create-message-fn]
  (let [c          (chan)
        handler-fn (fn [& args] (>!! c args))

        msg1 (create-message-fn)
        msg2 (create-message-fn)]

    (is (sqs-ext/send-message (sqs-ext-config) standard-queue-url msg1 {:format :transit}))

    (let [stop-fn (sqs-ext/handle-queue
                    (sqs-ext-config)
                    {:queue-url standard-queue-url
                     :auto-delete true}
                    handler-fn)]
      (is (= [msg1] (<!! c)))

      (is (sqs-ext/send-message (sqs-ext-config) standard-queue-url msg2 {:format :json}))
      (is (= [msg2] (<!! c)))

      (stop-fn))))

(deftest ^:integration roundtrip-standard-queue-test
  (testing "Small message, pure SQS"
    (roundtrip-message-test helpers/random-message-basic))

  (testing "Large 256kb+ message, S3-backed SQS"
    (roundtrip-message-test helpers/random-message-larger-than-256kb)))


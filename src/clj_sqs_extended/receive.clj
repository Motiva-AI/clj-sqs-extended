(ns clj-sqs-extended.receive
  (:require [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.internal.serdes :as serdes]))

(defn- extract-relevant-keys-from-message
  [message]
  (some-> message
          (bean)
          (select-keys [:messageId :receiptHandle :body])))

(defn- get-serdes-format-attribute
  [format-attribute-name message]
  (some-> message
          (.getMessageAttributes)
          (get format-attribute-name)
          (.getStringValue)
          (keyword)))

(defn- parse-message
  [format-attribute-name raw-message]
  (let [coll   (extract-relevant-keys-from-message raw-message)
        format (get-serdes-format-attribute raw-message format-attribute-name)]
    (if (seq coll)
      (assoc coll :format format)
      coll)))

(defn- deserialize-message-if-formatted
  [{message-body   :body
    message-format :format
    :as message}]
  (if message-format
    (->> (serdes/deserialize message-body message-format)
         (assoc message :body))

    message))

(defn receive-messages
  [sqs-client queue-url
   {:keys [wait-time-in-seconds
           format-attribute-name]
    ;; Defaults to maximum long polling
    ;; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    :or   {wait-time-in-seconds 20
           format-attribute-name sqs/clj-sqs-ext-format-attribute}}]
  (->> (sqs/wait-and-receive-messages-from-sqs
         sqs-client
         queue-url
         wait-time-in-seconds)
       (map (partial parse-message format-attribute-name))
       (map deserialize-message-if-formatted)))


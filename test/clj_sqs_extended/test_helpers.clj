(ns clj-sqs-extended.test-helpers
  (:require [tick.core :as t])
  (:import [java.util UUID]))

(defn random-bucket-name
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn random-queue-name
  ([]
   (random-queue-name {}))
  ([{:keys [prefix
            suffix]}]
   (str prefix
        (UUID/randomUUID)
        suffix)))

(defn random-group-id
  []
  (str (UUID/randomUUID)))

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 32 (rand 94))))
       (take length)
       (apply str)))

(defn random-message-basic
  []
  {:id      (rand-int 65535)
   :payload (random-string-with-length 512)})

(defn random-message-with-time
  []
  {:id        (rand-int 65535)
   :payload   (random-string-with-length 512)
   :timestamp (t/inst)})

(defn random-message-larger-than-256kb
  []
  {:id      (rand-int 65535)
   :payload (random-string-with-length 300000)})

(defn get-total-message-amount-in-queue
  [sqs-client queue-url]
  (->> ["ApproximateNumberOfMessages"
        "ApproximateNumberOfMessagesNotVisible"
        "ApproximateNumberOfMessagesDelayed"]
       (.getQueueAttributes sqs-client queue-url)
       (.getAttributes)
       (vals)
       (map read-string)
       (reduce +)))

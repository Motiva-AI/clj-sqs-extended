(ns clj-sqs-extended.test-helpers
  (:require [clj-sqs-extended.sqs :as sqs]
            [tick.alpha.api :as t])
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
  (->> (repeatedly #(char (+ 40 (rand 86))))
       (take length)
       (apply str)))

(defn random-message-basic
  []
  {:id (rand-int 65535)
   :payload (random-string-with-length 512)})

(defn random-message-with-time
  []
  {:id (rand-int 65535)
   :payload (random-string-with-length 512)
   :timestamp (t/inst)})

(defn random-message-larger-than-256kb
  []
  {:id (rand-int 65535)
   :payload (random-string-with-length 300000)})

(defn get-total-message-amount-in-queue
  [sqs-client name]
  (let [url (sqs/queue-name-to-url sqs-client name)]
    (->> ["ApproximateNumberOfMessages"
          "ApproximateNumberOfMessagesNotVisible"
          "ApproximateNumberOfMessagesDelayed"]
         (.getQueueAttributes sqs-client url)
         (.getAttributes)
         (vals)
         (map read-string)
         (reduce +))))


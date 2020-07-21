(ns clj-sqs-extended.tools
  "Provides the core functionalities of the wrapped library."
  (:require [clojure.tools.logging :as log]
            [tick.alpha.api :as t])
  (:import [java.util UUID]))


(defn random-bucket-name
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn random-queue-name
  []
  (str "queue-"
       (UUID/randomUUID)))

(defn random-string-with-length
  [length]
  (->> (repeatedly #(char (+ 40 (rand 86))))
       (take length)
       (apply str)))

(defn peak-message
  "Dumps some information about the passed SQS message on the screen for verification."
  [message]
  (log/infof
    "ID: %s\nReceipt handle: %s\nMessage body (first 5 characters): %s"
    (.getMessageId message)
    (.getReceiptHandle message)
    (subs (.getBody message) 0 5)))

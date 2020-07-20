(ns clj-sqs-extended.tools
  "Provides the core functionalities of the wrapped library."
  (:require [clojure.tools.logging :as log]
            [tick.alpha.api :as t])
  (:import [java.util UUID]))


(defn random-bucket-name
  "Creates a random name for the test bucket."
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn random-queue-name
  "Creates a random name for a dummy queue."
  []
  (str "queue-"
       (UUID/randomUUID)))

(defn peak-message
  "Dumps some information about the passed SQS message on the screen for verification."
  [message]
  (log/infof
    "ID: %s\nReceipt handle: %s\nMessage body (first 5 characters): %s"
    (.getMessageId message)
    (.getReceiptHandle message)
    (subs (.getBody message) 0 5)))

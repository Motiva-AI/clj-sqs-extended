(ns clj-sqs-extended.tools
  "Provides the core functionalities of the wrapped library."
  (:require [tick.alpha.api :as t])
  (:import [java.util UUID]
           [com.amazonaws.services.s3.model
            BucketLifecycleConfiguration
            BucketLifecycleConfiguration$Rule]))


(defn random-bucket-name
  []
  (str (UUID/randomUUID)
       "-"
       (t/format (t/formatter "yyMMdd-hhmmss") (t/date-time))))

(defn random-queue-name
  []
  (str "queue-"
       (UUID/randomUUID)))

(defn configure-bucket-lifecycle
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

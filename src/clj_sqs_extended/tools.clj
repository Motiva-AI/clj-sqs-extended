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
  "Creates a bucket lifecylce configuration with the passed status and expiration in days."
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

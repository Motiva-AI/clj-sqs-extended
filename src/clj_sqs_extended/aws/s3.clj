(ns clj-sqs-extended.aws.s3
  (:require [clj-sqs-extended.aws.configuration :as aws])
  (:import [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model
            BucketLifecycleConfiguration
            BucketLifecycleConfiguration$Rule
            ListVersionsRequest]))


(defn s3-client
  [aws-config]
  (let [endpoint (aws/configure-s3-endpoint aws-config)
        creds (aws/configure-credentials aws-config)
        builder (-> (AmazonS3ClientBuilder/standard)
                    (.withPathStyleAccessEnabled true))
        builder (if endpoint (.withEndpointConfiguration builder endpoint) builder)
        builder (if creds (.withCredentials builder creds) builder)]
    (.build builder)))

(defn configure-bucket-lifecycle
  [status expiration-days]
  (let [expiration (-> (BucketLifecycleConfiguration$Rule.)
                       (.withStatus status)
                       (.withExpirationInDays expiration-days))]
    (.withRules (BucketLifecycleConfiguration.) [expiration])))

(defn create-bucket
  ([s3-client name]
   (create-bucket s3-client name (configure-bucket-lifecycle "Enabled" 14)))

  ([s3-client name lifecycle]
   (doto s3-client
     (.createBucket name)
     (.setBucketLifecycleConfiguration name lifecycle))
   name))

(defn purge-bucket
  [s3-client bucket-name]
  (letfn [(delete-objects [objects]
            (doseq [o objects]
              (let [key (.getKey o)]
                (.deleteObject s3-client bucket-name key))))
          (delete-object-versions [versions]
            (doseq [v versions]
              (let [key (.getKey v)
                    id (.getVersionId v)]
                (.deleteVersion s3-client bucket-name key id))))]
    (loop [objects (.listObjects s3-client bucket-name)]
      (delete-objects (.getObjectSummaries objects))
      (when (.isTruncated objects)
        (recur (.listNextBatchOfObjects objects))))
    (let [version-request (-> (ListVersionsRequest.) (.withBucketName bucket-name))
          versions (->> (.listVersions s3-client version-request) (.getVersionSummaries))]
      (delete-object-versions versions))
    (.deleteBucket s3-client bucket-name)))

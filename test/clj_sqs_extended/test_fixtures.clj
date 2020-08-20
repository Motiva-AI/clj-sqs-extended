(ns clj-sqs-extended.test-fixtures
  (:require [clojure.core.async :refer [chan close! >!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]))


(def aws-config {:access-key   (env :aws-access-key-id)
                 :secret-key   (env :aws-secret-access-key)
                 :endpoint-url (env :aws-sqs-endpoint-url)
                 :region       (env :aws-sqs-region)})

(defonce test-sqs-ext-client (atom nil))
(defonce test-standard-queue-name (helpers/random-queue-name))
(defonce test-fifo-queue-name (helpers/random-queue-name {:suffix ".fifo"}))
(defonce test-bucket-name (helpers/random-bucket-name))

(defn wrap-standard-queue
  [f]
  (sqs/create-standard-queue @test-sqs-ext-client
                             test-standard-queue-name)
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs/delete-queue @test-sqs-ext-client
                    test-standard-queue-name))

(defmacro with-test-standard-queue
  [& body]
  `(wrap-standard-queue (fn [] ~@body)))

(defn wrap-fifo-queue
  [f]
  (sqs/create-fifo-queue @test-sqs-ext-client
                         test-fifo-queue-name)
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs/delete-queue @test-sqs-ext-client
                    test-fifo-queue-name))

(defmacro with-test-fifo-queue
  [& body]
  `(wrap-fifo-queue (fn [] ~@body)))

(defn with-test-sqs-ext-client
  [f]
  (let [s3-client (s3/s3-client aws-config)]
    (s3/create-bucket s3-client test-bucket-name)
    (reset! test-sqs-ext-client (sqs/sqs-ext-client aws-config
                                                    test-bucket-name))
    (f)
    (s3/purge-bucket s3-client test-bucket-name)))

(defn wrap-handle-queue-standard
  [handler-chan aws-opts queue-opts f]
  (let [queue-config (merge {:queue-name     test-standard-queue-name
                             :s3-bucket-name test-bucket-name}
                            queue-opts)
        handler-fn (fn ([message]
                        (>!! handler-chan message))
                       ([message done-fn]
                        (>!! handler-chan message)
                        (done-fn)))
        stop-fn (sqs-ext/handle-queue (merge aws-config aws-opts)
                                      queue-config
                                      handler-fn)]
    (f)
    (stop-fn)))

(defmacro with-handle-queue-standard
  [handler-chan aws-opts queue-opts & body]
  `(wrap-handle-queue-standard ~handler-chan
                               ~aws-opts
                               ~queue-opts
                               (fn [] ~@body)))

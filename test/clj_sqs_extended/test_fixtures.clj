(ns clj-sqs-extended.test-fixtures
  (:require [clojure.core.async :refer [>!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]))


(def aws-config {:access-key     (env :access-key)
                 :secret-key     (env :secret-key)
                 :s3-endpoint    (env :s3-endpoint)
                 :s3-bucket-name (helpers/random-bucket-name)
                 :sqs-endpoint   (env :sqs-endpoint)
                 :region         (env :region)})

(defonce test-sqs-ext-client (atom nil))
(defonce test-queue-url (atom nil))
(defonce test-standard-queue-name (helpers/random-queue-name))
(defonce test-fifo-queue-name (helpers/random-queue-name {:suffix ".fifo"}))

(defn with-test-sqs-ext-client
  [f]
  (let [s3-client (s3/s3-client aws-config)]
    (s3/create-bucket! s3-client (:s3-bucket-name aws-config))
    (reset! test-sqs-ext-client (sqs/sqs-ext-client aws-config))
    (f)
    (s3/purge-bucket! s3-client (:s3-bucket-name aws-config))))

(defn with-test-sqs-ext-client-no-s3
  [f]
  (reset! test-sqs-ext-client (sqs/sqs-ext-client aws-config))
  (f))

(defn wrap-standard-queue
  [f]
  (reset! test-queue-url
          (sqs-ext/create-standard-queue! aws-config
                                          test-standard-queue-name))
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs-ext/delete-queue! aws-config
                         @test-queue-url))

(defmacro with-test-standard-queue
  [& body]
  `(wrap-standard-queue (fn [] ~@body)))

(defn wrap-fifo-queue
  [f]
  (reset! test-queue-url
          (sqs-ext/create-fifo-queue! aws-config
                                      test-fifo-queue-name))
  (f)
  ;; TODO: https://github.com/Motiva-AI/clj-sqs-extended/issues/27
  (Thread/sleep 500)
  (sqs-ext/delete-queue! aws-config
                         @test-queue-url))

(defmacro with-test-fifo-queue
  [& body]
  `(wrap-fifo-queue (fn [] ~@body)))

(defn test-handler-fn
  ([chan message]
   (>!! chan message))
  ([chan message _]
   ;; WATCHOUT: Second parameter (done-fn) is not used here.
   (>!! chan message)))

(defn wrap-handle-queue
  [handler-chan settings f]
  (let [handler-config (merge {:queue-url @test-queue-url}
                              (:handler-opts settings))
        stop-fn (sqs-ext/handle-queue (merge aws-config (:aws-config settings))
                                      handler-config
                                      (partial test-handler-fn handler-chan))]
    (f)
    (if (:auto-stop-loop settings)
      (stop-fn)
      stop-fn)))

(defmacro with-handle-queue-defaults
  ([handler-chan & body]
   `(wrap-handle-queue ~handler-chan
                       {:auto-stop-loop true}
                       (fn [] ~@body))))

(defmacro with-handle-queue
  ([handler-chan settings & body]
   `(wrap-handle-queue ~handler-chan
                       (merge {:auto-stop-loop true} ~settings)
                       (fn [] ~@body))))

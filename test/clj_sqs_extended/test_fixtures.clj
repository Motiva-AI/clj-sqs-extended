(ns clj-sqs-extended.test-fixtures
  (:require [clojure.core.async :refer [>!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]))


(def sqs-ext-config {:access-key     (env :access-key)
                     :secret-key     (env :secret-key)
                     :s3-endpoint    (env :s3-endpoint)
                     :s3-bucket-name (helpers/random-bucket-name)
                     :sqs-endpoint   (env :sqs-endpoint)
                     :region         (env :region)})

(defonce test-sqs-ext-client (atom nil))
(defonce test-queue-url (atom nil))
(def test-standard-queue-name helpers/random-queue-name)
(def test-fifo-queue-name (partial helpers/random-queue-name {:suffix ".fifo"}))
(defonce test-handler-done-fn (atom nil))

(defn with-test-s3-bucket
  [f]
  (let [s3-client (s3/s3-client sqs-ext-config)]
    (s3/create-bucket! s3-client (:s3-bucket-name sqs-ext-config))
    (f)
    (s3/purge-bucket! s3-client (:s3-bucket-name sqs-ext-config))))

(defn with-test-sqs-ext-client
  [f]
  (reset! test-sqs-ext-client (sqs/sqs-ext-client sqs-ext-config))
  (f))

(defn with-test-standard-queue
  [f]
  (let [queue-url (sqs/create-standard-queue!
                    @test-sqs-ext-client
                    (test-standard-queue-name)
                    {:visibility-timeout-in-seconds 1})]
    (reset! test-queue-url queue-url)
    (f)
    (Thread/sleep 200) ;; wait for receive-loop to finish in the background
    (sqs/delete-queue! @test-sqs-ext-client queue-url)))

(defn wrap-fifo-queue
  [f]
  (let [queue-url (sqs/create-fifo-queue!
                    @test-sqs-ext-client
                    (test-fifo-queue-name))]
    (reset! test-queue-url queue-url)
    (f)
    (Thread/sleep 200) ;; wait for receive-loop to finish in the background
    (sqs/delete-queue! @test-sqs-ext-client queue-url)))

(defmacro with-test-fifo-queue
  [& body]
  `(wrap-fifo-queue (fn [] ~@body)))

(defn test-handler-fn
  ([chan message]
   (>!! chan message))
  ([chan message done-fn]
   (reset! test-handler-done-fn done-fn)
   (>!! chan message)))

(defn wrap-handle-queue
  [handler-chan settings f]
  (let [handler-config (merge {:queue-url @test-queue-url} (:handler-opts settings))
        stop-fn (sqs-ext/handle-queue
                  (sqs-ext/sqs-ext-client
                    (merge sqs-ext-config (:sqs-ext-config settings)))
                  handler-config
                  (partial test-handler-fn handler-chan))]
    (f)
    (if (:auto-stop-loop settings)
      (do
        (stop-fn)
        ;; wait for receive-loop async teardown
        (Thread/sleep 100))
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

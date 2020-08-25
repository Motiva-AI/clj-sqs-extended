(ns clj-sqs-extended.test-fixtures
  (:require [clojure.core.async :refer [chan close! >!!]]
            [environ.core :refer [env]]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.aws.s3 :as s3]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.test-helpers :as helpers]))


(def aws-config {:aws-access-key-id     (env :aws-access-key-id)
                 :aws-secret-access-key (env :aws-secret-access-key)
                 :aws-s3-endpoint-url   (env :aws-s3-endpoint-url)
                 :aws-sqs-endpoint-url  (env :aws-sqs-endpoint-url)
                 :aws-region            (env :aws-region)})

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

(defn wrap-handle-queue
  [handler-chan queue-name auto-stop aws-opts queue-opts f]
  (let [queue-config (merge {:queue-name     queue-name
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
    (if auto-stop
      (stop-fn)
      stop-fn)))

(defmacro with-handle-queue-full-opts-standard
  [handler-chan aws-opts queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      true
                      ~aws-opts
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-full-opts-fifo
  [handler-chan aws-opts queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-fifo-queue-name
                      true
                      ~aws-opts
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-aws-opts-standard
  [handler-chan aws-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      true
                      ~aws-opts
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-aws-opts-fifo
  [handler-chan aws-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-fifo-queue-name
                      true
                      ~aws-opts
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-standard
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      true
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-standard-no-autostop
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      false
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-fifo
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      test-fifo-queue-name
                      true
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-standard
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      true
                      {}
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-standard-no-autostop
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      test-standard-queue-name
                      false
                      {}
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-fifo
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      test-fifo-queue-name
                      true
                      {}
                      {}
                      (fn [] ~@body)))

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
    (s3/create-bucket s3-client
                      (:s3-bucket-name aws-config))
    (reset! test-sqs-ext-client (sqs/sqs-ext-client aws-config))
    (f)
    (s3/purge-bucket s3-client
                     (:s3-bucket-name aws-config))))

(defn with-test-bucket
  [f]
  (let [s3-client (s3/s3-client aws-config)]
    (s3/create-bucket s3-client
                      (:s3-bucket-name aws-config))
    (f)
    (s3/purge-bucket s3-client
                     (:s3-bucket-name aws-config))))

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
  [handler-chan auto-stop aws-opts queue-opts f]
  (let [queue-config (merge {:queue-url      @test-queue-url
                             :s3-bucket-name (:s3-bucket-name aws-config)}
                            queue-opts)
        stop-fn (sqs-ext/handle-queue (merge aws-config aws-opts)
                                      queue-config
                                      (partial test-handler-fn handler-chan))]
    (f)
    (if auto-stop
      (stop-fn)
      stop-fn)))

(defmacro with-handle-queue-full-opts-standard
  [handler-chan aws-opts queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      ~aws-opts
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-full-opts-fifo
  [handler-chan aws-opts queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      ~aws-opts
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-aws-opts-standard
  [handler-chan aws-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      ~aws-opts
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-aws-opts-fifo
  [handler-chan aws-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      ~aws-opts
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-standard
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-standard-no-autostop
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      false
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-queue-opts-fifo
  [handler-chan queue-opts & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      {}
                      ~queue-opts
                      (fn [] ~@body)))

(defmacro with-handle-queue-standard
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      {}
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-standard-no-autostop
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      false
                      {}
                      {}
                      (fn [] ~@body)))

(defmacro with-handle-queue-fifo
  [handler-chan & body]
  `(wrap-handle-queue ~handler-chan
                      true
                      {}
                      {}
                      (fn [] ~@body)))

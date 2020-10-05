(ns clj-sqs-extended.internal.receive-test
  (:require [clojure.test :refer [deftest is]]
            [bond.james :as bond]
            [clojure.core.async :as async :refer [chan close! timeout alts!!]]

            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]))

(deftest numerous-simultaneous-receive-loops
  (fixtures/with-test-standard-queue
    (let [message (helpers/random-message-basic)
          n       10
          c       (chan)

          sqs-ext-client
          (sqs/sqs-ext-client fixtures/sqs-ext-config)

          ;; setup
          stop-receive-loops
          (doall (for [_ (range n)]
                   (receive/receive-loop
                     sqs-ext-client
                     @fixtures/test-queue-url
                     c
                     {:auto-delete true})))]

      (is (= n (count stop-receive-loops)))

      (is (sqs/send-message sqs-ext-client
                            @fixtures/test-queue-url
                            message))

      (println "-------- waiting take!")
      (let [[out _] (alts!! [c (timeout 1000)])]
        (is (= message (:body out))))

      ;; teardown
      (doseq [stop-fn stop-receive-loops]
        (stop-fn))
      (close! c)

      ;; gives time for the receive-loop to stop
      (Thread/sleep 500))))


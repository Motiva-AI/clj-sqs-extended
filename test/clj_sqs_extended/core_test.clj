(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer [use-fixtures deftest testing is are]]
            [clojure.core.async :refer [chan close! <!!]]
            [clojure.core.async.impl.protocols :refer [closed?]]
            [bond.james :as bond :refer [with-spy]]
            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import [com.amazonaws
            AmazonServiceException
            SdkClientException]
           [java.net.http HttpTimeoutException]
           [java.net
            SocketException
            UnknownHostException]
           [java.lang ReflectiveOperationException]))


(use-fixtures :once fixtures/with-test-sqs-ext-client)

(defonce test-messages-basic
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-with-time (helpers/random-message-with-time))
(defonce test-message-large (helpers/random-message-larger-than-256kb))

(deftest send-nil-body-message-yields-exception
  (testing "Sending a standard message with a nil body yields exception"
    (fixtures/with-test-standard-queue
      (is (thrown? Exception
                   (sqs-ext/send-message fixtures/aws-config
                                         @fixtures/test-queue-url
                                         nil)))))

  (testing "Sending a FIFO message with a nil body yields exception"
    (fixtures/with-test-fifo-queue
      (is (thrown? Exception
                   (sqs-ext/send-fifo-message fixtures/aws-config
                                              @fixtures/test-queue-url
                                              nil
                                              (helpers/random-group-id)))))))

(deftest send-message-to-non-existing-queue-fails
  (testing "Sending a standard message to a non-existing queue yields proper exception"
    (fixtures/with-test-standard-queue
      (is (thrown-with-msg? SdkClientException
                            #"^.*Unable to execute HTTP request: non-existing-queue.*$"
                            (sqs-ext/send-message fixtures/aws-config
                                                  "https://non-existing-queue"
                                                  (first test-messages-basic))))))

  (testing "Sending a FIFO message to a non-existing queue yields proper exception"
    (fixtures/with-test-fifo-queue
      (is (thrown-with-msg? SdkClientException
                            #"^.*Unable to execute HTTP request: non-existing-queue.*$"
                            (sqs-ext/send-fifo-message fixtures/aws-config
                                                       "https://non-existing-queue"
                                                       (first test-messages-basic)
                                                       (helpers/random-group-id)))))))

(deftest handle-queue-sends-and-receives-basic-messages
  (doseq [format [:transit :json]]
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-queue-opts-standard
          handler-chan
          {:format format}

          (testing "handle-queue can send/receive basic message to standard queue"
            (is (string? (sqs-ext/send-message fixtures/aws-config
                                               @fixtures/test-queue-url
                                               (first test-messages-basic)
                                               {:format format})))
            (is (= (first test-messages-basic) (<!! handler-chan))))

          (testing "handle-queue can send/receive large message to standard queue"
            (is (string? (sqs-ext/send-message fixtures/aws-config
                                               @fixtures/test-queue-url
                                               test-message-large
                                               {:format format})))
            (is (= test-message-large (<!! handler-chan)))))))))

(deftest handle-queue-sends-and-receives-timestamped-message
  (testing "handle-queue can send/receive message including timestamp to standard queue"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-standard
          handler-chan

          (is (string? (sqs-ext/send-message fixtures/aws-config
                                             @fixtures/test-queue-url
                                             test-message-with-time)))
          (is (= test-message-with-time (<!! handler-chan))))))))

(deftest handle-queue-sends-and-receives-fifo-messages
  (testing "handle-queue can send/receive basic messages to FIFO queue"
    (doseq [format [:transit :json]]
      (let [handler-chan (chan)]
        (fixtures/with-test-fifo-queue
          (fixtures/with-handle-queue-queue-opts-fifo
            handler-chan
            {:format format}

            (doseq [message test-messages-basic]
              (is (string? (sqs-ext/send-fifo-message fixtures/aws-config
                                                      @fixtures/test-queue-url
                                                      message
                                                      (helpers/random-group-id)
                                                      {:format format}))))
            (doseq [message test-messages-basic]
              (let [received-message (<!! handler-chan)]
                (is (= message received-message))))))))))

(deftest handle-queue-terminates-with-non-existing-queue
  (testing "handle-queue terminates when non-existing queue is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-queue-opts-standard
                handler-chan
                {:queue-url "https://non-existing-queue"})]

          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

(deftest handle-queue-terminates-with-non-existing-bucket
  (testing "handle-queue terminates when non-existing bucket is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue-aws-opts-standard
                handler-chan
                {:s3-bucket-name "non-existing-bucket"})]

          (is (string? (sqs-ext/send-message fixtures/aws-config
                                             @fixtures/test-queue-url
                                             test-message-large)))
          (is (contains? stats :stopped-at))))
      (close! handler-chan))))

(deftest handle-queue-terminates-after-restart-count-exceeded
  (testing "handle-queue terminates when the restart-count exceeds the limit"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        ;; WATCHOUT: We redefine receive-message to permanently cause an error to be handled,
        ;;           which is recoverable by restarting and should cause the loop to be restarted
        ;;           by an amount of times that fits the restart-limit and delay settings.
        (with-redefs-fn {#'sqs/receive-message
                         (fn [_ _ _] (HttpTimeoutException.
                                       "Testing permanent network failure"))}
          #(let [restart-limit 3
                 restart-delay-seconds 1
                 stop-fn (fixtures/with-handle-queue-queue-opts-standard-no-autostop
                           handler-chan
                           {:restart-limit         restart-limit
                            :restart-delay-seconds restart-delay-seconds})]
             (Thread/sleep (+ (* restart-limit (* restart-delay-seconds 1000)) 500))
             (let [stats (stop-fn)]
               (is (= (:restart-count stats) restart-limit))))))
      (close! handler-chan))))

(deftest handle-queue-restarts-if-recoverable-errors-occurs
  (testing "handle-queue restarts properly and continues running upon recoverable error"
    (let [handler-chan (chan)
          receive-message sqs/receive-message
          called-counter (atom 0)]
      (fixtures/with-test-standard-queue
        ;; WATCHOUT: To test a temporary error, we redefine receive-message to throw
        ;;           an error once and afterwards do what the original function did,
        ;;           which we saved previously:
        (with-redefs-fn {#'sqs/receive-message
                         (fn [sqs-client queue-url opts]
                           (swap! called-counter inc)
                           (if (= @called-counter 1)
                             (HttpTimeoutException. "Testing temporary network failure")
                             (receive-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url
                                              {})))}
          #(let [restart-delay-seconds 1
                 stop-fn (fixtures/with-handle-queue-queue-opts-standard-no-autostop
                           handler-chan
                           {:restart-delay-seconds restart-delay-seconds})]
             ;; give the loop some time to handle that error ...
             (Thread/sleep (+ (* restart-delay-seconds 1000) 500))

             ;; verify that sending/receiving still works ...
             (is (string? (sqs-ext/send-message fixtures/aws-config
                                                @fixtures/test-queue-url
                                                test-message-with-time)))
             (is (= test-message-with-time (<!! handler-chan)))
             (let [stats (stop-fn)]
               ;; ... and that the loop was actually restarted ...
               (is (= (:restart-count stats) 1))
               ;; ... but terminated properly
               (is (contains? stats :stopped-at))))))
      (close! handler-chan))))

(deftest handle-queue-terminates-upon-unrecoverable-error-occured
  (testing "handle-queue terminates when an error occured that was considered fatal/unrecoverable"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        ;; WATCHOUT: We redefine receive-message to permanently cause an error to be handled,
        ;;           which is not recoverable by restarting and therefore terminates the loop.
        (with-redefs-fn {#'sqs/receive-message
                         (fn [_ _ _]
                           (RuntimeException. "Testing runtime error"))}
          #(let [stats (fixtures/with-handle-queue-standard
                         handler-chan)]
             (Thread/sleep 500)
             (is (= (:restart-count stats) 0))
             (is (contains? stats :stopped-at)))))
      (close! handler-chan))))

(deftest nil-returned-after-loop-was-terminated
  (testing "Stopping the listener yields nil response when receiving from the channel again"
    (doseq [format [:transit :json]]
      (fixtures/with-test-standard-queue
        (let [out-chan (chan)
              stop-fn (sqs-ext/receive-loop fixtures/aws-config
                                            @fixtures/test-queue-url
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message fixtures/aws-config
                                             @fixtures/test-queue-url
                                             (first test-messages-basic)
                                             {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))
          ;; terminate receive loop and thereby close the out-channel
          (stop-fn)
          (is (string? (sqs-ext/send-message fixtures/aws-config
                                             @fixtures/test-queue-url
                                             (last test-messages-basic)
                                             {:format format})))
          (is (clojure.core.async.impl.protocols/closed? out-chan))
          (is (nil? (<!! out-chan))))))))

(deftest done-fn-handle-present-when-auto-delete-false
  (testing "done-fn handle is present in response when auto-delete is false"
    (bond/with-spy [fixtures/test-handler-fn]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (fixtures/with-handle-queue-queue-opts-standard
            handler-chan
            {:auto-delete false}

            (is (string? (sqs-ext/send-message fixtures/aws-config
                                               @fixtures/test-queue-url
                                               (last test-messages-basic))))
            (let [received-message (<!! handler-chan)]
              (is (= (last test-messages-basic) received-message))

              (let [received-message (<!! handler-chan)]
                (is (= (last test-messages-basic) received-message))

                ;; delete function handle is returned as last argument ...
                (let [test-handler-fn-args
                      (-> fixtures/test-handler-fn bond/calls first :args)]
                  (is (fn? (last test-handler-fn-args))))))))
        (close! handler-chan)))))


(deftest done-fn-handle-absent-when-auto-delete-true
  (testing "done-fn handle is not present in response when auto-delete is true"
    (bond/with-spy [fixtures/test-handler-fn]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (fixtures/with-handle-queue-queue-opts-standard
            handler-chan
            {:auto-delete true}

            (is (string? (sqs-ext/send-message fixtures/aws-config
                                               @fixtures/test-queue-url
                                               (first test-messages-basic))))

            (let [received-message (<!! handler-chan)]
              (is (= (first test-messages-basic) received-message))

              ;; no delete function handle has been passed as last argument ...
              (let [test-handler-fn-args
                    (-> fixtures/test-handler-fn bond/calls first :args)]
                (is (not (fn? (last test-handler-fn-args))))))))

        (close! handler-chan)))))

(deftest recoverable-errors-get-judged-properly
  (testing "error-might-be-recovered-by-restarting? judges errors correctly"
    (are [severity error]
      (= severity (receive/error-might-be-recovered-by-restarting? error))
      true (UnknownHostException.)
      true (SocketException.)
      true (HttpTimeoutException. "test")
      false (RuntimeException.)
      false (ReflectiveOperationException.))))

(deftest unreachable-endpoint-yields-proper-exception
  (testing "Trying to connect to an unreachable endpoint yields a proper exception"
    (let [unreachable-aws-config (merge fixtures/aws-config
                                        {:sqs-endpoint "https://unreachable-endpoint"
                                         :s3-endpoint  "https://unreachable-endpoint"})]
      (is (thrown? SdkClientException
                   (sqs-ext/send-message unreachable-aws-config
                                         @fixtures/test-queue-url
                                         {:data "here-be-dragons"}))))))

(deftest cannot-send-to-non-existing-bucket
  (testing "Sending a large message using a non-existing S3 bucket yields proper exception"
    (fixtures/with-test-standard-queue
      (let [non-existing-bucket-aws-config (assoc fixtures/aws-config
                                                  :s3-bucket-name
                                                  "non-existing-bucket")]
        (is (thrown? AmazonServiceException
                     (sqs-ext/send-message non-existing-bucket-aws-config
                                           "test-queue"
                                           (helpers/random-message-larger-than-256kb))))))))

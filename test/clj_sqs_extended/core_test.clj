(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer [use-fixtures deftest testing is are]]
            [clojure.core.async :refer [chan close! timeout alt!! <!! thread]]
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
           [com.amazonaws.services.sqs.model AmazonSQSException]
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
                   (sqs-ext/send-message fixtures/sqs-ext-config
                                         @fixtures/test-queue-url
                                         nil)))))

  (testing "Sending a FIFO message with a nil body yields exception"
    (fixtures/with-test-fifo-queue
      (is (thrown? Exception
                   (sqs-ext/send-fifo-message fixtures/sqs-ext-config
                                              @fixtures/test-queue-url
                                              nil
                                              (helpers/random-group-id)))))))

(deftest send-message-to-non-existing-queue-fails
  (testing "Sending a standard message to a non-existing queue yields proper exception"
    (fixtures/with-test-standard-queue
      (is (thrown-with-msg? SdkClientException
                            #"^.*Unable to execute HTTP request: non-existing-queue.*$"
                            (sqs-ext/send-message fixtures/sqs-ext-config
                                                  "https://non-existing-queue"
                                                  (first test-messages-basic))))))

  (testing "Sending a FIFO message to a non-existing queue yields proper exception"
    (fixtures/with-test-fifo-queue
      (is (thrown-with-msg? SdkClientException
                            #"^.*Unable to execute HTTP request: non-existing-queue.*$"
                            (sqs-ext/send-fifo-message fixtures/sqs-ext-config
                                                       "https://non-existing-queue"
                                                       (first test-messages-basic)
                                                       (helpers/random-group-id)))))))

(deftest handle-queue-sends-and-receives-basic-messages
  (doseq [format [:transit :json]]
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-defaults
          handler-chan

          (testing "handle-queue can send/receive basic message to standard queue"
            (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                               @fixtures/test-queue-url
                                               (first test-messages-basic)
                                               {:format format})))
            (is (= (first test-messages-basic) (<!! handler-chan))))

          (testing "handle-queue can send/receive large message to standard queue"
            (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                               @fixtures/test-queue-url
                                               test-message-large
                                               {:format format})))
            (is (= test-message-large (<!! handler-chan))))))
      (close! handler-chan))))

(deftest handle-queue-sends-and-receives-messages-without-bucket
  (testing "handle-queue can send/receive message without using a s3 bucket"
    (let [handler-chan (chan)
          sqs-ext-config-without-bucket (dissoc fixtures/sqs-ext-config
                                                :s3-bucket-name)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue
          handler-chan
          {:sqs-ext-config {:s3-bucket-name nil}}

          (is (string? (sqs-ext/send-message sqs-ext-config-without-bucket
                                             @fixtures/test-queue-url
                                             (first test-messages-basic))))
          (is (= (first test-messages-basic) (<!! handler-chan)))
          (is (string? (sqs-ext/send-message sqs-ext-config-without-bucket
                                             @fixtures/test-queue-url
                                             test-message-with-time)))
          (is (= test-message-with-time (<!! handler-chan)))))
      (close! handler-chan))))

(deftest handle-queue-sends-and-receives-timestamped-message
  (testing "handle-queue can send/receive message including timestamp to standard queue"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (fixtures/with-handle-queue-defaults
          handler-chan

          (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                             @fixtures/test-queue-url
                                             test-message-with-time)))
          (is (= test-message-with-time (<!! handler-chan)))))
      (close! handler-chan))))

(deftest handle-queue-sends-and-receives-fifo-messages
  (testing "handle-queue can send/receive basic messages to FIFO queue"
    (doseq [format [:transit :json]]
      (let [handler-chan (chan)]
        (fixtures/with-test-fifo-queue
          (fixtures/with-handle-queue-defaults
            handler-chan

            (doseq [message test-messages-basic]
              (is (string? (sqs-ext/send-fifo-message fixtures/sqs-ext-config
                                                      @fixtures/test-queue-url
                                                      message
                                                      (helpers/random-group-id)
                                                      {:format format}))))
            (doseq [message test-messages-basic]
              (let [received-message (<!! handler-chan)]
                (is (= message received-message))))))
        (close! handler-chan)))))

(deftest handle-queue-terminates-with-non-existing-queue
  (testing "handle-queue terminates when non-existing queue is used"
    (let [handler-chan (chan)
          stats (fixtures/with-handle-queue
                   handler-chan
                   {:handler-opts {:queue-url "https://non-existing-queue"}})]
         (is (contains? stats :stopped-at))
      (close! handler-chan))))

(deftest handle-queue-terminates-with-non-existing-bucket
  (testing "handle-queue terminates when non-existing bucket is used"
    (let [handler-chan (chan)]
      (fixtures/with-test-standard-queue
        (let [stats
              (fixtures/with-handle-queue
                handler-chan
                {:sqs-ext-config {:s3-bucket-name "non-existing-bucket"}})]

          (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
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
                 stop-fn (fixtures/with-handle-queue
                           handler-chan
                           {:auto-stop-loop false
                            :handler-opts   {:restart-limit         restart-limit
                                             :restart-delay-seconds restart-delay-seconds}})]
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
                         (fn [_ _ _]
                           (swap! called-counter inc)
                           (if (= @called-counter 1)
                             (HttpTimeoutException. "Testing temporary network failure")
                             (receive-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url
                                              {})))}
          #(let [restart-delay-seconds 1
                 stop-fn (fixtures/with-handle-queue
                           handler-chan
                           {:auto-stop-loop false
                            :handler-opts   {:restart-delay-seconds restart-delay-seconds}})]
             ;; give the loop some time to handle that error ...
             (Thread/sleep (+ (* restart-delay-seconds 1000) 500))

             ;; verify that sending/receiving still works ...
             (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
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
          #(let [stats (fixtures/with-handle-queue-defaults
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
              stop-fn (sqs-ext/receive-loop fixtures/sqs-ext-config
                                            @fixtures/test-queue-url
                                            out-chan
                                            {:format format})]
          (is (fn? stop-fn))
          (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                             @fixtures/test-queue-url
                                             (first test-messages-basic)
                                             {:format format})))
          (is (= (first test-messages-basic) (:body (<!! out-chan))))

          ;; terminate receive loop and thereby close the out-channel
          (stop-fn)

          (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                             @fixtures/test-queue-url
                                             (last test-messages-basic)
                                             {:format format})))
          (is (clojure.core.async.impl.protocols/closed? out-chan))
          (is (nil? (<!! out-chan))))))))

(deftest manually-deleted-messages-dont-get-resent
  (testing "Messages deleted by invoking the done-fn handle are not resent"
    (bond/with-spy [fixtures/test-handler-fn]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (fixtures/with-handle-queue
            handler-chan
            {:handler-opts {:auto-delete false}}

            (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                               @fixtures/test-queue-url
                                               (last test-messages-basic))))

            (let [received-message (<!! handler-chan)]
              ;; message received properly
              (is (= (last test-messages-basic) received-message))

              ;; delete function handle is returned as last argument ...
              (let [test-handler-fn-args
                    (-> fixtures/test-handler-fn bond/calls first :args)]
                (is (fn? (last test-handler-fn-args))))

              ;; delete it manually now ...
              (@fixtures/test-handler-done-fn)

              ;; verify its not received again ...
              (is (alt!!
                    handler-chan false
                    (timeout 1000) true)))))

        (close! handler-chan)))))

(deftest messages-get-resent-if-not-deleted-manually-and-auto-delete-is-false
  (testing "Messages get resent if the done-fn is not invoked upon receival and auto-delete is false"
    (bond/with-spy [fixtures/test-handler-fn]
      (let [handler-chan (chan)
            visibility-timeout 1]
        (fixtures/with-test-standard-queue-opts
          {:visibility-timeout-in-seconds visibility-timeout}

          (fixtures/with-handle-queue
            handler-chan
            {:handler-opts {:auto-delete false}}

            (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                               @fixtures/test-queue-url
                                               (last test-messages-basic))))

            (let [received-message (<!! handler-chan)]
              ;; message received properly
              (is (= (last test-messages-basic) received-message))

              ;; delete function handle is returned as last argument ...
              (let [test-handler-fn-args
                    (-> fixtures/test-handler-fn bond/calls first :args)]
                (is (fn? (last test-handler-fn-args))))

              ;; nothing comes out of the channel within the visibility timeout ...
              (is (alt!!
                    handler-chan false
                    (timeout (- (* 1000 visibility-timeout) 100)) true))

              ;; but afterwards ...
              (Thread/sleep 200)
              (is (= (last test-messages-basic) (<!! handler-chan))))))

        (close! handler-chan)))))

;; WATCHOUT: This is the original function from the core, but it
;;           passes the entire SQS message into the channel so
;;           that the receipt handle is accessible in the next test.
(defn- launch-handler-threads-with-complete-sqs-message-forwarding
  [number-of-handler-threads receive-chan auto-delete handler-fn]
  (dotimes [_ number-of-handler-threads]
    (thread
      (loop []
        (when-let [message (<!! receive-chan)]
          (try
            (if auto-delete
              (handler-fn message)
              (handler-fn message (:done-fn message)))
            (catch Throwable _
              (println "Handler function threw an error!")))
          (recur))))))

(deftest done-fn-handle-absent-when-auto-delete-true
  (testing "done-fn handle is not present in response when auto-delete is true"
    (bond/with-spy [fixtures/test-handler-fn]
      (let [handler-chan (chan)]
        (fixtures/with-test-standard-queue
          (with-redefs-fn {#'sqs-ext/launch-handler-threads
                           launch-handler-threads-with-complete-sqs-message-forwarding}
            #(fixtures/with-handle-queue
               handler-chan
               {:handler-opts {:auto-delete true}}

               (is (string? (sqs-ext/send-message fixtures/sqs-ext-config
                                                  @fixtures/test-queue-url
                                                  (first test-messages-basic))))

               (let [received-message (<!! handler-chan)]
                 (is (= (first test-messages-basic) (:body received-message)))

                 ;; no delete function handle has been passed as last argument ...
                 (let [test-handler-fn-args
                       (-> fixtures/test-handler-fn bond/calls first :args)]
                   (is (not (fn? (last test-handler-fn-args)))))

                 ;; not sure why this is needed, but otherwise the next check
                 ;; becomes an intermittent fail
                 (Thread/sleep 50)

                 ;; WATCHOUT: With the handler thread redeffed we can now access
                 ;;           the receipt handle of the SQS message and try to
                 ;;           delete the message manually. If the message was
                 ;;           auto-deleted by the core API before this should
                 ;;           yield an AmazonSQSException:
                 (is (thrown-with-msg?
                       AmazonSQSException
                       #"^.*Service: AmazonSQS; Status Code: 400;.*$"
                       (sqs-ext/delete-message! fixtures/sqs-ext-config
                                                @fixtures/test-queue-url
                                                received-message)))))))

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
    (let [unreachable-sqs-ext-config (merge fixtures/sqs-ext-config
                                        {:sqs-endpoint "https://unreachable-endpoint"
                                         :s3-endpoint  "https://unreachable-endpoint"})]
      (is (thrown? SdkClientException
                   (sqs-ext/send-message unreachable-sqs-ext-config
                                         @fixtures/test-queue-url
                                         {:data "here-be-dragons"}))))))

(deftest cannot-send-to-non-existing-bucket
  (testing "Sending a large message using a non-existing S3 bucket yields proper exception"
    (fixtures/with-test-standard-queue
      (let [non-existing-bucket-sqs-ext-config (assoc fixtures/sqs-ext-config
                                                  :s3-bucket-name
                                                  "non-existing-bucket")]
        (is (thrown? AmazonServiceException
                     (sqs-ext/send-message non-existing-bucket-sqs-ext-config
                                           "test-queue"
                                           (helpers/random-message-larger-than-256kb))))))))

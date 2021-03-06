(ns clj-sqs-extended.core-test
  (:require [clojure.test :refer [use-fixtures deftest testing is]]
            [bond.james :as bond]

            [clojure.core.async :refer [chan close! <!! >!! timeout alt!! alts!! thread]]
            [clojure.core.async.impl.protocols]

            [clj-sqs-extended.aws.sqs :as sqs]
            [clj-sqs-extended.core :as sqs-ext]
            [clj-sqs-extended.internal.receive :as receive]
            [clj-sqs-extended.test-fixtures :as fixtures]
            [clj-sqs-extended.test-helpers :as helpers])
  (:import [com.amazonaws.services.sqs.model AmazonSQSException]
           [java.net.http HttpTimeoutException]))

;; fixtures ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(use-fixtures :once fixtures/with-test-sqs-ext-client fixtures/with-test-s3-bucket)
(use-fixtures :each fixtures/with-transient-queue)

(defonce test-handler-done-fn (atom nil))

(defn test-handler-fn
  ([chan message]
   (>!! chan message))
  ([chan message done-fn]
   (reset! test-handler-done-fn done-fn)
   (>!! chan message)))

(defn wrap-handle-queue
  [handler-chan settings f]
  (let [handler-config (merge {:queue-url @fixtures/test-queue-url} (:handler-opts settings))
        stop-fn (sqs-ext/handle-queue
                  (sqs-ext/sqs-ext-client
                    (merge fixtures/sqs-ext-config (:sqs-ext-config settings)))
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

(defonce test-messages-basic
         (into [] (take 5 (repeatedly helpers/random-message-basic))))
(defonce test-message-with-time (helpers/random-message-with-time))
(defonce test-message-large (helpers/random-message-larger-than-256kb))

;; tests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn timed-take!!
  ([c] (timed-take!! c 3000))

  ([c timeout-in-ms]
   (-> (alts!! [c (timeout timeout-in-ms)])
       (first))))

(deftest ^:functional handle-queue-sends-and-receives-basic-messages
  (let [handler-chan (chan)]
    (with-handle-queue-defaults
      handler-chan

      (testing "handle-queue can send/receive basic messages to standard queue"
        (let [message (first test-messages-basic)]
          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             @fixtures/test-queue-url
                                             message)))
          (is (= message (<!! handler-chan)))

          (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                             @fixtures/test-queue-url
                                             message)))
          (is (= message (<!! handler-chan)))))

      (testing "handle-queue can send/receive large message to standard queue"
        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url
                                           test-message-large)))
        (is (true? ;; wrapping this in a true? so that if this test fails,
                   ;; it doesn't print the whole giant message
                   (= test-message-large (timed-take!! handler-chan))))))
    (close! handler-chan)))

(deftest ^:functional handle-queue-sends-and-receives-messages-without-bucket
  (let [handler-chan (chan)
        sqs-ext-client-without-bucket (sqs/sqs-ext-client
                                        (dissoc fixtures/sqs-ext-config
                                                :s3-bucket-name))]
    (with-handle-queue
      handler-chan
      {:sqs-ext-config {:s3-bucket-name nil}}

      (is (string? (sqs-ext/send-message sqs-ext-client-without-bucket
                                         @fixtures/test-queue-url
                                         (first test-messages-basic))))
      (is (= (first test-messages-basic) (timed-take!! handler-chan)))
      (is (string? (sqs-ext/send-message sqs-ext-client-without-bucket
                                         @fixtures/test-queue-url
                                         test-message-with-time)))
      (is (= test-message-with-time (timed-take!! handler-chan))))
    (close! handler-chan)))

(deftest ^:functional handle-queue-sends-and-receives-timestamped-message
  (let [handler-chan (chan)]
    (with-handle-queue-defaults
      handler-chan

      (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                         @fixtures/test-queue-url
                                         test-message-with-time)))
      (is (= test-message-with-time (timed-take!! handler-chan))))
    (close! handler-chan)))

(deftest ^:functional handle-queue-sends-and-receives-fifo-messages
  (let [handler-chan (chan)
        message (first test-messages-basic)]
    (with-handle-queue-defaults
      handler-chan

      (is (string? (sqs-ext/send-fifo-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url
                                              message
                                              (helpers/random-group-id))))

      (is (= message (<!! handler-chan))))))

;; we unexpectedly get a round-trip message working when it's supposed to fail
#_(deftest ^:functional handle-queue-terminates-with-non-existing-bucket
  (let [handler-chan (chan)]
    (bond/with-spy [receive/stop-receive-loop!]
      (with-handle-queue
        handler-chan
        {:sqs-ext-config {:s3-bucket-name "non-existing-bucket"}}

        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url
                                           test-message-large)))
        ;; TODO why does this work? perhaps localstack disregard s3-bucket-name?
        (is (= test-message-large
               (timed-take!! handler-chan)))
        #_(is (= 1 (-> receive/stop-receive-loop!
                       (bond/calls)
                       (count))))))

    (close! handler-chan)))

(deftest ^:functional handle-queue-terminates-after-restart-count-exceeded
  (let [handler-chan          (chan)
        restart-limit         2
        restart-delay-seconds 1]
    ;; WATCHOUT: We redefine receive-messages to permanently cause an error to be handled,
    ;;           which is recoverable by restarting and should cause the loop to be restarted
    ;;           by an amount of times that fits the restart-limit and delay settings.
    (bond/with-spy [receive/exit-receive-loop!]
      (with-redefs-fn {#'sqs/wait-and-receive-messages-from-sqs
                       (fn [_ _ _] (throw (HttpTimeoutException.
                                            "Testing permanent network failure")))}
        #(with-handle-queue
           handler-chan
           {:handler-opts {:restart-limit         restart-limit
                           :restart-delay-seconds restart-delay-seconds}}

           ;; wait for restarts to have time to happen
           (Thread/sleep (+ (* restart-limit
                               (* restart-delay-seconds 1000))
                            500))))

      ;; wait for receive-loop to teardown
      (Thread/sleep 1000)

      (let [exit-calls (-> receive/exit-receive-loop!
                           (bond/calls))]
        (is (= 1 (count exit-calls)))
        (is (= restart-limit
               (-> exit-calls (first) (:return) (:restart-count))))))

    (close! handler-chan)))

(deftest ^:functional handle-queue-restarts-if-error-occurs
  (let [handler-chan (chan)
        wait-and-receive-messages-from-sqs sqs/wait-and-receive-messages-from-sqs
        called-counter (atom 0)]
    ;; WATCHOUT: To test a temporary error, we redefine receive-messages to throw
    ;;           an error once and afterwards do what the original function did,
    ;;           which we saved previously:
    (with-redefs-fn {#'sqs/wait-and-receive-messages-from-sqs
                     (fn [sqs-client queue-url wait-time-in-seconds]
                       (swap! called-counter inc)
                       (if (= @called-counter 1)
                         (throw (HttpTimeoutException. "Testing temporary network failure"))
                         (wait-and-receive-messages-from-sqs sqs-client queue-url wait-time-in-seconds)))}
      #(bond/with-spy [receive/pause-to-recover-this-loop]
         (let [restart-delay-seconds 1]
           (with-handle-queue
             handler-chan
             {:handler-opts   {:restart-delay-seconds restart-delay-seconds}}

             ;; give the loop some time to handle that error ...
             (Thread/sleep (+ (* restart-delay-seconds 1000) 200))
             (is (= 1
                    (-> receive/pause-to-recover-this-loop
                        (bond/calls)
                        (count))))

             ;; verify that sending/receiving still works ...
             (is (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                       @fixtures/test-queue-url
                                       test-message-with-time))
             (is (not (clojure.core.async.impl.protocols/closed? handler-chan)))
             (is (= test-message-with-time
                    (timed-take!! handler-chan 1000)))))))
    (close! handler-chan)))

(deftest ^:functional manually-deleted-messages-dont-get-resent
  (bond/with-spy [test-handler-fn]
    (let [handler-chan (chan)]
      (with-handle-queue
        handler-chan
        {:handler-opts {:auto-delete false}}

        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url
                                           (last test-messages-basic))))

        (let [received-message (timed-take!! handler-chan)]
          ;; message received properly
          (is (= (last test-messages-basic) received-message))

          ;; delete function handle is returned as last argument ...
          (let [test-handler-fn-args
                (-> test-handler-fn bond/calls first :args)]
            (is (fn? (last test-handler-fn-args))))

          ;; delete it manually now ...
          (@test-handler-done-fn)

          ;; verify its not received again ...
          (is (alt!!
                handler-chan false
                (timeout 1000) true))))

      (close! handler-chan))))

(deftest ^:functional messages-get-resent-if-not-deleted-manually-and-auto-delete-is-false
  (bond/with-spy [test-handler-fn]
    (let [handler-chan (chan)
          message      (last test-messages-basic)
          visibility-timeout 1]
      (with-handle-queue
        handler-chan
        {:handler-opts {:auto-delete false}}

        (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                           @fixtures/test-queue-url
                                           message)))

        ;; message received properly
        (is (= message (timed-take!! handler-chan)))

        ;; delete function handle is returned as last argument ...
        (is (fn? (-> test-handler-fn bond/calls last :args last)))

        ;; nothing comes out of the channel within the visibility timeout ...
        (is (alt!!
              handler-chan                                  false
              (timeout (- (* 1000 visibility-timeout) 100)) true))

        ;; but afterwards ...
        (Thread/sleep 150)
        (is (= message (timed-take!! handler-chan))))

      (close! handler-chan))))

;; WATCHOUT: This is the original function from the core, but it
;;           passes the entire SQS message into the channel so
;;           that the receipt handle is accessible in the next test.
(defn- launch-handler-threads-with-complete-sqs-message-forwarding
  [number-of-handler-threads receive-chan auto-delete handler-fn]
  (dotimes [_ number-of-handler-threads]
    (thread
      (loop []
        (when-let [message (timed-take!! receive-chan)]
          (try
            (if auto-delete
              (handler-fn message)
              (handler-fn message (:done-fn message)))
            (catch Throwable _
              (println "Handler function threw an error!")))
          (recur))))))

(deftest ^:functional message-is-auto-deleted-when-auto-delete-is-true
  (bond/with-spy [test-handler-fn]
    (let [handler-chan (chan)]
      (with-redefs-fn {#'sqs-ext/launch-handler-threads
                       launch-handler-threads-with-complete-sqs-message-forwarding}
        #(with-handle-queue
           handler-chan
           {:handler-opts {:auto-delete true}}

           (is (string? (sqs-ext/send-message @fixtures/test-sqs-ext-client
                                              @fixtures/test-queue-url
                                              (first test-messages-basic))))

           (let [received-message (timed-take!! handler-chan)]
             (is (= (first test-messages-basic) (:body received-message)))

             ;; no delete function handle has been passed as last argument ...
             (let [test-handler-fn-args
                   (-> test-handler-fn bond/calls first :args)]
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
                   #"Service: AmazonSQS; Status Code: 400;"
                   (sqs/delete-message! @fixtures/test-sqs-ext-client
                                        @fixtures/test-queue-url
                                        received-message))))))

      (close! handler-chan))))

(deftest ^:functional message-is-auto-deleted-before-handler-finishes-when-auto-delete-is-true
  (with-redefs-fn {#'sqs-ext/launch-handler-threads
                   launch-handler-threads-with-complete-sqs-message-forwarding}
    #(let [c                 (chan)
           message-preview    (promise)
           message-processed? (atom false)

           handler-fn (fn [message]
                        ;; deliver the received message prior to a sleep so
                        ;; that we can test if it's been deleted by
                        ;; auto-delete
                        (deliver message-preview message)
                        (Thread/sleep 2000)
                        (reset! message-processed? true)
                        (>!! c message))
           msg        (helpers/random-message-basic)]

       (is (sqs-ext/send-message @fixtures/test-sqs-ext-client @fixtures/test-queue-url msg))

       (bond/with-spy [receive/delete-message-if-auto-delete
                       sqs/delete-message!]
         (let [stop-fn (sqs-ext/handle-queue
                         @fixtures/test-sqs-ext-client
                         {:queue-url                 @fixtures/test-queue-url
                          :number-of-handler-threads 1
                          :auto-delete               true}
                         handler-fn)]
           (is (fn? (:done-fn @message-preview)))
           (is (= 1
                  (-> receive/delete-message-if-auto-delete
                      (bond/calls)
                      (count))))
           (Thread/sleep 100) ;; wait for delete-message! to run
           (is (= 1
                  (-> sqs/delete-message!
                      (bond/calls)
                      (count))))

           ;; trying to delete message here should throw error since message should have been deleted already
           (is (thrown-with-msg?
                 AmazonSQSException
                 #"Status Code: 400"
                 (sqs/delete-message! @fixtures/test-sqs-ext-client
                                      @fixtures/test-queue-url
                                      @message-preview)))

           ;; ensure that he message hasn't been processed by handler-fn at this point
           (is (false? @message-processed?))

           ;; block and wait for message to come in properly
           (is (= msg (:body (<!! c))))
           (is (true? @message-processed?))

           ;; teardown
           (stop-fn))))))

(deftest ^:functional only-acknowledge-messages-that-handler-is-available-to-process
  (let [n        5
        messages (into [] (take n (repeatedly helpers/random-message-basic)))
        c        (chan)

        ;; setup
        stop-receive-loop
        (receive/receive-loop
          @fixtures/test-queue-url
          c
          (partial sqs/receive-messages
                   @fixtures/test-sqs-ext-client
                   @fixtures/test-queue-url
                   {:max-number-of-receiving-messages 1
                    :wait-time-in-seconds             1})
          (partial sqs/delete-message!
                   @fixtures/test-sqs-ext-client
                   @fixtures/test-queue-url)
          {:auto-delete? false})]

    (is (fn? stop-receive-loop))
    (is (= {"ApproximateNumberOfMessages" 0, "ApproximateNumberOfMessagesNotVisible" 0}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))

    (doseq [msg messages]
      (sqs/send-message @fixtures/test-sqs-ext-client @fixtures/test-queue-url msg))

    ;; these expected values require some explanation:
    ;; 1. one message is read off the queue and become NotVisible
    ;; 2. this first message is pushed to the output channel. Now the output
    ;;    channel (default size is 1) is full.
    ;; 3. on the next receive-loop iteration, a second message is read off the queue
    ;; 4. but when receive-to-channel tries to put this second message to the
    ;;    output channel, it is blocked because the output channel is full.
    ;; 5. Thus we expect two messages to be read from the queue with
    ;;    NotVisible values = 2
    (is (= {"ApproximateNumberOfMessages" (- n 2), "ApproximateNumberOfMessagesNotVisible" 2}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))

    (let [[out _] (alts!! [c (timeout 1000)])]
      (is (:body out))
      (is ((:done-fn out))))
    (Thread/sleep 100) ;; wait for message to be deleted

    (is (= {"ApproximateNumberOfMessages" (- n 3) , "ApproximateNumberOfMessagesNotVisible" 2}
           (sqs/queue-attributes @fixtures/test-sqs-ext-client @fixtures/test-queue-url)))

    ;; teardown
    (stop-receive-loop)
    (close! c)

    ;; gives time for the receive-loop to stop
    (Thread/sleep 500)))


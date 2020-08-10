(ns clj-sqs-extended.core
  (:require [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.aws :as aws]
            [clojure.core.async :as async
             :refer [chan go-loop <! >! <!! >!! thread]]
            [tick.alpha.api :as t]
            [clojure.tools.logging :as log]
            [clj-sqs-extended.sqs :as sqs]))


(defn receive-loop
  ([sqs-ext-client queue-name out-chan]
   (receive-loop sqs-ext-client queue-name out-chan {}))

  ([sqs-ext-client queue-name out-chan
    {:keys [auto-delete
            restart-delay-seconds
            format
            num-consumers]
     :or   {auto-delete           true
            restart-delay-seconds 1
            format                :transit
            num-consumers         1}
     :as   opts}]
   (let [queue-url (sqs-ext/queue-name-to-url sqs-ext-client
                                              queue-name)
         receive-to-chan #(sqs/receive-message-channeled sqs-ext-client
                                                         queue-name
                                                         opts)
         loop-state (atom {:messages (receive-to-chan)
                           :running  true
                           :stats    {:count         0
                                      :started-at    (t/now)
                                      :restart-count 0
                                      :queue-url     queue-url}})]
     (letfn [(restart-loop
               []
               (log/infof "Restarting receive-loop for queue '%s'" queue-name)
               (let [messages-chan (:messages @loop-state)]
                 (swap! loop-state
                        (fn [state]
                          (-> state
                              (assoc :messages (receive-to-chan))
                              (update-in [:stats :restart-count] inc)
                              (assoc-in [:stats :restarted-at] (t/now)))))
                 (async/close! messages-chan)))

             (stop-loop
               []
               (when (:running @loop-state)
                 (log/infof "Terminating receive-loop for queue '%s'" queue-name)
                 (swap! loop-state assoc :running false)
                 (async/close! (:messages @loop-state))
                 (async/close! out-chan))
               (:stats @loop-state))

             (secs-between
               [d1 d2]
               (t/seconds (t/between d1 d2)))

             (update-stats
               [state]
               (let [now (t/now)]
                 (-> state
                     (update-in [:stats :count] inc)
                     (assoc-in [:stats :this-pass-started-at] now)
                     (assoc-in [:stats :loop-duration]
                               (secs-between (-> state :stats :started-at) now)))))]

       (go-loop []
         (swap! loop-state update-stats)

         (try
           (let [{:keys [body] :as message} (<! (:messages @loop-state))]
             (cond
               (nil? message)
               (stop-loop)

               (instance? Throwable message)
               (let [{:keys [this-pass-started-at] :as stats} (:stats @loop-state)]
                 (log/warn message "Received an error"
                           (assoc stats :last-wait-duration
                                        (secs-between this-pass-started-at
                                                      (t/now))))
                 ;; WATCHOUT: Adding a restart delay so that this doesn't go into an
                 ;;           abusively tight loop if the queue listener is failing to
                 ;;           start continuously.
                 (<! (async/timeout (int (* restart-delay-seconds 1000))))
                 (restart-loop))

               (not (empty? message))
               (let [done-fn #(sqs-ext/delete-message sqs-ext-client queue-name message)
                     msg (cond-> message
                                 (not auto-delete) (assoc :done-fn done-fn))]
                 (if body
                   (>! out-chan msg)
                   (log/warnf "Queue '%s' received a nil body message: %s"
                              queue-name
                              message))
                 (when auto-delete
                   (done-fn)))))

           (catch Exception e
             (log/errorf e "Failed receiving message for %s" (:stats @loop-state))))

         (if (:running @loop-state)
           (recur)
           (log/warnf "Receive-loop terminated for %s" (:stats @loop-state))))

       stop-loop))))


(defn handle-queue
  ([{:keys [access-key
            secret-key
            endpoint-url
            region]
     :or   {access-key   "default"
            secret-key   "default"
            endpoint-url "http://localhost:4566"
            region       "us-east-2"}
     :as   aws-creds}
    {:keys [queue-name
            s3-bucket-name
            num-handler-threads
            auto-delete
            format]
     :or   {num-handler-threads 4
            auto-delete         true
            format              :transit}}
    handler-fn]
   (let [endpoint (aws/configure-endpoint aws-creds)
         creds (aws/configure-credentials aws-creds)
         sqs-ext-client (sqs-ext/sqs-ext-client s3-bucket-name
                                                endpoint
                                                creds)
         receive-chan (chan)
         stop-fn (receive-loop sqs-ext-client
                               queue-name
                               receive-chan
                               {:auto-delete auto-delete
                                :format      format})]
     ((log/infof (str "Starting receive loop for queue '%s' with:\n"
                      "  num-handler-threads: %d\n"
                      "  auto-delete: %s")
                 queue-name num-handler-threads auto-delete)
      (dotimes [_ num-handler-threads]
        (thread
          (loop []
            (when-let [message (<!! receive-chan)]
              (try
                (if auto-delete
                  (handler-fn message)
                  (handler-fn message (:done-fn message)))
                (catch Throwable t
                  (log/error t "SQS handler function threw an error.")))
              (recur)))))
      stop-fn))))

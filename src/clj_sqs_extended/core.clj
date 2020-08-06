(ns clj-sqs-extended.core
  (:require [clj-sqs-extended.sqs :as sqs-ext]
            [clj-sqs-extended.utils :as utils]
            [clojure.core.async :as async
             :refer [chan go-loop <! >! <!! >!! thread]]
            [tick.alpha.api :as t]
            [clojure.tools.logging :as log]))


(defn receive-loop
  ([sqs-ext-client queue-url out-chan]
   (receive-loop sqs-ext-client queue-url out-chan {}))

  ([sqs-ext-client queue-url out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :or   {auto-delete           true
            restart-delay-seconds 1}}]
   (let [loop-state (atom {:running true
                           :stats   {:count         0
                                     :started-at    (t/now)
                                     :restart-count 0
                                     :restarted-at  nil
                                     :queue-url     queue-url}})]
     (letfn [(restart-loop
               []
               (log/infof "Restarting receive-loop for %s" queue-url)
               (swap! loop-state
                      (fn [state]
                        (-> state
                            (update-in [:stats :restart-count] inc)
                            (assoc-in [:stats :restarted-at] (t/now))))))

             (stop-loop
               []
               (when (:running @loop-state)
                 (log/infof "Terminating receive-loop for %s" queue-url)
                 (swap! loop-state assoc :running false)
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
           ;; TODO: Taking out messages from the queue isn't channeled anymore. Should it (still) be?!
           (let [{:keys [body] :as message} (sqs-ext/receive-message sqs-ext-client queue-url)]
             (cond
               (nil? message)
               (stop-loop)

               (instance? Throwable message)
               (let [{:keys [this-pass-started-at] :as stats} (:stats @loop-state)]
                 (log/warn message "Received an error"
                           (assoc stats :last-wait-duration
                                        (secs-between this-pass-started-at
                                                      (t/now))))
                 (<! (async/timeout (int (* restart-delay-seconds 1000))))
                 (restart-loop))

               (not (empty? message))
               (let [done-fn #(sqs-ext/delete-message sqs-ext-client queue-url message)
                     msg (cond-> message
                                 (not auto-delete) (assoc :done-fn done-fn))]
                 (if body
                   (>! out-chan msg)
                   (log/warnf "Queue %s received a nil body message: %s" queue-url message))
                 (when auto-delete
                   (done-fn)))))

           (catch Exception e
             (log/errorf e "Failed receiving message for %s" (:stats @loop-state))))

         (if (:running @loop-state)
           (recur)
           (log/warnf "Receive-loop terminated for %s" (:stats @loop-state))))

       stop-loop))))


(defn handle-queue
  ([queue-url handler-fn]
   (handle-queue queue-url handler-fn {}))
  ([queue-url handler-fn
    {:keys [num-handler-threads
            auto-delete
            bucket-name]
     :or   {num-handler-threads 4
            auto-delete         true}}]

   (let [endpoint (utils/configure-endpoint
                    {:url    "http://localhost:4566"
                     :region "us-east-2"})
         creds (utils/configure-credentials
                 {:access-key "localstack"
                  :secret-key "localstack"})
         sqs-ext-client (sqs-ext/sqs-ext-client bucket-name
                                                endpoint
                                                creds)]
     (log/infof (str "Starting receive loop for %s with:\n"
                     "  num-handler-threads: %d\n"
                     "  auto-delete: %s")
                queue-url num-handler-threads auto-delete)
     (let [receive-chan (chan)
           stop-fn (receive-loop sqs-ext-client
                                 queue-url
                                 receive-chan
                                 {:auto-delete auto-delete})]
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

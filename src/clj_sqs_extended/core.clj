(ns clj-sqs-extended.core
  (:require [clj-sqs-extended.sqs :as sqs-ext]
            [clojure.core.async :as async
             :refer [chan go-loop <! >! <!! >!! thread]]
            [tick.alpha.api :as t]
            [clojure.tools.logging :as log]))


(defn receive-loop
  ([sqs-client queue-url out-chan]
   (receive-loop sqs-client queue-url out-chan {}))

  ([sqs-client queue-url out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :or   {auto-delete           true
            restart-delay-seconds 1}}]
   (let [receive-from-chan #(sqs-ext/receive-message sqs-client queue-url)
         loop-state (atom {:messages (receive-from-chan)
                           :running  true
                           :stats    {:count         0
                                      :started-at    (t/now)
                                      :restart-count 0
                                      :restarted-at  nil
                                      :queue-url     queue-url}})]
     (letfn [(restart-loop
               []
               (log/infof "Restarting receive-loop for %s" queue-url)
               (let [messages-chan (:messages @loop-state)]
                 (swap! loop-state
                        (fn [state]
                          (-> state
                              (dissoc :messages)
                              (update-in [:stats :restart-count] inc)
                              (assoc-in [:stats :restarted-at] (t/now)))))
                 (async/close! messages-chan)))

             (stop-loop
               []
               (when (:running @loop-state)
                 (log/infof "Terminating receive-loop for %s" queue-url)
                 (swap! loop-state assoc :running false)
                 (async/close! (:messages @loop-state))
                 (async/close! out-chan))
               (:stats @loop-state))

             (secs-between
               [d1 d2]
               (t/seconds (t/between d1 d2)))

             (update-loop-state
               [state]
               (let [{:keys [started-at]} (:stats state)
                     now (t/now)]
                 (-> state
                     (assoc :messages (receive-from-chan))
                     (update-in [:stats :count] inc)
                     (assoc-in [:stats :this-pass-started-at] now)
                     (assoc-in [:stats :loop-duration]
                               (secs-between (-> state :stats :started-at) now)))))]

       (go-loop []
         (swap! loop-state update-loop-state)

         (try
           (let [message (:messages @loop-state)]
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

               :else
               (when-not (empty? message)
                 (>! out-chan message)
                 (when auto-delete
                   ;; FIXME: Assign :done-fn to be processed outside afterwards.
                   (sqs-ext/delete-message sqs-client queue-url message)))))

           (catch Exception e
             (log/errorf e "Failed receiving message for %s" (:stats @loop-state))))

         (if (:running @loop-state)
           (recur)
           (log/warnf "Receive-loop terminated for %s" (:stats @loop-state))))

       stop-loop))))


(defn handle-queue
  ([sqs-client queue-url handler-fn]
   (handle-queue sqs-client queue-url handler-fn {}))
  ([sqs-client queue-url handler-fn
    {:keys [num-handler-threads
            auto-delete]
     :or   {num-handler-threads 4
            auto-delete         true}}]
   (log/infof (str "Starting receive loop for %s with:\n"
                   "  num-handler-threads: %d\n"
                   "  auto-delete: %s")
              queue-url num-handler-threads auto-delete)
   (let [receive-chan (chan)
         stop-fn (receive-loop sqs-client
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
     stop-fn)))

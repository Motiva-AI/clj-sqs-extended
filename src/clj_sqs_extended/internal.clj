(ns clj-sqs-extended.internal
  (:require [clojure.core.async :refer [chan go-loop close! timeout <! >!]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
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
   (let [queue-url (sqs/queue-name-to-url sqs-ext-client
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
                 (close! messages-chan)))

             (stop-loop
               []
               (when (:running @loop-state)
                 (log/infof "Terminating receive-loop for queue '%s'" queue-name)
                 (swap! loop-state assoc :running false)
                 (close! (:messages @loop-state))
                 (close! out-chan))
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
                 (<! (timeout (int (* restart-delay-seconds 1000))))
                 (restart-loop))

               (not (empty? message))
               (let [done-fn #(sqs/delete-message sqs-ext-client queue-name message)
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

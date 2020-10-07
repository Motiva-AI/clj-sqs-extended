(ns clj-sqs-extended.channeled
  (:require [clj-sqs-extended.receive]

            [clojure.core.async :as async :refer [chan go go-loop <! >!]]
            [clojure.core.async.impl.protocols :as async-protocols]))

(defn receive-to-channel
  [receive-fn
   {max-number-of-receiving-messages-per-batch :max-number-of-receiving-messages-per-batch
    :or {max-number-of-receiving-messages-per-batch 10}}]
  (let [ch (chan max-number-of-receiving-messages-per-batch)]
    (go
      (try
        (loop []
          (let [messages (receive-fn)]
            (when-not (empty? messages)
              (async/onto-chan ch messages false)))
          (when-not (async-protocols/closed? ch)
            (recur)))
        (catch Throwable e
          (>! ch e)))

      ;; close channel when exiting loop
      (async/close! ch))

    ch))

(defn receive-loop
  [sqs-ext-client queue-url out-chan
   receive-fn
   {:keys [auto-delete
           restart-delay-seconds
           restart-limit]
    :as   receive-opts}]
  (let [receive-loop-running? (atom true)]
    (go-loop
      [loop-stats        (init-receive-loop-stats)
       ;; start listening on queue
       receiving-chan    (receive-to-channel
                           receive-fn
                           receive-opts)
       pause-and-restart-for-error? (atom false)]

      (->> (<! receiving-chan)
           (handle-unexpected-message
             queue-url
             loop-stats
             out-chan
             receive-loop-running?
             pause-and-restart-for-error?
             receive-opts)
           (put-legit-message-to-out-chan-and-maybe-delete-message
             {:sqs-ext-client sqs-ext-client
              :queue-url      queue-url
              :out-chan       out-chan
              :auto-delete?   auto-delete}))

      (if @receive-loop-running?
        (recur (update-receive-loop-stats loop-stats @pause-and-restart-for-error?)
               (next-receiving-chan
                 receiving-chan
                 #(channeled/receive-to-channel
                    sqs-ext-client
                    queue-url
                    receive-opts)
                 @pause-and-restart-for-error?)
               (atom false))
        (exit-receive-loop! queue-url loop-stats receiving-chan)))

    ;; returns a stop-fn
    (partial stop-receive-loop! queue-url out-chan receive-loop-running?)))


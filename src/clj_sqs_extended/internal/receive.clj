(ns clj-sqs-extended.internal.receive
  (:require [clojure.core.async :as async :refer [chan go-loop close! <! <!! >!!]]
            [clojure.core.async.impl.protocols :as async-protocols]

            [clojure.tools.logging :as log]
            [tick.core :as t]))

(defn- seconds-between
  [t1 t2]
  (t/seconds (t/between t1 t2)))

(defn- init-receive-loop-stats []
  {:iteration     0
   :restart-count 0
   :started-at    (t/now)})

(defn- update-receive-loop-stats
  [loop-stats pause-and-restart-for-error?]
  (let [now (t/now)]
    (-> loop-stats
        (update :iteration inc)
        ;; ensure that :last-loop-duration-in-seconds is updated before
        ;; updating :last-iteration-started-at
        (assoc :last-loop-duration-in-seconds
               (seconds-between
                 (or (-> loop-stats :last-iteration-started-at)
                     (-> loop-stats :started-at))
                 now))
        (assoc :last-iteration-started-at now)

        (cond-> pause-and-restart-for-error? (update :restart-count inc)))))

(defn stop-receive-loop!
  [queue-url out-chan ^clojure.lang.Atom receive-loop-running?]
  (log/infof "Stopping receive-loop for %s, watch for terminating message coming..." queue-url)
  (reset! receive-loop-running? false)
  (close! out-chan))

(defn exit-receive-loop!
  [queue-url loop-stats receiving-chan]
  (log/errorf "Receive-loop terminated for %s, stats: %s" queue-url loop-stats)
  (close! receiving-chan)

  loop-stats)

(defn- restart-limit-reached?
  [loop-state limit]
  (when limit
    (> (:restart-count loop-state)
       limit)))

(defn- raise-receive-loop-error [queue-url error]
  (throw (ex-info
           (format "receive-loop for queue '%s' failed." queue-url)
           {:error error})))

(defn- continue-after-message-receival-error?
  [loop-state restart-limit]
  (not (restart-limit-reached? loop-state restart-limit)))

(defn assoc-done-fn-to-message
  [message-delete-fn
   {receipt-handle :receiptHandle :as message}]
  (assoc message
         :done-fn
         ; delete-message on the AWS SDK can block for many seconds. This fn runs
         ; sqs/delete-message! in the background.
         ;
         ; Reference:
         ; https://www.selikoff.net/2018/02/14/the-amazon-aws-java-sqs-client-not-thread-safe/
         #(async/thread (message-delete-fn receipt-handle))))

(defn put-legit-message-to-out-chan
  [queue-url out-chan message]
  (if (:body message)
    (when-not (>!! out-chan message)
      ;; TODO refactor this fn's logic so that we don't need to throw an exception to stop auto-delete

      ;; throwing an exception here to stop the process so that we don't
      ;; delete the current received message without putting it to out-chan
      (throw (ex-info
               (format "Failed to put message to out-chan because the channel is closed already. Queue %s"
                       queue-url)
               {})))

    ;; Question: do we need to delete a nil message?
    (log/infof "Queue '%s' received a nil (:body message), message: %s"
               queue-url
               message))

  ;; pass along
  message)

(defn delete-message-if-auto-delete
  [auto-delete?
   {done-fn :done-fn :as message}]
  (when (and auto-delete? (fn? done-fn))
    (done-fn))

  ;; pass along
  message)

(defn pause-to-recover-this-loop
  [queue-url
   ^clojure.lang.Atom pause-and-restart-for-error?
   restart-delay-seconds
   error]
  (log/infof "Recovering from error '%s'... Waiting [%d] seconds before continuing receive-loop for queue '%s' ..."
             (.getMessage error)
             restart-delay-seconds
             queue-url)
  (reset! pause-and-restart-for-error? true)
  (Thread/sleep (* 1000 restart-delay-seconds)))

(defn- handle-message-exception-and-maybe-pause-this-loop
  [queue-url
   loop-stats
   out-chan
   ^clojure.lang.Atom receive-loop-running?
   ^clojure.lang.Atom pause-and-restart-for-error?
   {restart-delay-seconds :restart-delay-seconds
    restart-limit         :restart-limit
    :or {restart-delay-seconds 1}}
   error]
  (if-not (continue-after-message-receival-error? loop-stats restart-limit)
    (do
      (stop-receive-loop! queue-url out-chan receive-loop-running?)
      (raise-receive-loop-error queue-url error))

    (pause-to-recover-this-loop queue-url
                                pause-and-restart-for-error?
                                restart-delay-seconds
                                error)))

(defn handle-unexpected-message
  [queue-url
   loop-stats
   out-chan
   ^clojure.lang.Atom receive-loop-running?
   ^clojure.lang.Atom pause-and-restart-for-error?
   restart-opts
   message]
  (cond
    (nil? message)                (stop-receive-loop! queue-url out-chan receive-loop-running?)
    ;; TODO refactor out stop-receive-loop! call from handle-message-exception-and-maybe-pause-this-loop
    (instance? Throwable message) (handle-message-exception-and-maybe-pause-this-loop
                                    queue-url
                                    loop-stats
                                    out-chan
                                    receive-loop-running?
                                    pause-and-restart-for-error?
                                    restart-opts
                                    message)
    :else message))

(defn- restart-receiving-chan?
  [^java.lang.Boolean pause-and-restart-for-error?]
  pause-and-restart-for-error?)

(defn- next-receiving-chan
  [current-receiving-chan
   create-new-receiving-chan-fn
   ^java.lang.Boolean pause-and-restart-for-error?]
  (if (restart-receiving-chan? pause-and-restart-for-error?)
    (create-new-receiving-chan-fn)
    current-receiving-chan))

(defn receive-to-channel
  [message-receiver-fn]
  (let [ch (chan)]
    (async/thread ;; do not use a go-block here because https://eli.thegreenplace.net/2017/clojure-concurrency-and-blocking-with-coreasync/
      (try
        (loop []
          (let [messages (message-receiver-fn)]
            (when (seq messages)
              (<!! (async/onto-chan! ch messages false))))

          (when-not (async-protocols/closed? ch)
            (recur)))
        (catch Throwable e
          (>!! ch e)))

      ;; close channel when exiting loop
      (async/close! ch))

    ch))

(defn receive-loop
  [queue-url
   out-chan
   message-receiver-fn
   message-delete-fn
   {:keys [auto-delete?
           restart-opts]}]

  (let [receive-loop-running?        (atom true)
        create-new-receiving-chan-fn #(receive-to-channel message-receiver-fn)]

    (go-loop
      [loop-stats        (init-receive-loop-stats)
       receiving-chan    (create-new-receiving-chan-fn)
       pause-and-restart-for-error? (atom false)]

      (some->> (<! receiving-chan)
               (handle-unexpected-message
                 queue-url
                 loop-stats
                 out-chan
                 receive-loop-running?
                 pause-and-restart-for-error?
                 restart-opts)
               (assoc-done-fn-to-message message-delete-fn)
               (put-legit-message-to-out-chan queue-url out-chan)
               (delete-message-if-auto-delete auto-delete?))

      (if @receive-loop-running?
        (recur (update-receive-loop-stats loop-stats @pause-and-restart-for-error?)
               (next-receiving-chan
                 receiving-chan
                 create-new-receiving-chan-fn
                 @pause-and-restart-for-error?)
               (atom false))
        (exit-receive-loop! queue-url loop-stats receiving-chan)))

    ;; returns a stop-fn
    (partial stop-receive-loop! queue-url out-chan receive-loop-running?)))


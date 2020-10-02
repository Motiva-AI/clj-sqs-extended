(ns clj-sqs-extended.internal.receive
  (:require [clojure.core.async :refer [go-loop close! <! >!!]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.aws.sqs :as sqs])
  (:import [java.net
            SocketException
            UnknownHostException]
           [java.net.http HttpTimeoutException]))

(defn error-might-be-recovered-by-restarting?
  [error]
  (contains? #{UnknownHostException
               SocketException
               HttpTimeoutException} (type error)))

(defn- seconds-between
  [t1 t2]
  (t/seconds (t/between t1 t2)))

(defn- init-receive-loop-stats []
  {:iteration     0
   :restart-count 0
   :started-at    (t/now)})

(defn- update-receive-loop-stats [loop-stats]
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
        (assoc :last-iteration-started-at now))))

(defn- stop-receive-loop!
  [receive-loop-running?]
  (reset! receive-loop-running? false))

(defn- loop-stats-increment-restart-count
  [loop-state]
  (-> loop-state
      (update :restart-count inc)
      (assoc :restarted-at (t/now))))

(defn- restart-limit-reached?
  [loop-state limit]
  (> (get-in @loop-state [:stats :restart-count])
     limit))

(defn- raise-receive-loop-error [queue-url error]
  (throw (ex-info
           (format "receive-loop for queue '%s' failed." queue-url)
           {:error error})))

(defn message-receival-error-safe-to-continue?
  [error loop-state {restart-limit :restart-limit}]
  (and (error-might-be-recovered-by-restarting? error)
       (not (restart-limit-reached? loop-state restart-limit))))

(defn- put-message-to-out-chan-and-maybe-delete
  [{sqs-ext-client :sqs-ext-client
    queue-url      :queue-url
    message        :message
    out-chan       :out-chan
    auto-delete?   :auto-delete?}]
  (let [done-fn #(sqs/delete-message! sqs-ext-client queue-url message)
        msg (cond-> message
              (not auto-delete?) (assoc :done-fn done-fn))]
    (if (:body message)
      (>!! out-chan msg)

      (log/infof "Queue '%s' received a nil (:body message), message: %s"
                 queue-url
                 message))

    (when auto-delete?
      (done-fn))))

(defn exit-receive-loop
  [queue-url receiving-chan out-chan loop-stats]
  (log/infof "Receive-loop terminated for %s, stats: %s" queue-url loop-stats)
  (close! receiving-chan)
  (close! out-chan)
  loop-stats)

(defn receive-loop
  ([sqs-ext-client queue-url out-chan]
   (receive-loop sqs-ext-client queue-url out-chan {}))

  ([sqs-ext-client queue-url out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :as   receive-opts}]
   (let [receive-loop-running? (atom true)]
     (go-loop [loop-stats     (init-receive-loop-stats)
               receiving-chan (sqs/receive-to-channel sqs-ext-client
                                                      queue-url
                                                      receive-opts)]
       (let [message (<! receiving-chan)]
         (cond
           (nil? message)
           (stop-receive-loop! receive-loop-running?)

           (instance? Throwable message)
           ;; refactor out this handle-message-receival-error block
           (if (message-receival-error-safe-to-continue? message loop-stats receive-opts)
             (do
               (log/infof "Recovering from error '%s'... Waiting %d seconds and then restarting receive-loop for queue '%s' ..."
                          (.getMessage message)
                          restart-delay-seconds
                          queue-url)
               (Thread/sleep (* 1000 restart-delay-seconds))

               (close! receiving-chan)
               (recur (loop-stats-increment-restart-count loop-stats)
                      (sqs/receive-to-channel sqs-ext-client
                                              queue-url
                                              receive-opts)))

             (raise-receive-loop-error queue-url message))


           (seq message)
           (put-message-to-out-chan-and-maybe-delete
             {:sqs-ext-client sqs-ext-client
              :queue-url      queue-url
              :out-chan       out-chan
              :message        message
              :auto-delete?   auto-delete})))

       (if @receive-loop-running?
         (recur (update-receive-loop-stats loop-stats) receiving-chan)

         (exit-receive-loop queue-url receiving-chan out-chan loop-stats)))

     (partial stop-receive-loop! receive-loop-running?))))

(ns clj-sqs-extended.internal.receive
  (:require [clojure.core.async :refer [go-loop close! <! >!!]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.aws.sqs :as sqs])
  (:import [java.net
            SocketException
            UnknownHostException]
           [java.net.http HttpTimeoutException]))

(defn error-is-safe-to-continue?
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

(defn- update-receive-loop-stats
  [loop-stats paused-for-error?]
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

        (cond-> paused-for-error? (update :restart-count inc)))))

(defn stop-receive-loop!
  [queue-url receiving-chan out-chan ^clojure.lang.Atom receive-loop-running?]
  (log/infof "Stopping receive-loop for %s ..." queue-url)
  (reset! receive-loop-running? false)
  (close! receiving-chan)
  (close! out-chan))

(defn- exit-receive-loop!
  [queue-url loop-stats]
  (log/infof "Receive-loop terminated for %s, stats: %s" queue-url loop-stats)
  loop-stats)

(defn- restart-limit-reached?
  [loop-state limit]
  (> (:restart-count loop-state)
     limit))

(defn- raise-receive-loop-error [queue-url error]
  (throw (ex-info
           (format "receive-loop for queue '%s' failed." queue-url)
           {:error error})))

(defn- message-receival-error-safe-to-continue?
  [loop-state restart-limit error]
  (and (error-is-safe-to-continue? error)
       (not (restart-limit-reached? loop-state restart-limit))))

(defn put-legit-message-to-out-chan-and-maybe-delete-message
  [{sqs-ext-client :sqs-ext-client
    queue-url      :queue-url
    out-chan       :out-chan
    auto-delete?   :auto-delete?}
   message]
  (when message
    (let [done-fn #(sqs/delete-message! sqs-ext-client queue-url message)
          msg (cond-> message
                (not auto-delete?) (assoc :done-fn done-fn))]
      (if (:body message)
        (>!! out-chan msg)

        (log/infof "Queue '%s' received a nil (:body message), message: %s"
                   queue-url
                   message))

      ;; TODO refactor this out to its own fn
      (when auto-delete?
        (done-fn)))))

(defn pause-to-recover-this-loop
  [queue-url
   ^clojure.lang.Atom paused-for-error?
   restart-delay-seconds
   error]
  (log/infof "Recovering from error '%s'... Waiting [%d] seconds before continuing receive-loop for queue '%s' ..."
             (.getMessage error)
             restart-delay-seconds
             queue-url)
  (reset! paused-for-error? true)
  (Thread/sleep (* 1000 restart-delay-seconds)))

(defn- handle-message-exception-and-maybe-pause-this-loop
  [queue-url
   loop-stats
   receiving-chan
   out-chan
   ^clojure.lang.Atom receive-loop-running?
   ^clojure.lang.Atom paused-for-error?
   ;; TODO rename these to pause-*
   {restart-delay-seconds :restart-delay-seconds
    restart-limit         :restart-limit}
   error]
  (if-not (message-receival-error-safe-to-continue? loop-stats restart-limit error)
    (do
      (stop-receive-loop! queue-url receiving-chan out-chan receive-loop-running?)
      (raise-receive-loop-error queue-url error))

    (pause-to-recover-this-loop queue-url
                                paused-for-error?
                                restart-delay-seconds
                                error)))

(defn handle-unexpected-message
  [queue-url
   loop-stats
   receiving-chan
   out-chan
   ^clojure.lang.Atom receive-loop-running?
   ^clojure.lang.Atom paused-for-error?
   receive-opts
   message]
  (cond
    (nil? message)                (stop-receive-loop! queue-url receiving-chan out-chan receive-loop-running?)
    ;; TODO refactor out stop-receive-loop! call from handle-message-exception-and-maybe-pause-this-loop
    (instance? Throwable message) (handle-message-exception-and-maybe-pause-this-loop
                                    queue-url
                                    loop-stats
                                    receiving-chan
                                    out-chan
                                    receive-loop-running?
                                    paused-for-error?
                                    receive-opts
                                    message)
    :else message))

;; TODO rename this to e.g. process-message-loop
(defn receive-loop
  ([sqs-ext-client queue-url out-chan]
   (receive-loop sqs-ext-client queue-url out-chan {}))

  ([sqs-ext-client queue-url out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :as   receive-opts}]
   (let [receive-loop-running? (atom true)

         ;; start listening on queue
         receiving-chan (sqs/receive-to-channel
                          sqs-ext-client
                          queue-url
                          receive-opts)]
     (go-loop
       [loop-stats        (init-receive-loop-stats)
        paused-for-error? (atom false)]

       (->> (<! receiving-chan)
            (handle-unexpected-message
              queue-url
              loop-stats
              receiving-chan
              out-chan
              receive-loop-running?
              paused-for-error?
              receive-opts)
            (put-legit-message-to-out-chan-and-maybe-delete-message
              {:sqs-ext-client sqs-ext-client
               :queue-url      queue-url
               :out-chan       out-chan
               :auto-delete?   auto-delete}))

       (if @receive-loop-running?
         (recur (update-receive-loop-stats loop-stats @paused-for-error?) (atom false))
         (exit-receive-loop! queue-url loop-stats)))

     ;; returns a stop-fn
     (partial stop-receive-loop! queue-url receiving-chan out-chan receive-loop-running?))))


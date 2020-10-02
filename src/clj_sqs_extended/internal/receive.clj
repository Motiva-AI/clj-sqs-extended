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

(defn- init-receive-loop-state
  [sqs-ext-client queue-url receive-opts]
  (log/debugf "Initializing new receive-loop state for queue '%s' ..." queue-url)
  (atom {:running   true
         :stats     {:iteration     0
                     :restart-count 0
                     :started-at    (t/now)}
         :in-chan   (sqs/receive-to-channel sqs-ext-client
                                            queue-url
                                            receive-opts)}))

(defn- update-receive-loop-stats
  [loop-state]
  (let [now (t/now)]
    (-> loop-state
        (update-in [:stats :iteration] inc)
        ;; ensure that :last-loop-duration-in-seconds is updated before
        ;; updating :last-iteration-started-at
        (assoc-in [:stats :last-loop-duration-in-seconds]
                  (seconds-between
                    (or (-> loop-state :stats :last-iteration-started-at)
                        (-> loop-state :stats :started-at))
                    now))
        (assoc-in [:stats :last-iteration-started-at] now))))

(defn- stop-receive-loop
  [queue-url out-chan loop-state]
  (when (:running @loop-state)
    (log/debugf "Terminating receive-loop for queue '%s' ..." queue-url)
    (swap! loop-state assoc :running false)
    (swap! loop-state assoc-in [:stats :stopped-at] (t/now))
    (close! (:in-chan @loop-state))
    (close! out-chan))
  (:stats @loop-state))

(defn- restart-receive-loop
  [sqs-ext-client queue-url restart-delay-seconds loop-state receive-opts]
  (log/infof "Waiting %d seconds and then restarting receive-loop for queue '%s' ..."
             restart-delay-seconds
             queue-url)
  (Thread/sleep (* 1000 restart-delay-seconds))

  (let [old-in-chan (:in-chan @loop-state)]
    (swap! loop-state
           (fn [state]
             (-> state
                 (assoc :in-chan
                        (sqs/receive-to-channel sqs-ext-client queue-url receive-opts))
                 (update-in [:stats :restart-count] inc)
                 (assoc-in [:stats :restarted-at] (t/now)))))
    (close! old-in-chan)))

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

(defn handle-message-receival-error
  [sqs-ext-client queue-url loop-state error restart-delay-seconds receive-opts]
  (if (message-receival-error-safe-to-continue? error loop-state receive-opts)
    (do
      (log/infof "Restarting receive-loop to recover from error: '%s'"
                 (.getMessage error))
      (restart-receive-loop sqs-ext-client
                            queue-url
                            restart-delay-seconds
                            loop-state
                            receive-opts))

    (raise-receive-loop-error queue-url error)))

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

(defn receive-loop
  ([sqs-ext-client queue-url out-chan]
   (receive-loop sqs-ext-client queue-url out-chan {}))

  ([sqs-ext-client queue-url out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :as   receive-opts}]
   (let [loop-state (init-receive-loop-state sqs-ext-client
                                             queue-url
                                             receive-opts)]
     (go-loop []
       (swap! loop-state update-receive-loop-stats)

       (let [message (<! (:in-chan @loop-state))]
         (cond
           (nil? message)
           (stop-receive-loop queue-url out-chan loop-state)

           (instance? Throwable message)
           (handle-message-receival-error
             sqs-ext-client
             queue-url
             loop-state
             message
             restart-delay-seconds
             receive-opts)

           (seq message)
           (put-message-to-out-chan-and-maybe-delete
             {:sqs-ext-client sqs-ext-client
              :queue-url      queue-url
              :out-chan       out-chan
              :message        message
              :auto-delete?   auto-delete})))

       (if (:running @loop-state)
         (recur)
         (log/infof "Receive-loop terminated for %s, stats: %s"
                    queue-url
                    (:stats @loop-state))))

     (partial stop-receive-loop queue-url out-chan loop-state))))

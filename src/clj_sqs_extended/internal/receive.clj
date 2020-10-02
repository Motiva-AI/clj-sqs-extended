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
         :queue-url queue-url
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
  [out-chan loop-state]
  (when (:running @loop-state)
    (log/debugf "Terminating receive-loop for queue '%s' ..."
                (:queue-url @loop-state))
    (swap! loop-state assoc :running false)
    (swap! loop-state assoc-in [:stats :stopped-at] (t/now))
    (close! (:in-chan @loop-state))
    (close! out-chan))
  (:stats @loop-state))

(defn- restart-receive-loop
  [sqs-ext-client loop-state receive-opts]
  (log/infof "Restarting receive-loop for queue '%s' ..."
             (:queue-url @loop-state))
  (let [old-in-chan (:in-chan @loop-state)]
    (swap! loop-state
           (fn [state]
             (-> state
                 (assoc :in-chan
                        (sqs/receive-to-channel sqs-ext-client
                                                (:queue-url @loop-state)
                                                receive-opts))
                 (update-in [:stats :restart-count] inc)
                 (assoc-in [:stats :restarted-at] (t/now)))))
    (close! old-in-chan)))

(defn- restart-limit-reached?
  [loop-state limit]
  (> (get-in @loop-state [:stats :restart-count])
     limit))

(def ^:private restart-limit-not-reached?
  (complement restart-limit-reached?))

(defn- handle-message-receival-error
  [sqs-ext-client loop-state error restart-delay-seconds receive-opts]
  (let [restart-limit (:restart-limit receive-opts)]
    (letfn [(raise-error []
              (throw (ex-info
                       (format "receive-loop for queue '%s' failed."
                               (:queue-url @loop-state))
                       {:error error})))]

      (cond
        (and (error-might-be-recovered-by-restarting? error)
             (restart-limit-not-reached? loop-state restart-limit))
        (do
          (Thread/sleep (* 1000 restart-delay-seconds))
          (log/infof "Restarting receive-loop to recover from error: '%s'"
                     (.getMessage error))
          (restart-receive-loop sqs-ext-client
                                loop-state
                                receive-opts))

        (and (error-might-be-recovered-by-restarting? error)
             (restart-limit-reached? loop-state restart-limit))
        (raise-error)

        (not (error-might-be-recovered-by-restarting? error))
        (raise-error)))))

(defn- put-message-to-out-chan-and-maybe-delete
  [{sqs-ext-client :sqs-ext-client
    queue-url      :queue-url
    message        :message
    out-chan       :out-chan
    auto-delete?   :auto-delete?}]
  (let [done-fn #(sqs/delete-message! sqs-ext-client
                                      queue-url
                                      message)
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
           (stop-receive-loop out-chan loop-state)

           (instance? Throwable message)
           (handle-message-receival-error sqs-ext-client
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

     (partial stop-receive-loop out-chan loop-state))))

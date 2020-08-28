(ns clj-sqs-extended.internal.receive
  (:require [clojure.core.async :refer [go go-loop close! <! >!]]
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

(defn- secs-between
  [t1 t2]
  (t/seconds (t/between t1 t2)))

(defn- init-receive-loop-state
  [sqs-ext-client queue-name receive-opts out-chan]
  (log/infof "Initializing new receive-loop state for queue '%s' ..." queue-name)
  (atom {:running    true
         :stats      {:iteration     0
                      :restart-count 0
                      :started-at    (t/now)}
         :queue-name queue-name
         :in-chan    (sqs/receive-message-channeled sqs-ext-client
                                                    queue-name
                                                    receive-opts)
         :out-chan   out-chan}))

(defn- update-receive-loop-stats
  [loop-state]
  (let [now (t/now)]
    (-> loop-state
        (update-in [:stats :iteration] inc)
        (assoc-in [:stats :this-pass-started-at] now)
        (assoc-in [:stats :loop-duration]
                  (secs-between (-> loop-state :stats :started-at)
                                now)))))

(defn- stop-receive-loop
  [loop-state]
  (when (:running @loop-state)
    (log/warnf "Terminating receive-loop for queue '%s' ..."
               (:queue-name @loop-state))
    (swap! loop-state assoc :running false)
    (swap! loop-state assoc-in [:stats :stopped-at] (t/now))
    (close! (:in-chan @loop-state))
    (close! (:out-chan @loop-state)))
  (:stats @loop-state))

(defn- restart-receive-loop
  [sqs-ext-client loop-state receive-opts]
  (log/infof "Restarting receive-loop for queue '%s' ..."
             (:queue-name @loop-state))
  (let [old-in-chan (:in-chan @loop-state)]
    (swap! loop-state
           (fn [state]
             (-> state
                 (assoc :in-chan
                        (sqs/receive-message-channeled sqs-ext-client
                                                       (:queue-name @loop-state)
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
                               (:queue-name @loop-state))
                       {:reason (.getMessage error)})))]
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

(defn- process-message
  [sqs-ext-client loop-state message auto-delete]
  (let [done-fn #(sqs/delete-message sqs-ext-client
                                     (:queue-name @loop-state)
                                     message)
        msg (cond-> message
                    (not auto-delete) (assoc :done-fn done-fn))]
    (if (:body message)
      (go (>! (:out-chan @loop-state) msg))
      (log/warnf "Queue '%s' received a nil body message: %s"
                 (:queue-name @loop-state)
                 message))
    (when auto-delete
      (done-fn))))

(defn receive-loop
  ([sqs-ext-client queue-name out-chan]
   (receive-loop sqs-ext-client queue-name out-chan {}))

  ([sqs-ext-client queue-name out-chan
    {:keys [auto-delete
            restart-delay-seconds]
     :as   receive-opts}]
   (let [loop-state (init-receive-loop-state sqs-ext-client
                                             queue-name
                                             receive-opts
                                             out-chan)]
     (go-loop []
       (swap! loop-state update-receive-loop-stats)

       (let [message (<! (:in-chan @loop-state))]
         (cond
           (nil? message)
           (stop-receive-loop loop-state)

           (instance? Throwable message)
           (handle-message-receival-error sqs-ext-client
                                          loop-state
                                          message
                                          restart-delay-seconds
                                          receive-opts)

           (seq message)
           (process-message sqs-ext-client
                            loop-state
                            message
                            auto-delete)))

       (if (:running @loop-state)
         (recur)
         (log/infof "Receive-loop terminated for %s"
                    (:stats @loop-state))))

     (partial stop-receive-loop loop-state))))

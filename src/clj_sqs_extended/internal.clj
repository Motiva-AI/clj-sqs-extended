(ns clj-sqs-extended.internal
  (:require [clojure.core.async :refer [chan go go-loop close! timeout <! >!]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.sqs :as sqs]))


(defn- secs-between
  [t1 t2]
  (t/seconds (t/between t1 t2)))

(defn- init-receive-loop-state
  [sqs-ext-client queue-name receive-opts out-chan]
  (log/infof "Initializing new receive-loop state for queue '%s'" queue-name)
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

(defn- restart-receive-loop
  [sqs-ext-client loop-state receive-opts]
  (log/infof "Restarting receive-loop for queue '%s'" (:queue-name @loop-state))
  (let [old-in-chan (:in-chan @loop-state)]
    (swap! loop-state
           (fn [state]
             (-> state
                 (assoc :in-chan (sqs/receive-message-channeled sqs-ext-client
                                                                (:queue-name @loop-state)
                                                                receive-opts))
                 (update-in [:stats :restart-count] inc)
                 (assoc-in [:stats :restarted-at] (t/now)))))
    (close! old-in-chan)))

(defn- stop-receive-loop
  [loop-state]
  (when (:running @loop-state)
    (log/infof "Terminating receive-loop for queue '%s'" (:queue-name @loop-state))
    (swap! loop-state assoc :running false)
    (swap! loop-state assoc-in [:stats :stopped-at] (t/now))
    (close! (:in-chan @loop-state))
    (close! (:out-chan @loop-state))
    (:stats @loop-state)))

(defn- handle-message-receival-error
  [sqs-ext-client loop-state error restart-delay-seconds receive-opts]
  (let [{:keys [this-pass-started-at] :as stats} (:stats @loop-state)]
    (log/warn error "Received an error!"
              (assoc stats :last-wait-duration
                           (secs-between this-pass-started-at
                                         (t/now))))
    ;; WATCHOUT: Adding a restart delay so that this doesn't go into an
    ;;           abusively tight loop if the queue listener is failing to
    ;;           start continuously.
    (<! (timeout (int (* restart-delay-seconds 1000))))
    (restart-receive-loop sqs-ext-client
                          loop-state
                          receive-opts)))

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
            restart-delay-seconds
            format
            num-consumers]
     :or   {auto-delete           true
            restart-delay-seconds 1
            format                :transit
            num-consumers         1}
     :as   receive-opts}]
   (let [loop-state (init-receive-loop-state sqs-ext-client
                                             queue-name
                                             receive-opts
                                             out-chan)]
     (go-loop []
       (swap! loop-state update-receive-loop-stats)

       (try
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

             (not (empty? message))
             (process-message sqs-ext-client
                              loop-state
                              message
                              auto-delete)))

         (catch Exception e
           (log/errorf e "Failed receiving message for %s" (:stats @loop-state))))

       (if (:running @loop-state)
         (recur)
         (log/warnf "Receive-loop terminated for %s" (:stats @loop-state))))

     (partial stop-receive-loop loop-state))))

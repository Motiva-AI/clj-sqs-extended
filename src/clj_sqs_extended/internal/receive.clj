(ns clj-sqs-extended.internal.receive
  (:require [clojure.core.async :refer [chan go go-loop close! timeout <! >!]]
            [clojure.tools.logging :as log]
            [tick.alpha.api :as t]
            [clj-sqs-extended.aws.sqs :as sqs]))


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
    (close! (:out-chan @loop-state))
    (:stats @loop-state)))

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

(defn- error-might-be-recovered-by-restarting
  [error]
  ;; TODO: Depending on the type of error return true if we think we can
  ;;       recover from this, or false if its game over.
  true)

(defn- handle-message-receival-error
  [sqs-ext-client loop-state error restart-delay-seconds receive-opts]
  (log/warnf error "Error while receiving message:")
  (if (error-might-be-recovered-by-restarting error)
    (let [count (get-in @loop-state [:stats :restart-count])]
      (if (< count (:restart-limit receive-opts))
        (do
          (Thread/sleep (* 1000 restart-delay-seconds))
          (restart-receive-loop sqs-ext-client
                                loop-state
                                receive-opts))
        (log/warnf "Skipping receive-loop restart because the limit (%d) has been reached!"
                    count)))
    (do
      (log/fatalf "An unrecovereable error occured!")
      (stop-receive-loop loop-state))))

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
            restart-limit
            format
            num-consumers]
     :or   {auto-delete           true
            restart-delay-seconds 1
            restart-limit         10
            format                :transit
            num-consumers         1}
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

           (not (empty? message))
           (process-message sqs-ext-client
                            loop-state
                            message
                            auto-delete)))

       (if (:running @loop-state)
         (recur)
         (log/infof "Receive-loop terminated for %s" (:stats @loop-state))))

     (partial stop-receive-loop loop-state))))
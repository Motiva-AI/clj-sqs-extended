(ns clj-sqs-extended.internal.troublemaker)


(defonce ^:private active-troublemakers (atom {}))

(defn activate-troublemaker
  [id]
  (swap! active-troublemakers assoc id :active))

(defn deactivate-troublemaker
  [id]
  (swap! active-troublemakers dissoc id))

(defn debug-troublemaker
  [id error]
  (when (contains? @active-troublemakers id)
    (throw error)))

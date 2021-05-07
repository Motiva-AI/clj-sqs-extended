(ns clj-sqs-extended.internal.serdes
  (:require [pipeline-transit.core :as transit]
            [clojure.data.json :as json]))

(defn- unsupported-format-exception
  [got]
  (ex-info (format "Only %s formats are supported. We received %s."
                   [:transit :json] got)
           {:cause :unsupported-serdes-format}))

(defn serialize
  [out format]
  (if out
    (cond
      (= format :transit) (transit/write-transit-json out)
      (= format :json) (json/write-str out)
      (= format :raw) out
      :else (throw (unsupported-format-exception format)))
    nil))

(defn deserialize
  [in format]
  (if in
    (cond
      (= format :transit) (transit/read-transit-json in)
      (= format :json) (json/read-str in :key-fn keyword)
      (= format :raw) in
      :else (throw (unsupported-format-exception format)))
    nil))

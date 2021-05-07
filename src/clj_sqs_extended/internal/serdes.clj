(ns clj-sqs-extended.internal.serdes
  (:require [pipeline-transit.core :as transit]
            [cheshire.core :as json]))

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
      (= format :json) (json/generate-string out)
      (= format :raw) out
      :else (throw (unsupported-format-exception format)))
    nil))

(defn deserialize
  [in format]
  (if in
    (cond
      (= format :transit) (transit/read-transit-json in)
      (= format :json) (json/parse-string in true)
      (= format :raw) in
      :else (throw (unsupported-format-exception format)))
    nil))

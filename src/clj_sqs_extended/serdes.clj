(ns clj-sqs-extended.serdes
  (:require [cognitect.transit :as transit]
            [tick.alpha.api :as t]
            [cheshire.core :as json])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))


(def ^:private tick-time-writer
  (transit/write-handler
    "tick-time"
    (fn [t] (.toString t))))

(def ^:private tick-time-reader
  (transit/read-handler
    (fn [t] (t/parse t))))

(defn- transit-write
  [out]
  (let [baos (ByteArrayOutputStream.)
        w (transit/writer baos
                          :json
                          {:handlers {(type (t/date-time)) tick-time-writer}})
        _ (transit/write w out)
        ret (.toString baos)]
    (.reset baos)
    ret))

(defn- transit-read
  [in]
  (-> (.getBytes in)
      (ByteArrayInputStream.)
      (transit/reader :json
                      {:handlers {"tick-time" tick-time-reader}})
      (transit/read)))

(defn serialize
  [out format]
  (cond
    (= format :transit) (transit-write out)
    (= format :json) (json/generate-string out)
    (= format :raw) out
    :else out))

(defn deserialize
  [in format]
  (cond
    (= format :transit) (transit-read in)
    (= format :json) (json/parse-string in true)
    (= format :raw) in
    :else in))


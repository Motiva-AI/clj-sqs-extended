(ns clj-sqs-extended.serdes
  ;; WATCHOUT: Check for future upstream improvements to use directly:
  ;;           https://github.com/henryw374/time-literals/issues/2
  (:require [cognitect.transit :as transit]
            [tick.alpha.api :as t]
            [cheshire.core :as json]
            [time-literals.read-write])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.time
            Period
            LocalDate
            LocalDateTime
            ZonedDateTime
            OffsetTime
            Instant
            OffsetDateTime
            ZoneId
            DayOfWeek
            LocalTime
            Month
            Duration
            Year
            YearMonth]))


(def ^:private time-classes
  {'period           Period
   'date             LocalDate
   'date-time        LocalDateTime
   'zoned-date-time  ZonedDateTime
   'offset-time      OffsetTime
   'instant          Instant
   'offset-date-time OffsetDateTime
   'time             LocalTime
   'duration         Duration
   'year             Year
   'year-month       YearMonth
   'zone             ZoneId
   'day-of-week      DayOfWeek
   'month            Month})

(def ^:private write-handlers
  {:handlers
   (into {}
         (for [[tick-class host-class] time-classes]
           [host-class (transit/write-handler (constantly (name tick-class)) str)]))})

(def ^:private read-handlers
  {:handlers
   (into {} (for [[sym fun] time-literals.read-write/tags]
              [(name sym) (transit/read-handler fun)]))})   ; omit "time/" for brevity

(defn- transit-write
  [arg]
  (let [out (ByteArrayOutputStream.)
        writer (transit/writer out :json write-handlers)]
    (transit/write writer arg)
    (.toString out)))

(defn- transit-read
  [json]
  (when json
    (let [in (ByteArrayInputStream. (.getBytes json))
          reader (transit/reader in :json read-handlers)]
      (transit/read reader))))

(defn- unsupported-format-exception
  [got]
  (ex-info (format "Only %s formats are supported. We received %s."
                   [:transit :json] got)
           {:cause :unsupported-serdes-format}))

(defn serialize
  [out format]
  (cond
    (= format :transit) (transit-write out)
    (= format :json) (json/generate-string out)
    (= format :raw) out
    :else (throw (unsupported-format-exception format))))

(defn deserialize
  [in format]
  (cond
    (= format :transit) (transit-read in)
    (= format :json) (json/parse-string in true)
    (= format :raw) in
    :else (throw (unsupported-format-exception format))))

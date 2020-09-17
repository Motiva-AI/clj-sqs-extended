(ns clj-sqs-extended.serdes-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-time.core :as clj-time]
            [clj-sqs-extended.internal.serdes :as serdes]
            [clj-sqs-extended.test-helpers :as helpers]))


(deftest roundtrip-transit-basic
  (testing "Transit roundtrip with a basic message"
    (let [message (helpers/random-message-basic)]
      (is (= message
             (serdes/deserialize
               (serdes/serialize message :transit)
               :transit))))))

(deftest roundtrip-transit-timestamp
  (testing "Transit roundtrip with a message including a timestamp"
    (let [message (helpers/random-message-with-time)]
      (is (= message
             (serdes/deserialize
               (serdes/serialize message :transit)
               :transit)))))

  (testing "Transit roundtrip with a message including a org.joda.time.DateTime"
    (let [message {:id        (rand-int 65535)
                   :timestamp (clj-time/now)}]
      (is (= message
             (-> message
                 (serdes/serialize :transit)
                 (serdes/deserialize :transit)))))))

(deftest nil-handled-properly
  (testing "(De)serializing nil"
    (is (= nil
           (serdes/deserialize
             (serdes/serialize nil :transit)
             :transit)))
    (is (= nil
           (serdes/deserialize
             (serdes/serialize nil :json)
             :json)))))

(deftest roundtrip-json-basic
  (testing "JSON roundtrip with a basic message"
    (let [message (helpers/random-message-basic)]
      (is (= message
             (serdes/deserialize
               (serdes/serialize message :json)
               :json))))))

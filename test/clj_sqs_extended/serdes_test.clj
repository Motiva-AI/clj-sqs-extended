(ns clj-sqs-extended.serdes-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-sqs-extended.serdes :as serdes]
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
               :transit))))))

(deftest roundtrip-json-basic
  (testing "JSON roundtrip with a basic message"
    (let [message (helpers/random-message-basic)]
      (is (= message
             (serdes/deserialize
               (serdes/serialize message :json)
               :json))))))

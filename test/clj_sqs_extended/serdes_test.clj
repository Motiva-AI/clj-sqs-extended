(ns clj-sqs-extended.serdes-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-sqs-extended.serdes :as serdes]
            [tick.alpha.api :as t]))


(def ^:private basic-map
  {:quote "State. You're doing it wrong."
   :by    "Rich Hickey"})

(def ^:private timestamp-map
  {:step      1
   :timestamp (t/inst)})

(deftest roundtrip-transit-basic-map
  (testing "Transit roundtrip with basic map"
    (is (= basic-map
           (serdes/deserialize
             (serdes/serialize basic-map :transit)
             :transit)))))

(deftest roundtrip-transit-with-timestamp
  (testing "Transit roundtrip with timestamp"
    (is (= timestamp-map
           (serdes/deserialize
             (serdes/serialize timestamp-map :transit)
             :transit)))))

(deftest roundtrip-json-basic-map
  (testing "JSON roundtrip with basic map"
    (is (= basic-map
           (serdes/deserialize
             (serdes/serialize basic-map :json)
             :json)))))

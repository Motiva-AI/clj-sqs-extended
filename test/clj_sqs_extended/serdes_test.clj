(ns clj-sqs-extended.serdes-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-sqs-extended.serdes :as serdes]
            [tick.alpha.api :as t]))


(def ^:private basic-map
  {:quote "State. You're doing it wrong."
   :by "Rich Hickey"})

(def ^:private timestamp-map
  {:step 1
   :timestamp (t/inst)})

(deftest roundtrip-transit
  (testing "Transit with basic map"
    (is (= basic-map
           (serdes/deserialize
             (serdes/serialize basic-map :transit)
             :transit))))
  (testing "Transit with timestamp"
    (is (= timestamp-map
           (serdes/deserialize
             (serdes/serialize timestamp-map :transit)
             :transit)))))

(deftest roundtrip-json
  (testing "JSON with basic map"
    (is (= basic-map
           (serdes/deserialize
             (serdes/serialize basic-map :json)
             :json)))))
(require '[circleci.test.report.junit :as junit])

{:test-results-dir "target/test_output"
 :reporters        [circleci.test.report/clojure-test-reporter
                    junit/reporter]

 :selectors {:acceptance  (fn [m] (or (:integration m) (:functional m)))
             :default     (complement :acceptance)
             :all         (constantly true)}}

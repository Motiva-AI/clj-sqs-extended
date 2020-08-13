(require '[circleci.test.report.junit :as junit])

{:test-results-dir "target/test_output"
 :reporters        [circleci.test.report/clojure-test-reporter
                    junit/reporter]

 :selectors {:default     (complement :integration)
             :integration :integration
             :all         (constantly true)}}

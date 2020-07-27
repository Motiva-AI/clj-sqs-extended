(require '[circleci.test.report.junit :as junit])

{:test-results-dir "target/test-results"
 :reporters        [circleci.test.report/clojure-test-reporter
                    junit/reporter]

 :selectors {:default     (complement :integration)
             :integration :integration
             :all         (constantly true)}}

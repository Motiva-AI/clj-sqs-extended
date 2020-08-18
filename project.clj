(defproject motiva/clj-sqs-extended "0.1.0-SNAPSHOT"
  :description "Clojure wrapper for https://github.com/awslabs/amazon-sqs-java-extended-client-lib"
  :url "https://github.com/Motiva-AI/clj-sqs-extended"

  :license {:name "MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.3.610"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.cognitect/transit-clj "1.0.324"]
                 [cheshire "5.10.0"]
                 [tick "0.4.26-alpha"]
                 [javax.xml.bind/jaxb-api "2.3.1"]
                 [com.amazonaws/amazon-sqs-java-extended-client-lib "1.0.2"]]

  :repl-options {:init-ns user
                 :timeout 120000}

  :profiles {:dev {:source-paths ["src" "dev/src"]
                   ;; "test" is included by default - adding it here confuses
                   ;; circleci.test which runs everything twice.
                   :test-paths []
                   :resource-paths ["dev/resources"]

                   :dependencies [[circleci/bond "0.4.0"]
                                  [circleci/circleci.test "0.4.3"]
                                  [org.clojure/tools.namespace "1.0.0"]
                                  [environ "1.2.0"]]

                   :plugins [[lein-environ "1.2.0"]]

                   :env {:aws-access-key-id          "default"
                         :aws-secret-access-key      "default"
                         :aws-sqs-endpoint-url       "http://localhost:4566"
                         :aws-sqs-region             "us-east-2"
                         :integration-aws-access-key ""
                         :integration-aws-secret-key ""}}}

  :aliases {"test"   ["run" "-m" "circleci.test/dir" :project/test-paths]
            "tests"  ["run" "-m" "circleci.test"]
            "retest" ["run" "-m" "circleci.test.retest"]})

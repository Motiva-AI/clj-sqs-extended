(defproject motiva/clj-sqs-extended "0.2.0-SNAPSHOT"
  :description "Clojure wrapper for https://github.com/awslabs/amazon-sqs-java-extended-client-lib"
  :url "https://github.com/Motiva-AI/clj-sqs-extended"

  :license {:name "MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.3.610"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.amazonaws/amazon-sqs-java-extended-client-lib "1.2.0"]
                 [com.cognitect/transit-clj "1.0.324"]
                 [cheshire "5.10.0"]
                 [tick "0.4.26-alpha"]]

  :repl-options {:init-ns user
                 :timeout 120000}

  :profiles {:dev {:source-paths ["src" "dev/src"]
                   ;; "test" is included by default - adding it here confuses
                   ;; circleci.test which runs everything twice.
                   :test-paths []
                   :resource-paths ["dev/resources"]

                   :dependencies [[org.clojure/tools.namespace "1.0.0"]
                                  [circleci/bond "0.4.0"]
                                  [circleci/circleci.test "0.4.3"]
                                  [environ "1.2.0"]]

                   :plugins [[lein-environ "1.2.0"]]

                   :env {:access-key             "default"
                         :secret-key             "default"
                         :sqs-endpoint           "http://localhost:4566"
                         :s3-endpoint            "http://localhost:4566"
                         :region                 "us-east-2"
                         :integration-access-key ""
                         :integration-secret-key ""}}

             :provided {:dependencies [[clj-time "0.15.2"]]}}

  :aliases {"test"   ["run" "-m" "circleci.test/dir" :project/test-paths]
            "tests"  ["run" "-m" "circleci.test"]
            "retest" ["run" "-m" "circleci.test.retest"]}

  :repositories [["releases" {:url           "https://clojars.org/repo"
                              :username      "motiva-ai"
                              :password      :env
                              :sign-releases false}]])

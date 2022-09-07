(defproject motiva/clj-sqs-extended "lein-git-inject/version"
  :description "Clojure wrapper for https://github.com/awslabs/amazon-sqs-java-extended-client-lib"
  :url "https://github.com/Motiva-AI/clj-sqs-extended"

  :license {:name "MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/core.async "1.5.648"]
                 [org.clojure/tools.logging "1.2.4"]
                 [com.amazonaws/amazon-sqs-java-extended-client-lib "1.2.0"]
                 [ai.motiva/pipeline-transit "0.1.0"]
                 [org.clojure/data.json "2.4.0"]
                 [tick "0.5.0"]]

  :repl-options {:init-ns user
                 :timeout 120000}

  :plugins [[lein-environ "1.2.0"]
            [day8/lein-git-inject "0.0.14"]]
  :middleware [leiningen.git-inject/middleware]

  :profiles {:dev {:source-paths ["src" "dev/src"]
                   ;; "test" is included by default - adding it here confuses
                   ;; circleci.test which runs everything twice.
                   :test-paths []
                   :resource-paths ["dev/resources"]

                   :dependencies [[org.clojure/clojure "1.11.1"]
                                  [org.clojure/tools.namespace "1.3.0"]
                                  [circleci/bond "0.6.0"]
                                  [circleci/circleci.test "0.5.0"]
                                  [environ "1.2.0"]]

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

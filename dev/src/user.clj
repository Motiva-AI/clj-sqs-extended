(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [circleci.test]))

;; Hotfix for https://github.com/clojure-emacs/orchard/issues/103 and
;; https://github.com/clojure-emacs/cider/issues/2686
;; Do not try to load source code from 'resources' directory
(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "src" "test")


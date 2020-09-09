(ns clj-sqs-extended.internal.core)


(defmacro macro-name-tbd
  [fn-name sqs-fn]
  `(let [meta# (meta (var ~sqs-fn))]
     (def ~fn-name
       #(apply ~sqs-fn
              (clj-sqs-extended.aws.sqs/sqs-ext-client %1)
              %&))
     (alter-meta! (var ~fn-name) merge meta#)))

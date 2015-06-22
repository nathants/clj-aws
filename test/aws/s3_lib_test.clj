(ns aws.s3-lib-test
  (:require [aws.s3-lib :as lib]
            [clojure.test :refer :all]))

(deftest test-prefixes
  (is (= ["a" "a/b" "a/b/c"]
         (lib/prefixes "a/b/c/d.csv"))))

(deftest test-indexed-keys
  (is (= {"a" ["a/b/c.csv"
               "a/d.csv"]
          "a/b" ["a/b/c.csv"]}
         (lib/indexed-keys ["a/b/c.csv"
                            "a/d.csv"]))))

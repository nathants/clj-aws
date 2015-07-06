(ns aws.s3-test
  (:require [clojure.test :refer :all]
            [aws.s3 :as s3]))

(deftest test-prefixes
  (is (= ["a" "a/b" "a/b/c"]
         (s3/-prefixes "a/b/c/d.csv"))))

(deftest test-indexed-keys
  (is (= {"a" ["a/b/c.csv"
               "a/d.csv"]
          "a/b" ["a/b/c.csv"]}
         (s3/-indexed-keys ["a/b/c.csv"
                            "a/d.csv"]))))

(deftest test-with-stubbed-s3
  (let [bucket "a-fake-bucket"
        key "a/fake/prefix/fake-file.csv"
        content "fake\ncontent"
        data {bucket {key content}}]
    (s3/with-stubbed-s3 data
      (is (= [key] (s3/list-keys nil bucket "a/fake/prefix")))
      (is (= content (s3/get-key-text nil bucket key))))))

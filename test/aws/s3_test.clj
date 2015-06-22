(ns aws.s3-test
  (:require [clojure.test :refer :all]
            [aws.s3 :as s3]
            [aws.s3-lib :as lib]))

(deftest test-with-stubbed-s3
  (let [bucket "a-fake-bucket"
        key "a/fake/prefix/fake-file.csv"
        content "fake\ncontent"
        data {bucket {key content}}]
    (s3/with-stubbed-s3 data
      (is (= [{:key key :size (lib/num-bytes content)}]
             (s3/list-keys nil bucket "a/fake/prefix")))
      (is (= content (s3/get-key-text nil bucket key))))))

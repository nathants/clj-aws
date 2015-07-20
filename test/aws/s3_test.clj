(ns aws.s3-test
  (:require [clojure.test :refer :all]
            [aws.s3 :as s3]))

(def *integration-test* false)

(def creds
  {:access-key (System/getenv "AWS_ACCESS_KEY")
   :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")})

(def bucket (System/getenv "S3_TEST_BUCKET"))

(def prefix (System/getenv "S3_TEST_PREFIX"))

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

(when (and *integration-test* creds bucket prefix)
  (defn temp-path
    []
    (.getAbsolutePath (java.io.File/createTempFile "temp" "")))

  (defn uuid
    []
    (str (java.util.UUID/randomUUID)))

  (deftest test-delete-get-put-get-delete-text
    (let [key (str prefix "/" (uuid))
          val (uuid)]
      (s3/delete-key creds bucket key)
      (is (thrown? Exception (s3/get-key-text creds bucket key)))
      (s3/put-key-text creds bucket key val)
      (is (= val (s3/get-key-text creds bucket key)))
      (s3/delete-key creds bucket key)))

  (deftest test-delete-get-put-get-delete-path
    (let [key (str prefix "/" (uuid))
          val (uuid)
          path (temp-path)]
      (s3/delete-key creds bucket key)
      (is (thrown? Exception (s3/get-key-text creds bucket key)))
      (spit path val)
      (s3/put-key-path creds bucket key path)
      (is (= val (s3/get-key-text creds bucket key)))
      (s3/delete-key creds bucket key))))

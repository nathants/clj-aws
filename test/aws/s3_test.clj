(ns aws.s3-test
  (:import com.amazonaws.services.s3.model.DeleteObjectsRequest$KeyVersion)
  (:require [clojure.test :refer :all]
            [clojure.java.shell :as sh]
            [amazonica.aws.s3 :as amz.s3]
            [clojure.string :as s]
            [aws.s3 :as s3]))

(def *integration-test* false)

(def test-bucket (System/getenv "S3_TEST_BUCKET"))

(def test-prefix "testing/clj-aws")

(defn wipe-test-keys
  []
  (as-> {:bucket-name test-bucket :prefix test-prefix :max-keys 1000000} $
    (amz.s3/list-objects $)
    (:object-summaries $)
    (map :key $)
    (doto $
      (->> (mapv #(println "deleting test s3 key:" %))))
    (if (-> $ count (> 1))
      (amz.s3/delete-objects :bucket-name test-bucket :keys (mapv #(DeleteObjectsRequest$KeyVersion. %) $))
      (mapv #(amz.s3/delete-object test-bucket %) $))))

(defmacro with-wipe-test-keys
  [& forms]
  `(do (wipe-test-keys)
       (try
         (do ~@forms)
         (finally
           (wipe-test-keys)))))

(deftest test-prefixes
  (is (= ["" "a" "a/b" "a/b/c"]
         (s3/-prefixes "a/b/c/d.csv"))))

(deftest test-indexed-keys
  (is (= {"" ["a/b/c.csv"
              "a/d.csv"
              "e/f.csv"]
          "a" ["a/b/c.csv"
               "a/d.csv"]
          "a/b" ["a/b/c.csv"]
          "e" ["e/f.csv"]}
         (s3/-indexed-keys ["a/b/c.csv"
                            "a/d.csv"
                            "e/f.csv"]))))

(deftest test-with-stubbed-s3
  (let [bucket "a-fake-bucket"
        key0 "a/fake/prefix/00.csv"
        key1 "a/fake/prefix/01.csv"
        key2 "a/fake/prefix/02.csv"
        content "fake\ncontent"
        data {bucket {key1 content
                      key2 content}}]
    (testing "reads work"
      (s3/with-stubbed-s3 data
        (testing "that 'prefix' and 'prefix/' are synonymous"
          (is (= [key1 key2] (s3/list-keys bucket "a/fake/prefix")))
          (is (= [key1 key2] (s3/list-keys bucket "a/fake/prefix/"))))
        (is (= content (s3/get-key-str bucket key1)))
        (is (= content (s3/get-key-str bucket key2)))))
    (testing "writes work"
      (s3/with-stubbed-s3 data
        (is (= 2 (count (s3/list-keys bucket "a/fake/prefix"))))
        (s3/put-key-str bucket key0 content)
        (is (= [key0 key1 key2] (s3/list-keys bucket "a/fake/prefix")))
        (is (= content (s3/get-key-str bucket key0)))))
    (testing "that previous stub data gets cleaned up"
      (s3/with-stubbed-s3 {}
        (is (thrown? Error (s3/list-keys bucket "a/fake/prefix")))
        (is (thrown? Error (s3/get-key-str bucket key0)))))))

(when (and *integration-test* test-bucket)

  (defn mk-key
    [key]
    (str (s/replace test-prefix #"/$" "")
         "/"
         (s/replace key #"^/" "")))

  (defn temp-path
    []
    (.getAbsolutePath (java.io.File/createTempFile "temp" "")))

  (defn uuid
    []
    (str (java.util.UUID/randomUUID)))

  (deftest test-mk-key
    (is (= "testing/clj-aws/123"  (mk-key "/123")))
    (is (= "testing/clj-aws/123"  (mk-key "123")))
    (is (= "testing/clj-aws/123"  (mk-key "123")))
    (is (= "testing/clj-aws/123"  (mk-key "/123"))))

  (deftest test-put-get-text
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)]
        (s3/put-key-str test-bucket key val)
        (is (= val (s3/get-key-str test-bucket key))))))

  (deftest test-put-get-path
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)
            path (temp-path)]
        (spit path val)
        (s3/put-key-path test-bucket key path)
        (is (= val (s3/get-key-str test-bucket key))))))

  (deftest test-delete-key
    (with-wipe-test-keys
      (let [key (mk-key "foo")]
        (s3/put-key-str test-bucket key "")
        (s3/delete-key test-bucket key)
        (is (thrown? Exception (s3/get-key-str test-bucket key))))))

  (deftest test-delete-keys
    (with-wipe-test-keys
      (testing "delete with more than 1 key"
        (let [key1 (mk-key "foo1")
              key2 (mk-key "foo2")]
          (s3/put-key-str test-bucket key1 "")
          (s3/put-key-str test-bucket key2 "")
          (s3/delete-keys test-bucket [key1 key2])
          (is (thrown? Exception (s3/get-key-str test-bucket key1)))
          (is (thrown? Exception (s3/get-key-str test-bucket key2)))))
      (testing "delete with 1 key"
        (let [key1 (mk-key "foo1")]
          (s3/put-key-str test-bucket key1 "")
          (s3/delete-keys test-bucket [key1])
          (is (thrown? Exception (s3/get-key-str test-bucket key1)))))
      (testing "delete with 0 keys"
        (is (= nil (s3/delete-keys test-bucket []))))))

  (deftest test-list-keys
    (with-wipe-test-keys
      (mapv #(s3/put-key-str test-bucket (mk-key %) "")
            ["a.txt"
             "dir1/b.txt"
             "c.txt"
             "dir2/dir3.txt"])
      (is (= ["testing/clj-aws/a.txt"
              "testing/clj-aws/c.txt"
              "testing/clj-aws/dir1/b.txt"
              "testing/clj-aws/dir2/dir3.txt"]
             (s3/list-keys test-bucket test-prefix)))))

  (deftest test-list-all
    (with-wipe-test-keys
      (mapv #(s3/put-key-str test-bucket (mk-key %) "")
            ["a.txt"
             "dir1/b.txt"])
      (testing "that we can fetch a specific number of keys"
        (is (= 1 (count (s3/list-all test-bucket test-prefix :fetch-exactly 1)))))
      (testing "normal list shows the contents of the test-prefix directory's keys and dirs"
        (is (= (->> ["dir1/"
                     "a.txt"]
                 (mapv mk-key))
               (s3/list-all test-bucket test-prefix))))
      (testing "list :prefixes-only shows the contents of the test-prefix directory's prefixes"
        (is (= (->> ["dir1/"]
                 (mapv mk-key))
               (s3/list-all test-bucket test-prefix :prefixes-only true))))
      (testing "list :keys-only shows the contents of the test-prefix directory's keys"
        (is (= (->> ["a.txt"]
                 (mapv mk-key))
               (s3/list-all test-bucket test-prefix :keys-only true))))
      (testing "list :recursive shows all keys for the current test-prefix including sub prefixes"
        (is (= (->> ["a.txt"
                     "dir1/b.txt"]
                 (mapv mk-key))
               (s3/list-all test-bucket test-prefix :recursive true))))))

  (deftest test-with-cached-s3
    (s3/with-clean-cache-dir
      (with-wipe-test-keys
        (let [key (mk-key (uuid))
              val (uuid)]
          (s3/put-key-str test-bucket key val)
          (s3/with-cached-s3
            (testing "we get the right stuff back with the cache"
              (is (= [key] (s3/list-keys test-bucket test-prefix)))
              (is (= val (s3/get-key-str test-bucket key))))
            ;; remember delete-* is completely ignorant of this caching
            (s3/delete-key test-bucket key)
            (testing "after deleting the key, we are still able to get stuff back from the cache"
              (is (= [key] (s3/list-keys test-bucket test-prefix)))
              (is (= val (s3/get-key-str test-bucket key)))))
          (testing "after exiting the cached form, we discover that the key has been deleted"
            (is (= [] (s3/list-keys test-bucket test-prefix)))
            (is (thrown? Exception (s3/get-key-str test-bucket key)))))))))

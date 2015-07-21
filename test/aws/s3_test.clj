(ns aws.s3-test
  (:import com.amazonaws.services.s3.model.DeleteObjectsRequest$KeyVersion)
  (:require [clojure.test :refer :all]
            [amazonica.aws.s3 :as amz.s3]
            [clojure.string :as s]
            [aws.s3 :as s3]))

(def *integration-test* true)

(def creds
  {:access-key (System/getenv "AWS_ACCESS_KEY")
   :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")})

(def bucket (System/getenv "S3_TEST_BUCKET"))

(def prefix "testing/cljs_aws")

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
  (s3/clear-cache)
  (testing "that stubbing works"
    (let [bucket "a-fake-bucket"
          key "a/fake/prefix/fake-file.csv"
          content "fake\ncontent"
          data {bucket {key content}}]
      (s3/with-stubbed-s3 data
        (is (= [key] (s3/list-keys nil bucket "a/fake/prefix")))
        (is (= content (s3/get-key-str nil bucket key))))))
  (testing "that previous stub data gets cleaned up"
    (s3/with-stubbed-s3 {}
      (is (thrown? Exception (s3/list-keys nil bucket "a/fake/prefix")))
      (is (thrown? Exception (s3/get-key-str nil bucket key))))))

(defn wipe-test-keys
    []
    (as-> {:bucket-name bucket :prefix prefix :max-keys 1000000} $
          (amz.s3/list-objects creds $)
          (:object-summaries $)
          (map :key $)
          (doto $
            (->> (mapv #(println "deleting test s3 key:" %))))
          (if (-> $ count (> 1))
            (amz.s3/delete-objects creds :bucket-name bucket :keys (mapv #(DeleteObjectsRequest$KeyVersion. %) $))
            (mapv #(amz.s3/delete-object creds bucket %) $))))

(defmacro with-wipe-test-keys
  [& forms]
  `(do (wipe-test-keys)
       (let [res# (do ~@forms)]
         (wipe-test-keys)
         res#)))

(when (and *integration-test* creds bucket)

  (defn mk-key
    [key]
    (str (s/replace prefix #"/$" "")
         "/"
         (s/replace key #"^/" "")))

  (defn temp-path
    []
    (.getAbsolutePath (java.io.File/createTempFile "temp" "")))

  (defn uuid
    []
    (str (java.util.UUID/randomUUID)))

  (deftest test-mk-key
    (is (= "testing/cljs_aws/123"  (mk-key "/123")))
    (is (= "testing/cljs_aws/123"  (mk-key "123")))
    (is (= "testing/cljs_aws/123"  (mk-key "123")))
    (is (= "testing/cljs_aws/123"  (mk-key "/123"))))

  (deftest test-put-get-text
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)]
        (s3/put-key-str creds bucket key val)
        (is (= val (s3/get-key-str creds bucket key))))))

  (deftest test-put-get-path
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)
            path (temp-path)]
        (spit path val)
        (s3/put-key-path creds bucket key path)
        (is (= val (s3/get-key-str creds bucket key))))))

  (deftest test-delete-key
    (with-wipe-test-keys
      (let [key (mk-key "foo")]
        (s3/put-key-str creds bucket key "")
        (s3/delete-key creds bucket key)
        (is (thrown? Exception (s3/get-key-str creds bucket key))))))

  (deftest test-delete-keys
    (with-wipe-test-keys
      (testing "delete with more than 1 key"
        (let [key1 (mk-key "foo1")
              key2 (mk-key "foo2")]
          (s3/put-key-str creds bucket key1 "")
          (s3/put-key-str creds bucket key2 "")
          (s3/delete-keys creds bucket [key1 key2])
          (is (thrown? Exception (s3/get-key-str creds bucket key1)))
          (is (thrown? Exception (s3/get-key-str creds bucket key2)))))
      (testing "delete with 1 key"
        (let [key1 (mk-key "foo1")]
          (s3/put-key-str creds bucket key1 "")
          (s3/delete-keys creds bucket [key1])
          (is (thrown? Exception (s3/get-key-str creds bucket key1)))))
      (testing "delete with 0 keys"
        (is (= nil (s3/delete-keys creds bucket []))))))

  (deftest test-list-keys
    (with-wipe-test-keys
      (mapv #(s3/put-key-str creds bucket (mk-key %) "")
            ["a.txt"
             "dir1/b.txt"
             "c.txt"
             "dir2/dir3.txt"])
      (is (= ["testing/cljs_aws/a.txt"
              "testing/cljs_aws/c.txt"
              "testing/cljs_aws/dir1/b.txt"
              "testing/cljs_aws/dir2/dir3.txt"]
             (s3/list-keys creds bucket prefix)))))

  (deftest test-list-all
    (with-wipe-test-keys
      (mapv #(s3/put-key-str creds bucket (mk-key %) "")
            ["a.txt"
             "dir1/b.txt"])
      (testing "that we can fetch a specific number of keys"
        (is (= 1 (count (s3/list-all creds bucket prefix :fetch-exactly 1)))))
      (testing "normal list shows the contents of the prefix directory's keys and dirs"
        (is (= (->> ["dir1/"
                     "a.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix))))
      (testing "list :prefixes-only shows the contents of the prefix directory's prefixes"
        (is (= (->> ["dir1/"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :prefixes-only true))))
      (testing "list :keys-only shows the contents of the prefix directory's keys"
        (is (= (->> ["a.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :keys-only true))))
      (testing "list :recursive shows all keys for the current prefix including sub prefixes"
        (is (= (->> ["a.txt"
                     "dir1/b.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :recursive true))))))

  (deftest test-with-cached-s3
    (s3/clear-cache)
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)]
        (s3/put-key-str creds bucket key val)
        (s3/with-cached-s3
          (testing "we get the right stuff back with the cache"
            (is (= [key] (s3/list-keys creds bucket prefix)))
            (is (= val (s3/get-key-str creds bucket key))))
          ;; remember delete-* and put-* are completely ignorant of this caching
          (s3/delete-key creds bucket key)
          (testing "after deleting the key, we are still able to get stuff back from the cache"
            (is (= [key] (s3/list-keys creds bucket prefix)))
            (is (= val (s3/get-key-str creds bucket key)))))
        (testing "after exiting the cached form, we discover that the key has been deleted"
          (is (= [] (s3/list-keys creds bucket prefix)))
          (is (thrown? Exception (s3/get-key-str creds bucket key))))))))

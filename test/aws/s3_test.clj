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
  (let [bucket "a-fake-bucket"
        key "a/fake/prefix/fake-file.csv"
        content "fake\ncontent"
        data {bucket {key content}}]
    (s3/with-stubbed-s3 data
      (is (= [key] (s3/list-keys nil bucket "a/fake/prefix")))
      (is (= content (s3/get-key-text nil bucket key))))))

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
        (s3/put-key-text creds bucket key val)
        (is (= val (s3/get-key-text creds bucket key))))))

  (deftest test-put-get-path
    (with-wipe-test-keys
      (let [key (mk-key (uuid))
            val (uuid)
            path (temp-path)]
        (spit path val)
        (s3/put-key-path creds bucket key path)
        (is (= val (s3/get-key-text creds bucket key))))))

  (deftest test-list
    (with-wipe-test-keys
      (mapv #(s3/put-key-text creds bucket (mk-key %) "")
            ["a.txt"
             "dir1/b.txt"])
      (testing "normal list shows the contents of the prefix directory's keys and dirs"
        (is (= (->> ["dir1/"
                     "a.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix))))
      (testing "normal list :dirs-only the contents of the prefix directory's dirs"
        (is (= (->> ["dir1/"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :dirs-only true))))
      (testing "normal list :dirs-only the contents of the prefix directory's dirs"
        (is (= (->> ["a.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :keys-only true))))
      (testing "normal list :recursive shows all keys for the current prefix"
        (is (= (->> ["a.txt"
                     "dir1/b.txt"]
                    (mapv mk-key))
               (s3/list-all creds bucket prefix :recursive true)))))))

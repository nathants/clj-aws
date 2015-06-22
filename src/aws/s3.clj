(ns aws.s3
  (:require [amazonica.aws.s3 :as amz.s3]
            [aws.s3-lib :as lib]
            [clojure.string :as s]
            [clojure.java.io :as io]))

;; todo support fetch-exactly for sizes larger than 1000 with paginating. currently fetch-exactly does the whole fetch in one round trip.
(defn list
  [creds bucket prefix & {:keys [recursive fetch-exactly max-keys keys-only]}]
  (let [delimiter (if-not recursive "/")
        prefix (-> prefix lib/strip-slash (str (or delimiter "")))
        max-keys (or fetch-exactly max-keys 1000)]
    ((fn f [m]
       (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker m :delimiter delimiter :max-keys max-keys)]
         (lazy-cat (if-not keys-only (:common-prefixes resp))
                   (->> resp :object-summaries (map :key) (filter #(lib/stripped-not= prefix %)))
                   (if (and (:truncated? resp) (not fetch-exactly))
                     (f (:next-marker resp))))))
     nil)))

(defn list-keys
  [creds bucket prefix & [marker]]
  (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker marker)]
    (lazy-cat
     (map #(select-keys % [:key :size]) (:object-summaries resp))
     (if (:truncated? resp)
       (list-keys creds bucket prefix (:next-marker resp))))))

(defn get-key-stream
  [creds bucket key]
  (:input-stream (amz.s3/get-object creds bucket key)))

(defn get-key-text
  [creds bucket key]
  (slurp (get-key-stream creds bucket key)))

(defn download-key
  [creds bucket key file-path]
  (-> (get-key-stream creds bucket key)
      (io/copy (io/file file-path))))

(defmacro with-cached-s3
  [& forms]
  `(with-redefs [list-keys (lib/cached-list-keys list-keys)
                 get-key-stream (lib/cached-lookup-key get-key-stream)]
     ~@forms))

(defmacro with-stubbed-s3
  [data & forms]
  `(let [paths# (lib/stub-s3 ~data)]
     (with-cached-s3
       ~@forms)
     (doseq [path# paths#]
       (-> path# java.io.File. .delete))))

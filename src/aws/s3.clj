(ns aws.s3
  (:require [amazonica.aws.s3 :as amz.s3]
            [clojure.string :as s]
            [clojure.java.io :as io]))

(defn -strip-slash
  [x]
  (s/replace x #"/$" ""))

(defn -stripped-not=
  [a b]
  (not= (-strip-slash a) (-strip-slash b)))

;; todo support fetch-exactly for sizes larger than 1000 with paginating. currently fetch-exactly does the whole fetch in one round trip.
(defn ls
  [creds bucket prefix & {:keys [recursive fetch-exactly max-keys keys-only]}]
  (let [delimiter (if-not recursive "/")
        prefix (-> prefix -strip-slash (str (or delimiter "")))
        max-keys (or fetch-exactly max-keys 1000)]
    ((fn f [m]
       (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker m :delimiter delimiter :max-keys max-keys)]
         (lazy-cat (if-not keys-only (:common-prefixes resp))
                   (->> resp :object-summaries (map :key) (filter #(-stripped-not= prefix %)))
                   (if (and (:truncated? resp) (not fetch-exactly))
                     (f (:next-marker resp))))))
     nil)))

(defn get-stream
  [creds bucket key]
  (:input-stream (amz.s3/get-object creds bucket key)))

(defn get
  [creds bucket key]
  (slurp (get-stream creds bucket key)))

(defn download
  [creds bucket key file-path]
  (-> (get-stream creds bucket key)
      (io/copy (io/file file-path))))

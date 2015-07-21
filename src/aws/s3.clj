(ns aws.s3
  (:import com.amazonaws.services.s3.model.DeleteObjectsRequest$KeyVersion)
  (:require [amazonica.aws.s3 :as amz.s3]
            [byte-transforms :as bt]
            [clojure.java.io :as io]
            [clojure.string :as s]))

(def -get-root-path
  (memoize
   #(doto (str (System/getProperty "user.home") "/tmp/s3cache")
      (-> java.io.File. .getParentFile .mkdirs))))

(defn -cache-path
  [& args]
  (str (-get-root-path) (bt/hash (apply str args))))

(defn -cached-get-key-stream
  [lookup-key]
  (fn [creds bucket key]
    (let [path (-cache-path bucket key)]
      (when-not (-> path java.io.File. .exists)
        (with-open [stream (lookup-key creds bucket key)
                    reader (io/reader stream)]
          (->> reader line-seq (s/join "\n") (spit path))))
      (io/reader path))))

(defn -cached-list-keys
  [list-keys]
  (fn f
    ([creds bucket prefix] (f creds bucket prefix [:key] nil))
    ([creds bucket prefix keys & [marker]]
     (let [path (-cache-path bucket prefix)]
       (when-not (-> path java.io.File. .exists)
         ;; todo this is eagerly reading all keys.
         ;; we should wrap the lazy seq and only cache the keys the caller actually consumes.
         (spit path (s/join "\n" (list-keys creds bucket prefix keys marker))))
       (-> path slurp (s/split #"\n"))))))

(defn -prefixes
  [key]
  (as-> key $
        (remove s/blank? (s/split $ #"/"))
        (butlast $)
        (for [i (range (count $))]
          (take (inc i) $))
        (map #(apply str "/" (interpose "/" %)) $)
        (map #(s/replace % #"^/" "") $)))

(defn -indexed-keys
  [keys]
  (loop [acc {}
         keys keys]
    (if (empty? keys)
      acc
      (let [key (first keys)
            append-key (fn [ks]
                         (conj (or ks []) key))
            f (fn [acc prefix]
                (update-in acc [prefix] append-key))
            result (reduce f acc (-prefixes key))]
        (recur result (rest keys))))))

(defn -stub-s3
  [data]
  (->> (for [[bucket vals] data]
         (concat
          (for [[prefix ks] (-> vals keys -indexed-keys)]
            (let [path (-cache-path bucket prefix)]
              (spit path (s/join "\n" ks))
              path))
          (for [[key text] vals]
            (let [path (-cache-path bucket key)]
              (spit path text)
              path))))
       (apply concat)
       vec))

(defn -strip-slash
  [x]
  (s/replace x #"/$" ""))

(defn -stripped-not=
  [a b]
  (not= (-strip-slash a) (-strip-slash b)))

(defn list-all
  [creds bucket prefix & {:keys [recursive fetch-exactly max-keys keys-only dirs-only]}]
  (let [delimiter (if-not recursive "/")
        prefix (if prefix (-> prefix -strip-slash (str (or delimiter ""))))
        max-keys (or fetch-exactly max-keys 1000)]
    ((fn f [m]
       (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker m :delimiter delimiter :max-keys max-keys)]
         (lazy-cat (if-not keys-only (:common-prefixes resp))
                   (if-not dirs-only (->> resp :object-summaries (map :key) (filter #(-stripped-not= prefix %))))
                   (if (and (:truncated? resp) (not fetch-exactly))
                     (f (:next-marker resp))))))
     nil)))

(defn list-keys
  ([creds bucket prefix] (list-keys creds bucket prefix [:key] nil))
  ([creds bucket prefix keys & [marker]]
   (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker marker)]
     (lazy-cat
      (map (if (-> keys count (> 1))
             #(select-keys % keys)
             (first keys))
           (:object-summaries resp))
      (if (:truncated? resp)
        (list-keys creds bucket prefix keys (:next-marker resp)))))))

(defn get-key-stream
  [creds bucket key]
  (:input-stream (amz.s3/get-object creds bucket key)))

(defn get-key-text
  [creds bucket key]
  (slurp (get-key-stream creds bucket key)))

(defn get-key-path
  [creds bucket key path]
  (-> (get-key-stream creds bucket key)
      (io/copy (io/file path))))

(defn put-key-text
  [creds bucket key text]
  (let [bytes (.getBytes text "UTF-8")
        stream (java.io.ByteArrayInputStream. bytes)
        metadata {:content-length (count bytes)}]
    (amz.s3/put-object creds :bucket-name bucket :key key :input-stream stream :metadata metadata)))

(defn put-key-path
  [creds bucket key path]
  (amz.s3/put-object creds :bucket-name bucket :key key :file path))

(defn delete-key
  [creds bucket key]
  (amz.s3/delete-object creds bucket key))

(defn delete-keys
  [creds bucket keys]
  (amz.s3/delete-objects creds :bucket-name bucket :keys (mapv #(DeleteObjectsRequest$KeyVersion. %) keys)))

(defmacro with-cached-s3
  [& forms]
  `(with-redefs [list-keys (-cached-list-keys list-keys)
                 get-key-stream (-cached-get-key-stream get-key-stream)]
     ~@forms))

(defmacro with-stubbed-s3
  [data & forms]
  `(let [paths# (-stub-s3 ~data)
         res# (with-cached-s3
                ~@forms)]
     (doseq [path# paths#]
       (-> path# java.io.File. .delete))
     res#))

(ns aws.s3
  (:require [amazonica.aws.s3 :as amz.s3]
            [byte-transforms :as bt]
            [clojure.java.io :as io]
            [clojure.string :as s]))

(defn -cache-path
  [& args]
  (str "/tmp/s3cache" (bt/hash (apply str args))))

(defn -cached-lookup-key
  [lookup-key]
  (fn [creds bucket key]
    (let [path (-cache-path bucket key)]
      (if-not (-> path java.io.File. .exists)
        (with-open [stream (lookup-key creds bucket key)
                    reader (io/reader stream)]
          (->> reader line-seq (s/join "\n") (spit path))))
      (io/reader path))))

(defn -cached-list-keys
  [list-keys]
  (fn [creds bucket prefix & [marker]]
    (let [path (-cache-path bucket prefix)]
      (when-not (-> path java.io.File. .exists)
        (spit path (s/join "\n" (list-keys creds bucket prefix marker))))
      (-> path slurp (s/split #"\n")))))

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

(defn list
  [creds bucket prefix & {:keys [recursive fetch-exactly max-keys keys-only]}]
  (let [delimiter (if-not recursive "/")
        prefix (if prefix (-> prefix -strip-slash (str (or delimiter ""))))
        max-keys (or fetch-exactly max-keys 1000)]
    ((fn f [m]
       (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker m :delimiter delimiter :max-keys max-keys)]
         (lazy-cat (if-not keys-only (:common-prefixes resp))
                   (->> resp :object-summaries (map :key) (filter #(-stripped-not= prefix %)))
                   (if (and (:truncated? resp) (not fetch-exactly))
                     (f (:next-marker resp))))))
     nil)))

(defn list-keys
  [creds bucket prefix & [marker]]
  (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker marker)]
    (lazy-cat
     (map :key (:object-summaries resp))
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
  `(with-redefs [list-keys (-cached-list-keys list-keys)
                 get-key-stream (-cached-lookup-key get-key-stream)]
     ~@forms))

(defmacro with-stubbed-s3
  [data & forms]
  `(let [paths# (-stub-s3 ~data)]
     (with-cached-s3
       ~@forms)
     (doseq [path# paths#]
       (-> path# java.io.File. .delete))))

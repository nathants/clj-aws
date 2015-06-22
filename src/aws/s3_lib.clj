(ns aws.s3-lib
  (:require [amazonica.aws.s3 :as amz.s3]
            [clojure.string :as s]
            [pandect.algo.sha1 :refer [sha1]]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn strip-slash
  [x]
  (s/replace x #"/$" ""))

(defn stripped-not=
  [a b]
  (not= (strip-slash a) (strip-slash b)))

(defn path-s3cache
  [bucket key]
  (str "/tmp/s3cache" (sha1 (str bucket key))))

(defn path-s3cache-keys
  [bucket prefix]
  (str "/tmp/s3cachekeys" (sha1 (str bucket prefix))))

;; todo these two cached fns should use some kind of generic fn disk cachine wrapper

(defn cached-lookup-key
  [lookup-key]
  (fn [creds bucket key]
    (let [path (path-s3cache bucket key)
          max-lines 50]
      (if-not (-> path java.io.File. .exists)
        (with-open [stream (lookup-key creds bucket key)
                    reader (io/reader stream)]
          (->> reader line-seq (take max-lines) (s/join "\n") (spit path))))
      (io/reader path))))

(defn cached-list-keys
  [list-keys]
  (fn [creds bucket prefix & [marker]]
    (let [path (path-s3cache-keys bucket prefix)]
      (when-not (-> path java.io.File. .exists)
        (spit path (json/generate-string (list-keys creds bucket prefix marker))))
      (-> path slurp (json/parse-string true)))))

(defn path->parts
  [path]
  (remove s/blank? (s/split path #"/")))

(defn parts->path
  [parts]
  (apply str "/" (interpose "/" parts)))

(defn prefixes
  [key]
  (as-> key $
        (path->parts $)
        (butlast $)
        (for [i (range (count $))]
          (take (inc i) $))
        (map parts->path $)
        (map #(s/replace % #"^/" "") $)))

(defn indexed-keys
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
            result (reduce f acc (prefixes key))]
        (recur result (rest keys))))))

(defn num-bytes
  [str]
  (-> str (.getBytes "UTF-8") count))

(defn stub-s3
  [data]
  (->> (for [[bucket vals] data]
         (concat
          (for [[prefix ks] (-> vals keys indexed-keys)]
            (let [path (path-s3cache-keys bucket prefix)]
              (->> (for [k ks]
                     {:key k :size (num-bytes (get vals k))})
                   json/generate-string
                   (spit path))
              path))
          (for [[key text] vals]
            (let [path (path-s3cache bucket key)]
              (spit path text)
              path))))
       (apply concat)
       vec))

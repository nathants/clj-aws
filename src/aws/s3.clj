(ns aws.s3
  (:import com.amazonaws.services.s3.model.DeleteObjectsRequest$KeyVersion)
  (:require [amazonica.aws.s3 :as amz.s3]
            [clojure.java
             [shell :as sh]
             [io :as io]]
            [clojure.string :as s]))

(def *max-keys*
  "The number of keys to fetch in a single roundtrip to aws."
  1000)

(defn -cache-dir
  "Create the temp directory in $HOME for cache files, mkdir it, and return the path."
  []
  (doto (str (System/getProperty "user.home") "/tmp")
    (-> java.io.File. .mkdirs)))

(defn -cache-path
  "Returns the path to use for a cache file based on a hash of the flags provided."
  [& flags]
  (->> [0]
    (when (not (number? (last flags))))
    (concat flags)
    (map str)
    (map #(s/replace % #"/$" ""))
    (apply str)
    clojure.lang.Murmur3/hashUnencodedChars
    (str (-cache-dir) "/s3cache_")))

(defn -cached-get-key-stream
  "Creates a disk cached get-key-stream fn."
  [get-key-stream]
  (fn [creds bucket k]
    (let [path (-cache-path bucket k)]
      (when-not (-> path java.io.File. .exists)
        (with-open [stream (get-key-stream creds bucket k)
                    reader (io/reader stream)]
          (->> reader line-seq (s/join "\n") (spit path))))
      (io/reader path))))

(defn -cached-list-keys
  "Creates a lazy disk cached list-keys fn. Please note that because this is lazy,
   the cache will only contain as many keys as the first caller consumed. "
  [list-keys]
  (let [lazy-read-cache (fn [bucket prefix]
                          (->> (map #(-cache-path bucket prefix %) (range))
                            (take-while #(-> % java.io.File. .exists))
                            (map slurp)
                            (map #(s/split % #"\n"))
                            (apply concat)
                            sort))
        lazy-generate-cache (fn [creds bucket prefix marker]
                              ((fn f [vals]
                                 (if-let [[i ks] (first vals)]
                                   (let [path (-cache-path bucket prefix i)]
                                     (spit path (s/join "\n" ks))
                                     (lazy-cat ks (f (rest vals))))))
                               (->> (list-keys creds bucket prefix marker)
                                 (partition-all *max-keys*)
                                 (map vector (range)))))
        has-cache (fn [bucket prefix]
                    (-> (-cache-path bucket prefix) java.io.File. .exists))]
    (fn [creds bucket prefix & [marker]]
      (if (has-cache bucket prefix)
        (lazy-read-cache bucket prefix)
        (lazy-generate-cache creds bucket prefix marker)))))

(defn -prefixes
  "For a given key, find all the prefixes that would return that key."
  [k]
  (as-> k $
    (s/split $ #"/")
    (butlast $)
    (map #(take % $) (->> $ count range (map inc)))
    (map #(apply str (interpose "/" %)) $)))

(defn -indexed-keys
  "Create a mapping of prefix->keys for every prefix of every key provided."
  [ks]
  (let [conj-key (fn [k acc prefix]
                   (update-in acc [prefix] #(conj (or % []) k)))
        build-index (fn [acc k]
                      (reduce (partial conj-key k) acc (-prefixes k)))]
    (reduce build-index {} ks)))

(defn -stub-s3
  "Take data, a mapping of {bucket {key str}}, and create cache files for
   for list-keys and get-key-stream as if this str content really existed, and had
   already been fetched and cached. Returns the paths of the cache files created."
  [data]
  (doseq [[bucket keys->contents] data]
    ;; stub all possible prefixes for all ks
    (doseq [[prefix ks] (-> keys->contents keys -indexed-keys)]
      (doseq [[i ks] (->> ks (partition-all *max-keys*) (map vector (range)))]
        (spit (-cache-path bucket prefix i) (str (s/join "\n" ks) "\n") :append true)))
    ;; stub keys contents
    (doseq [[k content] keys->contents]
      (spit (-cache-path bucket k) content))))

(defn list-all
  "Returns a lazy-seq of keys and/or prefixes for a given bucket and prefix.
   :recursive true -  works the way you are used with the filesystem.
   :fetch-exactly true -  limits the results to a fixed number of results.
   :keys-only true - return only keys
   :prefixes-only true - return only prefixes
   "
  [creds bucket prefix & {:keys [recursive fetch-exactly keys-only prefixes-only marker] :as flags}]
  (let [delimiter (if-not recursive "/")
        prefix (if prefix (-> prefix (s/replace #"/$" "") (str (or delimiter ""))))
        max-keys (or fetch-exactly *max-keys*)
        resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker marker :delimiter delimiter :max-keys max-keys)
        results (concat (if-not keys-only (:common-prefixes resp))
                        (if-not prefixes-only (->> resp :object-summaries (map :key))))]
    (concat results
            (if (and (:truncated? resp) (not fetch-exactly))
              (lazy-seq (apply list-all creds bucket prefix (apply concat (assoc flags :marker (:next-marker resp)))))))))

(defn list-keys
  "Returns a lazy-seq of keys for the given bucket and prefix."
  [creds bucket prefix & [marker]]
  (let [resp (amz.s3/list-objects creds :bucket-name bucket :prefix prefix :marker marker)
        results (map :key (:object-summaries resp))]
    (concat results
            (if (:truncated? resp)
              (lazy-seq (list-keys creds bucket prefix (:next-marker resp)))))))

(defn get-key-stream
  "Get key as InputStream"
  [creds bucket k]
  (:input-stream (amz.s3/get-object creds bucket k)))

(defn get-key-str
  "Get k as str."
  [creds bucket k]
  (slurp (get-key-stream creds bucket k)))

(defn get-key-path
  "Download k to path."
  [creds bucket k path]
  (with-open [r (get-key-stream creds bucket k)]
    (io/copy r (io/file path))))

(defn put-key-str
  "Upload str to key."
  [creds bucket k str]
  (let [bytes (.getBytes str "UTF-8")
        stream (java.io.ByteArrayInputStream. bytes)
        metadata {:content-length (count bytes)}]
    (amz.s3/put-object creds :bucket-name bucket :key k :input-stream stream :metadata metadata)))

(defn put-key-path
  "Upload the file at path to key."
  [creds bucket k path]
  (amz.s3/put-object creds :bucket-name bucket :key k :file path))

(defn delete-key
  "Delete key."
  [creds bucket k]
  (amz.s3/delete-object creds bucket k))

(defn delete-keys
  "Delete multiple keys in a single request."
  [creds bucket ks]
  (if (-> ks count (> 0))
    (amz.s3/delete-objects creds :bucket-name bucket :keys (mapv #(DeleteObjectsRequest$KeyVersion. %) ks))))

(defmacro with-cached-s3
  "Within this form, all calls to list-keys and get-key-* will be cached to disk, and only hit the network the first time.
   Please note that delete-* and put-* have no effect on this caching.
   Please note that list-keys will eagerly consume all keys to do it's caching, see todo on that fn."
  [& forms]
  `(with-redefs [list-keys (-cached-list-keys list-keys)
                 get-key-stream (-cached-get-key-stream get-key-stream)]
     ~@forms))

(defmacro with-stubbed-puts
  [& forms]
  `(with-redefs [put-key-str #(do %1 (-stub-s3 {%2 {%3 %4}}))
                 put-key-path #(do %1 (-stub-s3 {%2 {%3 (slurp %4)}}))]
     ~@forms))

(defmacro with-clean-cache-dir
  [& forms]
  `(let [dir# "/tmp/s3_test_cache"]
     (-> (sh/sh "rm" "-rf" dir#) :exit (= 0) assert)
     (-> (sh/sh "mkdir" "-p" dir#) :exit (= 0) assert)
     (with-redefs [s3/-cache-dir (constantly dir#)]
       ~@forms)))

(defmacro with-stubbed-s3
  "This builds on with-cached-s3 to provide a hook for caching custom data. This is useful for testing so that you
   can stub s3 with whatever data you want. The data provided, which is a map of {bucket {key str}}, will be written
   to disk as cache files. These cache files will be consumed by with-cached-s3 to provide results for calls to the
   list-keys and get-key-* functions. When this form exits, it cleans up it's cache files so they don't leak.
   Please note that delete-* and put-* have no effect on this caching.
   Please note that requests for data which is not stubbed will fallback to existing cache and then actually hit s3."
  [data & forms]
  `(with-clean-cache-dir
     (with-stubbed-puts
       (with-cached-s3
         (-stub-s3 ~data)
         ~@forms))))

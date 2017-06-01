(ns aws.s3
  (:require [amazonica.aws.s3 :as amz.s3]
            [clojure.java
             [io :as io]
             [shell :as sh]]
            [clojure.string :as s])
  (:import com.amazonaws.services.s3.model.DeleteObjectsRequest$KeyVersion
           java.io.FileOutputStream
           java.net.URI
           [java.nio.file Files Paths]))

;; TODO stubbed-s3 should redef amazonica.* to assert false

(def ^:dynamic *max-keys*
  "The number of keys to fetch in a single roundtrip to aws."
  1000)

(defn -cache-dir
  "Create the temp directory in $HOME for cache files, mkdir it, and return the path."
  []
  (doto (str (System/getProperty "user.home") "/tmp")
    (-> java.io.File. .mkdirs)))

(defn -cache-path
  "Returns the path to use for a cache file based on a hash of the flags provided."
  [& args]
  (as-> args $
    ;; the number as the last arg is a part of list-keys pagination
    (concat $ (when (not (number? (last args))) [0]))
    ;; drop trailing slashes, so lookups for directories, "foo/" and "foo" are synonamous
    (map #(if (string? %) (s/replace % #"/$" "") %) $)
    (hash $)
    (str (-cache-dir) "/s3cache_" $)))

(defn -cached-get-key-stream
  "Creates a disk cached get-key-stream fn."
  [get-key-stream]
  (fn [bucket key]
    (let [path (-cache-path bucket key)]
      (when-not (-> path java.io.File. .exists)
        (with-open [stream (get-key-stream bucket key)
                    reader (io/reader stream)]
          (io/copy reader (io/file path))))
      (io/reader path))))

(defn -cached-get-key-path [bucket key path]
  (Files/copy (Paths/get (URI. (str "file://" (-cache-path bucket key))))
              (FileOutputStream. path)))

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
        lazy-generate-cache (fn [bucket prefix marker]
                              ((fn f [vals]
                                 (if-let [[i ks] (first vals)]
                                   (let [path (-cache-path bucket prefix i)]
                                     (spit path (s/join "\n" ks))
                                     (lazy-cat ks (f (rest vals))))))
                               (->> (list-keys bucket prefix marker)
                                 (partition-all *max-keys*)
                                 (map vector (range)))))
        has-cache (fn [bucket prefix]
                    (-> (-cache-path bucket prefix) java.io.File. .exists))]
    (fn [bucket prefix & [marker]]
      (if (has-cache bucket prefix)
        (lazy-read-cache bucket prefix)
        (lazy-generate-cache bucket prefix marker)))))

(defn -prefixes
  "For a given key, find all the prefixes that would return that key."
  [key]
  (as-> key $
    (s/split $ #"/")
    (butlast $)
    (map #(take % $) (->> $ count range (map inc)))
    (map #(apply str (interpose "/" %)) $)
    (cons "" $)))

(defn -indexed-keys
  "Create a mapping of prefix->keys for every prefix of every key provided."
  [ks]
  (let [conj-key (fn [key acc prefix]
                   (update-in acc [prefix] #(conj (or % []) key)))
        build-index (fn [acc key]
                      (reduce (partial conj-key key) acc (-prefixes key)))]
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
    (doseq [[key content] keys->contents
            :let [path (-cache-path bucket key)]]
      (if (vector? content)
        (Files/copy (Paths/get (URI. (str "file://" (first content)))) (FileOutputStream. path))
        (spit path content)))))

(defn list-all
  "Returns a lazy-seq of keys and/or prefixes for a given bucket and prefix.
   :recursive true -  works the way you are used with the filesystem.
   :fetch-exactly true -  limits the results to a fixed number of results.
   :keys-only true - return only keys
   :prefixes-only true - return only prefixes
   "
  [bucket prefix & {:keys [recursive fetch-exactly keys-only prefixes-only marker] :as flags}]
  (let [delimiter (if-not recursive "/")
        prefix (if prefix (-> prefix (s/replace #"/$" "") (str (or delimiter ""))))
        max-keys (or fetch-exactly *max-keys*)
        resp (amz.s3/list-objects :bucket-name bucket :prefix prefix :marker marker :delimiter delimiter :max-keys max-keys)
        results (concat (if-not keys-only (:common-prefixes resp))
                        (if-not prefixes-only (->> resp :object-summaries (map :key))))]
    (concat results
            (if (and (:truncated? resp) (not fetch-exactly))
              (lazy-seq (apply list-all bucket prefix (apply concat (assoc flags :marker (:next-marker resp)))))))))

(defn list-keys
  "Returns a lazy-seq of keys for the given bucket and prefix."
  [bucket prefix & [marker]]
  (let [resp (amz.s3/list-objects :bucket-name bucket :prefix prefix :marker marker)
        results (map :key (:object-summaries resp))]
    (concat results
            (if (:truncated? resp)
              (lazy-seq (list-keys bucket prefix (:next-marker resp)))))))

(defn get-key-stream
  "Get key as InputStream"
  [bucket key]
  (:input-stream (amz.s3/get-object bucket key)))

(defn get-key-str
  "Get key as str."
  [bucket key]
  (slurp (get-key-stream bucket key)))

(defn get-key-path
  "Download key to path."
  [bucket key path]
  (with-open [r (get-key-stream bucket key)]
    (io/copy r (io/file path))))

(defn put-key-str
  "Upload str to key."
  [bucket key str]
  (let [bytes (.getBytes str "UTF-8")
        stream (java.io.ByteArrayInputStream. bytes)
        metadata {:content-length (count bytes)}]
    (amz.s3/put-object :bucket-name bucket :key key :input-stream stream :metadata metadata)))

(defn put-key-path
  "Upload the file at path to key."
  [bucket key path]
  (amz.s3/put-object :bucket-name bucket :key key :file path))

(defn delete-key
  "Delete key."
  [bucket key]
  (amz.s3/delete-object bucket key))

(defn delete-keys
  "Delete multiple keys in a single request."
  [bucket ks]
  (if (-> ks count (> 0))
    (amz.s3/delete-objects :bucket-name bucket :keys (mapv #(DeleteObjectsRequest$KeyVersion. %) ks))))

(defmacro with-cached-s3
  "Within this form, all calls to list-keys and get-key-* will be cached to disk, and only hit the network the first time.
   Please note that delete-* and put-* have no effect on this caching.
   Please note that list-keys will eagerly consume all keys to do it's caching, see todo on that fn."
  [& forms]
  `(with-redefs [list-keys (-cached-list-keys list-keys)
                 get-key-stream (-cached-get-key-stream get-key-stream)
                 get-key-path -cached-get-key-path]
     ~@forms))

(defmacro with-stubbed-puts
  [& forms]
  `(with-redefs [put-key-str  #(-stub-s3 {%1 {%2 %3}})
                 put-key-path #(-stub-s3 {%1 {%2 [%3]}})
                 amz.s3/list-objects ~(fn [_ bucket _ prefix & _]
                                        (assert false (str "you tried to list-keys, but you haven't stubbed anything for: s3://" bucket "/" prefix)))
                 amz.s3/get-object ~(fn [bucket key]
                                      (assert false (str "you tried to get-key, but you haven't stubbed anything for: s3://" bucket "/" key)))
                 amz.s3/put-object ~(fn [& _]
                                      (assert false "put-object should never be called while stubbed"))
                 amz.s3/delete-object ~(fn [& _]
                                         (assert false "delete-object should never be called while stubbed"))
                 amz.s3/delete-objects ~(fn [& _]
                                          (assert false "delete-objects should never be called while stubbed"))]
     ~@forms))

(defmacro with-clean-cache-dir
  [& forms]
  `(let [dir# (.trim (:out (sh/sh "mktemp" "--directory")))]
     (try
       (with-redefs [aws.s3/-cache-dir (constantly dir#)]
         ~@forms)
       (finally
         (-> (sh/sh "rm" "-rf" dir#) :exit (= 0) assert)))))

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

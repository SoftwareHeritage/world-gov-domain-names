(ns common
  "Helpers shared by the scripts of this repository: CSV and HTTP I/O,
  country directories, validated.csv access, bounded parallelism. Pure
  definitions only -- loading this namespace has no side effect. Loaded
  through the :paths [\"scripts\"] of bb.edn, so every `bb …` command run
  from the repository root can (require '[common])."
  (:require [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def ua "world-gov-domain-names/0.1 (https://github.com/bzg)")
(def force? (= "1" (System/getenv "FORCE")))

(defn env-int [env-name default]
  (let [v (System/getenv env-name)]
    (if (and v (re-matches #"\d+" v)) (Integer/parseInt v) default)))

(defn err [& xs] (binding [*out* *err*] (println (apply str xs))))

(defn single-line
  "Collapse a (possibly multi-line) string to a single trimmed line. Keeps the
  CSV well-formed when storing HTTP error messages as a status."
  [s]
  (-> (str s) (str/replace #"\s+" " ") str/trim))

(defn read-csv-file
  "Read a CSV with header as a seq of maps {col-name value}. Column names
  are kept as strings (preserves spaces, e.g. 'Government Portal Domain')."
  [path]
  (when (fs/exists? path)
    (with-open [r (io/reader (str path))]
      (let [rows (doall (csv/read-csv r))]
        (when (seq rows)
          (let [headers (first rows)]
            (vec (for [row (rest rows)]
                   (zipmap headers row)))))))))

(defn read-csv-raw
  "Read a CSV as a seq of vectors (header included)."
  [path]
  (when (fs/exists? path)
    (with-open [r (io/reader (str path))]
      (doall (csv/read-csv r)))))

(defn write-csv-file [path header rows]
  (when-let [parent (fs/parent path)]
    (fs/create-dirs parent))
  (with-open [w (io/writer (str path))]
    (csv/write-csv w (cons header rows))))

(defn ensure-dir [path] (fs/create-dirs path) path)

(defn country-src
  "Path under countries/<c>/sources/<source>/. With a file, appends it:
  (country-src \"FRA_france\" \"iana\" \"cctld.csv\"). With none, the dir."
  [country-dir source & [file]]
  (str "countries/" country-dir "/sources/" source (when file (str "/" file))))

(defn country-dirs
  "All country_dir present under countries/. ASCII-sorted."
  []
  (->> (fs/list-dir "countries")
       (filter fs/directory?)
       (map (comp str fs/file-name))
       sort
       vec))

(defn valid-hostname?
  "True if h is a syntactically valid hostname: dotted labels of a-z0-9 with
  internal hyphens, at least two labels. Rejects URLs, paths, wildcards, email
  addresses and stray punctuation (spaces, quotes, commas, pipes, '?', ...)."
  [h]
  (boolean
    (and h (re-matches #"[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+" h))))

(defn validated-rows
  "All rows of countries/<c>/validated.csv, the explicit per-country list
  of confirmed roots (domain,level,source; level is central or
  central-1). Seq of [country domain level source]."
  []
  (for [c (country-dirs)
        :let [path (str "countries/" c "/validated.csv")]
        :when (fs/exists? path)
        [domain level source name] (rest (read-csv-raw path))
        :let [domain (some-> domain str/trim str/lower-case)]
        :when (valid-hostname? domain)]
    [c domain (str/trim (or level "")) (str/trim (or source ""))
     (str/trim (or name ""))]))

(defn normalize-name
  "Lowercase a name and strip everything but [a-z0-9], for matching country
  names (from CSVs or remote sources) against country_dir slugs."
  [s]
  (-> (or s "") str/lower-case (str/replace #"[^a-z0-9]" "")))

(defn country-slug
  "FRA_france -> france, COD_democratic_republic_of_the_congo -> democraticrepublicofthecongo."
  [country-dir]
  (normalize-name (second (str/split country-dir #"_" 2))))

(defn extract-host
  "https://www.example.com/path -> example.com. Returns nil on blank input."
  [url]
  (when (and url (not (str/blank? url)))
    (-> url
        str/lower-case
        (str/replace #"^https?://" "")
        (str/replace #"^www\." "")
        (str/replace #"/.*$" "")
        (str/replace #":.*$" ""))))

(defn bounded-pmap
  "Like pmap but with a fixed thread pool of size n. Returns a vector of
  results. Useful when each task does HTTP and we want a controlled
  concurrency (avoids saturating endpoints like Wikidata SPARQL)."
  [n f coll]
  (let [pool (java.util.concurrent.Executors/newFixedThreadPool (int (max 1 n)))
        ;; convey dynamic bindings (*out* rebinding in cmd-enrich's log
        ;; capture...) to the pool threads; a bare fn would print to the
        ;; real stdout instead
        g (bound-fn* f)]
    (try
      (->> coll
           (mapv #(.submit pool ^Callable (fn [] (g %))))
           (mapv #(.get ^java.util.concurrent.Future %)))
      (finally
        ;; cancel the pending tasks when one failed: with a plain shutdown
        ;; the pool would keep running them in the background
        (.shutdownNow pool)))))

(defn iter-countries
  "Apply f to each country_dir. concurrency >= 2 runs up to that many in
  parallel via bounded-pmap; default 1 = sequential doseq."
  ([f selection] (iter-countries f selection 1))
  ([f selection concurrency]
   (let [targets (if (seq selection) selection (country-dirs))]
     (if (<= concurrency 1)
       (doseq [c targets] (f c))
       (bounded-pmap concurrency f targets)))))

(defn build-un-status-map
  "Read data/world-governments.csv once and return a map country_dir -> un_status.
  The country_dir is recovered by matching ISO3-stripped slugs."
  []
  (let [master (or (read-csv-file "data/world-governments.csv") [])
        slug->status
        (into {}
              (for [row master
                    :let [slug (normalize-name (get row "Country"))
                          status (str/trim (or (get row "un_status") "member"))]
                    :when (not (str/blank? slug))]
                [slug status]))]
    (into {}
          (for [c (country-dirs)
                :let [s (get slug->status (country-slug c))]
                :when s]
            [c s]))))

(defn csv-field
  "Read column 2 of a key-value CSV for the row where col1 == k."
  [csv-path k]
  (some (fn [[col1 col2]]
          (when (= col1 k) col2))
        (rest (read-csv-raw csv-path))))

(defn mapping-row
  "First data row (header dropped) of a CSV whose first column equals k, or nil."
  [csv-path k]
  (some #(when (= (first %) k) %)
        (rest (read-csv-raw csv-path))))

(defn dedup-by-first
  "Keep the first row per first-column value, sorted by first column."
  [rows]
  (->> rows
       (reduce (fn [acc r] (if (contains? acc (first r)) acc (assoc acc (first r) r)))
               {})
       vals
       (sort-by first)))

(defn merge-field-rows
  "Merge freshly-fetched [field value] rows into the existing field-CSV at path
  so a (re)fetch only ADDS or UPDATES, never erases: a new non-blank value
  updates its field, a blank new value falls back to the existing value, and any
  pre-existing field the fetch did not emit is preserved. Order: emitted fields
  first (in fetch order), then extra pre-existing fields."
  [path new-rows]
  (let [existing     (when (fs/exists? path) (rest (read-csv-raw path)))
        existing-map (into {} (for [[k v] existing] [k v]))
        emitted      (set (map first new-rows))
        primary (for [[k v] new-rows]
                  [k (if (str/blank? v) (get existing-map k "") v)])
        extra   (for [[k v] existing :when (not (emitted k))] [k v])]
    (concat primary extra)))

(defn merge-rows-union
  "Union the existing CSV rows (header dropped) at path with new-rows,
  de-duplicated on the whole row: existing rows are never dropped, genuinely new
  rows are added. Sorted by the column at sort-idx, then the full row."
  [path new-rows sort-idx]
  (let [existing (when (fs/exists? path) (rest (read-csv-raw path)))]
    (->> (concat existing new-rows)
         (map vec)
         distinct
         (sort-by (juxt #(nth % sort-idx "") identity)))))

(defn skip? [out-path] (and (fs/exists? out-path) (not force?)))

;; ===========================================================================
;;  HTTP helpers
;; ===========================================================================

;; Shared java.net.http client. :follow-redirects :never mirrors the previous
;; curl behaviour (no -L): a 3xx is reported as-is rather than chased, which is
;; what the probe relies on to record 301/302 statuses. Per-request :timeout
;; bounds the whole exchange (incl. connect); :connect-timeout is a backstop.
(def http-client
  (http/client (assoc http/default-client-opts
                      :follow-redirects :never
                      :connect-timeout 15000)))

(defn http-get
  "GET via babashka.http-client with User-Agent and retries on network errors.
  Returns the body string on HTTP 200, nil otherwise. Honors :timeout
  (seconds, default 30), :retries (default 3), :query-params, :accept and
  :client (defaults to the no-redirect client above)."
  ([url] (http-get url {}))
  ([url {:keys [timeout retries query-params accept client]
         :or {timeout 30 retries 3 accept "*/*"}}]
   (loop [attempt 1]
     (let [resp (try (http/get url
                               {:client (or client http-client)
                                :headers {"User-Agent" ua "Accept" accept}
                                :query-params (or query-params {})
                                :throw false
                                :timeout (* timeout 1000)})
                     (catch Exception _ nil))
           status (:status resp)]
       (cond
         (and resp (= 200 status) (not (str/blank? (:body resp))))
         (:body resp)

         ;; a 4xx is deterministic (404, 403...): retrying cannot help.
         ;; 429 is the exception -- it clears once the rate window resets.
         (and status (<= 300 status 499) (not= 429 status))
         nil

         (< attempt retries)
         (do (Thread/sleep (* attempt 3000))
             (recur (inc attempt)))

         :else nil)))))

(defn http-get-curl
  "GET via the curl binary, for hosts whose WAF rejects the JVM HTTP client
  (publicadministration.un.org answers 400 to it regardless of headers).
  Returns the body string on HTTP 2xx, nil otherwise. Honors :timeout
  (seconds, default 30) and :retries (default 3)."
  ([url] (http-get-curl url {}))
  ([url {:keys [timeout retries] :or {timeout 30 retries 3}}]
   (loop [attempt 1]
     (let [{:keys [exit out]}
           (try (proc/sh "curl" "-sfL" "--max-time" (str timeout)
                         "-A" ua url)
                (catch Exception _ nil))
           body (when (and exit (zero? exit)) out)]
       (if (not (str/blank? body))
         body
         (if (< attempt retries)
           (do (Thread/sleep (* attempt 3000))
               (recur (inc attempt)))
           nil))))))

(defn merge-validated-rows
  "Replace, among the [domain level source name] rows of a validated.csv,
  the rows owned by source with fresh ones: the old rows of that source
  go, the fresh rows come in unless the domain is already listed under
  another source (a manual decision or another registry wins). Pure;
  result ASCII-sorted by domain."
  [rows source fresh]
  (let [kept   (remove #(= source (nth % 2)) rows)
        taken  (set (map first kept))]
    (->> (concat kept (remove #(contains? taken (first %)) fresh))
         (sort-by first))))

(defn sync-validated!
  "Rewrite the rows of countries/<c>/validated.csv owned by source with
  fresh [domain level name] rows (see merge-validated-rows). This is how
  a generated list (registry, cmd-govuk) enters the hand-edited file
  without touching anyone else's rows."
  [country-dir source fresh]
  (let [path (str "countries/" country-dir "/validated.csv")
        rows (for [[d level src name] (rest (read-csv-raw path))
                   :let [d (some-> d str/trim str/lower-case)]
                   :when (valid-hostname? d)]
               [d (or level "") (or src "") (or name "")])
        fresh (for [[d level name] fresh] [d level source (or name "")])]
    (write-csv-file path ["domain" "level" "source" "name"]
                    (merge-validated-rows rows source fresh))))

(defn truncate [s n]
  (if (> (count s) n) (str (subs s 0 (- n 3)) "...") s))

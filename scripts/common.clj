(ns common
  "Helpers shared by the scripts of this repository: CSV and HTTP I/O,
  country directories, the per-country decision files, the
  country-metadata tables, bounded parallelism. Pure
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
  (country-src \"FRA_france\" \"crtsh\" \"gouv.fr.csv\"). With none, the dir."
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

(def levels
  "The tiers a confirmed domain may carry: central government, the first
  administrative tier below it (Land, state, region…), and everything
  lower (local). local rows are never harvested: they carve lower-tier
  labels out of a shared root (abingdon.gov.uk under gov.uk) and keep
  such hosts out of proposed.csv. A lower-tier body is thus a confirmed
  public-sector domain of level local, not an exclusion (decision of
  2026-09-10: the former excluded.csv files, all local bodies, became
  local rows of curated.csv); excluded.csv is for what is not a public
  body at all."
  #{"central" "central-1" "local"})

(def harvest-levels
  "The levels whose domains stand for a subtree to harvest and probe."
  #{"central" "central-1"})

;; Per-country decision files, named after the status of their domains.
;; Hand-edited: countries/<c>/curated.csv (domain,level,name -- confirmed
;; by hand) and countries/<c>/excluded.csv (domain,reason -- rejected).
;; Generated: countries/<c>/sources/<registry>/registered.csv
;; (domain,level -- listed by an official registry). The confirmed
;; domains of a country are their compilation, registered plus curated
;; minus excluded, computed in memory (confirmed-rows): no file to
;; regenerate after a curation.
(defn curated-file [country-dir] (str "countries/" country-dir "/curated.csv"))
(defn excluded-file [country-dir] (str "countries/" country-dir "/excluded.csv"))
(defn registered-file [country-dir registry]
  (country-src country-dir registry "registered.csv"))

(defn- read-domain-rows
  "Rows of a CSV whose first column is a hostname, as vectors of n cells:
  hostname normalised, the other cells trimmed (\"\" when missing), rows
  with an invalid hostname dropped. Empty when the file is absent."
  [path n]
  (for [row (rest (read-csv-raw path))
        :let [domain (some-> (first row) str/trim str/lower-case)]
        :when (valid-hostname? domain)]
    (into [domain] (map #(str/trim (or (nth row % nil) "")) (range 1 n)))))

(defn read-curated
  "[domain level name] rows of countries/<c>/curated.csv."
  [country-dir] (read-domain-rows (curated-file country-dir) 3))

(defn read-excluded
  "[domain reason] rows of countries/<c>/excluded.csv."
  [country-dir] (read-domain-rows (excluded-file country-dir) 2))

(defn read-registered
  "[domain level] rows of countries/<c>/sources/<registry>/registered.csv."
  [country-dir registry]
  (read-domain-rows (registered-file country-dir registry) 2))

(defn registries
  "The registries of a country: the <registry> of every
  countries/<c>/sources/<registry>/registered.csv. ASCII-sorted."
  [country-dir]
  (let [dir (str "countries/" country-dir "/sources")]
    (when (fs/exists? dir)
      (sort (map #(str (fs/file-name (fs/parent %))) (fs/glob dir "*/registered.csv"))))))

(defn compile-confirmed
  "The confirmed [domain level source name] rows of a country from its
  curated [domain level name] rows, the [[registry [[domain level]…]]…]
  of its registries and its excluded [domain reason] rows: a curated row
  wins over any registered row of the same domain (its level too),
  registries fold in in the given order, and an excluded domain leaves.
  source is curated or the registry's name; name is blank for a
  registered row. Throws (ex-info) on an unknown level or on a domain
  both curated and excluded: a contradiction is for the curator to
  settle, not for the compiler to arbitrate. Pure; ASCII-sorted by
  domain."
  [curated registries excluded]
  (let [rows     (concat (for [[d level name] curated] [d level "curated" name])
                         (for [[registry rs] registries, [d level] rs] [d level registry ""]))
        excluded (set (map first excluded))]
    (when-let [[d level] (first (remove #(contains? levels (second %)) rows))]
      (throw (ex-info (str d ": unknown level '" level "'") {:domain d})))
    (when-let [[d] (first (filter #(contains? excluded (first %)) curated))]
      (throw (ex-info (str d ": both curated and excluded") {:domain d})))
    (->> rows
         (remove #(contains? excluded (first %)))
         (reduce (fn [m [d :as row]] (cond-> m (not (contains? m d)) (assoc d row))) {})
         vals
         (sort-by first))))

(defn confirmed-country
  "compile-confirmed over the decision files of a country."
  [country-dir]
  (compile-confirmed (read-curated country-dir)
                     (for [r (registries country-dir)] [r (read-registered country-dir r)])
                     (read-excluded country-dir)))

(def ^:private confirmed-cache (atom nil))

(defn confirmed-rows
  "The confirmed domains of every country: vector of [country domain
  level source name] (confirmed-country). Compiled once per run and
  cached -- the per-country callers (aggregate, probes) would otherwise
  reread the decision files each time; write-registered! drops the
  cache. Throws (ex-info) on the first contradiction: the dispatcher
  reports it and exits 1."
  []
  (or @confirmed-cache
      (reset! confirmed-cache
              (vec (for [c (country-dirs)
                         [d level source name] (try (confirmed-country c)
                                                    (catch clojure.lang.ExceptionInfo e
                                                      (throw (ex-info (str c ": " (ex-message e))
                                                                      (assoc (ex-data e) :country c)))))]
                     [c d level source name])))))

(defn write-registered!
  "Write the fresh [domain level] rows of
  countries/<c>/sources/<registry>/registered.csv: this is how an
  official list enters the confirmed domains without touching anyone
  else's rows. Drops the confirmed cache."
  [country-dir registry rows]
  (write-csv-file (registered-file country-dir registry) ["domain" "level"]
                  (sort-by first (for [[d level] rows] [d level])))
  (reset! confirmed-cache nil))

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
        (str/replace #":\d+$" ""))))

(def multi-tlds
  #{"co.uk" "gov.uk" "ac.uk" "org.uk" "com.au" "gov.au" "org.au"
    "co.nz" "gov.nz" "com.br" "gov.br" "co.za" "gov.za"})

(defn parent-domain [host]
  (or (some #(when (str/ends-with? host (str "." %)) %) multi-tlds)
      (let [parts (str/split host #"\.")
            n (count parts)]
        (when (>= n 2)
          (str (nth parts (- n 2)) "." (last parts))))))

(def slug->country-dir
  "{normalised country name -> country_dir}, e.g. \"france\" -> FRA_france."
  (delay (into {} (for [c (country-dirs)] [(country-slug c) c]))))

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

(defn scoped-countries
  "The country_dirs named in selection, or all of them when selection is
  empty. An unknown name is an error (ERR, nil): a typo must not create
  a countries/<typo>/ directory through the first file written there."
  [selection]
  (let [all (country-dirs)
        unknown (remove (set all) selection)]
    (if (seq unknown)
      (err "ERR: unknown country dir(s): " (str/join ", " unknown))
      (or (seq selection) all))))

(defn iter-countries
  "Apply f to each country_dir of selection (all when empty; see
  scoped-countries). concurrency >= 2 runs up to that many in parallel
  via bounded-pmap; default 1 = sequential doseq. Returns 1 without
  running anything when selection names an unknown country, 0 otherwise,
  so a command can pass it on as its exit code."
  ([f selection] (iter-countries f selection 1))
  ([f selection concurrency]
   (if-let [targets (scoped-countries selection)]
     (do (if (<= concurrency 1)
           (doseq [c targets] (f c))
           (bounded-pmap concurrency f targets))
         0)
     1)))

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

(defn skip? [out-path] (and (fs/exists? out-path) (not force?)))

;; ---------------------------------------------------------------------------
;;  Country-metadata tables -- data/sources/<source>.csv
;; ---------------------------------------------------------------------------
;; One table per enrichment source (iana, cia_factbook, un_desa, oecd,
;; country_data), one row per country keyed by country_dir, holding only
;; the fields the reports and the scoring read. Read once per run and
;; cached; upsert-table-row! rewrites the table under a lock, as the
;; enrich sources process countries in parallel.

(defn source-table [source] (str "data/sources/" source ".csv"))

(def ^:private tables-cache (atom {}))
(def ^:private tables-lock (Object.))

(defn read-table
  "{country_dir {column value}} of data/sources/<source>.csv; {} when absent."
  [source]
  (or (get @tables-cache source)
      (locking tables-lock
        (or (get @tables-cache source)
            (let [rows (into {} (for [{:strs [country_dir] :as row} (read-csv-file (source-table source))]
                                  [country_dir row]))]
              (swap! tables-cache assoc source rows)
              rows)))))

(defn table-row
  "The {column value} row of a country in data/sources/<source>.csv, or nil."
  [source country-dir]
  (get (read-table source) country-dir))

(defn table-field
  "One field of a country's row in data/sources/<source>.csv, \"\" when absent."
  [source country-dir column]
  (or (get (table-row source country-dir) column) ""))

(defn table-row-done?
  "True when the country already has a row in the table and FORCE is not set:
  the per-row counterpart of skip?."
  [source country-dir]
  (and (some? (table-row source country-dir)) (not force?)))

(defn upsert-table-row!
  "Replace or add the row of country-dir in data/sources/<source>.csv
  (header: country_dir first, then the columns; new-row: {column value}).
  A blank new value keeps the existing one, so a refetch only adds or
  updates, never erases. Rows ASCII-sorted by country_dir. Thread-safe."
  [source header country-dir new-row]
  (locking tables-lock
    (let [rows (read-table source)
          old  (get rows country-dir {})
          row  (into {"country_dir" country-dir}
                     (for [col (rest header)
                           :let [v (get new-row col "")]]
                       [col (if (str/blank? v) (get old col "") v)]))
          rows (assoc rows country-dir row)]
      (write-csv-file (source-table source) header
                      (for [[_ r] (sort-by key rows)] (map #(get r % "") header)))
      (swap! tables-cache assoc source rows))))

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

(defn- http-outcome
  "What to do with one HTTP attempt: :ok on a 2xx with a non-blank body,
  :give-up on a deterministic 3xx/4xx (redirects are never followed by the
  JVM client; a 404 or 403 will not change -- 429 excepted, it clears once
  the rate window resets), :retry on anything else (network error, 5xx,
  empty body)."
  [status body]
  (cond
    (and status (<= 200 status 299) (not (str/blank? body))) :ok
    (and status (<= 300 status 499) (not= 429 status))       :give-up
    :else                                                     :retry))

(defn- with-retries
  "Run attempt!, a thunk returning [status body], up to retries times with
  a growing pause (3 s, 6 s, …) between attempts, following http-outcome.
  Returns the body or nil."
  [retries attempt!]
  (loop [attempt 1]
    (let [[status body] (try (attempt!) (catch Exception _ [nil nil]))]
      (case (http-outcome status body)
        :ok      body
        :give-up nil
        :retry   (if (< attempt retries)
                   (do (Thread/sleep (* attempt 3000))
                       (recur (inc attempt)))
                   nil)))))

(defn http-get
  "GET via babashka.http-client with User-Agent; retries per http-outcome.
  Returns the body string on a 2xx, nil otherwise. Honors :timeout
  (seconds, default 30), :retries (default 3), :query-params, :accept and
  :client (defaults to the no-redirect client above)."
  ([url] (http-get url {}))
  ([url {:keys [timeout retries query-params accept client]
         :or {timeout 30 retries 3 accept "*/*"}}]
   (with-retries retries
     (fn []
       ;; :query-params is only passed when given: with an empty map the
       ;; client still rebuilds the URI, decoding a literal %25 into % and
       ;; throwing "Malformed escape pair" -- every crt.sh fetch failed.
       (let [resp (http/get url
                            (cond-> {:client (or client http-client)
                                     :headers {"User-Agent" ua "Accept" accept}
                                     :throw false
                                     :timeout (* timeout 1000)}
                              (seq query-params) (assoc :query-params query-params)))]
         [(:status resp) (:body resp)])))))

(defn http-get-curl
  "GET via the curl binary (following redirects), for hosts whose WAF
  rejects the JVM HTTP client (publicadministration.un.org answers 400 to
  it regardless of headers). Same outcome and retry policy as http-get:
  returns the body string on a 2xx, nil otherwise. Honors :timeout
  (seconds, default 30) and :retries (default 3)."
  ([url] (http-get-curl url {}))
  ([url {:keys [timeout retries] :or {timeout 30 retries 3}}]
   (with-retries retries
     (fn []
       ;; the status code travels on a last line appended to the body
       (let [{:keys [exit out]} (proc/sh "curl" "-sL" "--max-time" (str timeout)
                                         "-A" ua "-w" "\n%{http_code}" url)
             i    (str/last-index-of (str out) "\n")
             code (when (and (zero? exit) i) (parse-long (subs out (inc i))))]
         [code (when i (subs out 0 i))])))))

(defn truncate [s n]
  (if (> (count s) n) (str (subs s 0 (- n 3)) "...") s))

(defn parallel
  "Read PARALLEL env var, fall back to default-n."
  [default-n]
  (let [v (System/getenv "PARALLEL")]
    (if (and v (re-matches #"\d+" v)) (Integer/parseInt v) default-n)))

(defn confirmed-domains
  "Every confirmed domain of every country, whatever its level (local
  included): a set, from the cached rows. A host equal to or under one
  of them is already covered, so the candidate channels and the
  Wikidata gap list drop it: re-listing confirmed domains would only add
  noise to the manual validation pass. Deliberately world-wide: Wikidata
  attributes embassies to their host country (eda.admin.ch under
  Zimbabwe), and only the Swiss root covers them."
  []
  (set (map second (confirmed-rows))))

(defn harvest-roots
  "The confirmed domains of a country that stand for a subtree to
  harvest and probe: its central and central-1 rows (harvest-levels).
  local rows only carve labels out of a root."
  [country-dir]
  (for [[c d level] (confirmed-rows)
        :when (and (= c country-dir) (contains? harvest-levels level))]
    d))

(defn host-suffixes
  "host and every parent domain of it, at label boundaries:
  culture.gouv.fr -> (culture.gouv.fr gouv.fr fr)."
  [host]
  (let [parts (str/split host #"\.")]
    (map #(str/join "." (drop % parts)) (range (count parts)))))

(defn host-covered?
  "True when host is one of the known domains or sits under one of them.
  Walks host's suffixes with one set lookup each instead of scanning known
  (2851 confirmed domains times 40 000 candidates made report take 40 s)."
  [host known]
  (let [known (if (set? known) known (set known))]
    (boolean (some known (host-suffixes host)))))

#!/usr/bin/env bb
;; world-gov-domain-names -- full pipeline (Babashka).
;;
;; Main commands:
;;   collect            crt.sh harvest + normalize + probe + aggregate
;;   enrich             wikidata (hardened) + iana/cia/un-desa/oecd/meta (parallel)
;;   report             cross-check: score + per-country report
;;   all                collect + enrich + report
;;
;; Targeted commands:
;;   fetch [DOM…]       crt.sh fetch (1+ domains)
;;   retry [DOM…]       retry the FAILs from /tmp/fetch_subdomains.log
;;   normalize          clean every subdomains.csv
;;   probe [DOM…]       HTTPS HEAD probe of rows with empty status
;;   mx [DOM…]          DNS MX lookup per host -> mx.csv (email signal)
;;   aggregate          aggregate every host -> public-sector.csv
;;   central            extract root domains -> public-sector-central-gov.csv
;;   cisa               fetch CISA federal .gov registry -> US root registry
;;   lannuaire          fetch FR service-public.gouv.fr directory -> FR registry
;;   wikidata [Q:C…]    fetch + diff Wikidata (central administration)
;;   iana [C…]          IANA ccTLD registry
;;   cia [C…]           Government section from factbook.json
;;   un-desa [C…]       UN/DESA national portal + EGDI
;;   oecd [C…]          OECD membership flag
;;   meta [C…]          country metadata (REST Countries + World Bank GDP)
;;   cross-check [C…]   alias of report
;;   build-qid          (re)build data/country_qid.csv
;;
;; Environment variables:
;;   FORCE=1            force-overwrite existing outputs
;;   PARALLEL           # concurrent requests (fetch/probe)
;;   TIMEOUT            HTTPS request timeout in seconds (probe), default 5s

(ns pipeline
  (:require [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ===========================================================================
;;  Config & helpers
;; ===========================================================================

(def ua "world-gov-domain-names/0.1 (https://github.com/bzg)")
(def force? (= "1" (System/getenv "FORCE")))

;; Per-source concurrency limits. Each thread fires HTTP requests against
;; the same endpoint; numbers are picked to stay well under typical rate
;; limits while still saturating bandwidth. Tunable via env vars.
(defn- env-int [name default]
  (let [v (System/getenv name)]
    (if (and v (re-matches #"\d+" v)) (Integer/parseInt v) default)))

(def conc-wikidata (env-int "CONC_WIKIDATA" 3))
(def conc-iana     (env-int "CONC_IANA"     4))
(def conc-cia      (env-int "CONC_CIA"      8))
(def conc-un-desa  (env-int "CONC_UN_DESA"  4))

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

(defn read-mx-map
  "Read a root dir's mx.csv into a {host mx} map (empty map if absent)."
  [dir]
  (let [path (str dir "/mx.csv")]
    (if (fs/exists? path)
      (into {} (for [[h mx] (rest (read-csv-raw path))] [h mx]))
      {})))

(defn country-dirs
  "All country_dir present under countries/. ASCII-sorted."
  []
  (->> (fs/list-dir "countries")
       (filter fs/directory?)
       (map (comp str fs/file-name))
       sort
       vec))

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
  (let [pool (java.util.concurrent.Executors/newFixedThreadPool (int n))]
    (try
      (->> coll
           (mapv #(.submit pool ^Callable (fn [] (f %))))
           (mapv #(.get ^java.util.concurrent.Future %)))
      (finally
        (.shutdown pool)))))

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

(defn merge-status
  "Reduce a seq of [key status] pairs into a map keeping, per key, the first
  non-blank status seen; return it as a seq of [key status] sorted by key."
  [pairs]
  (->> pairs
       (reduce (fn [m [k st]]
                 (let [cur (get m k)]
                   (if (or (nil? cur)
                           (and (str/blank? cur) (not (str/blank? st))))
                     (assoc m k st)
                     m)))
               {})
       (sort-by first)))

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
  (seconds, default 30), :retries (default 3), :query-params and :accept."
  ([url] (http-get url {}))
  ([url {:keys [timeout retries query-params accept]
         :or {timeout 30 retries 3 accept "*/*"}}]
   (loop [attempt 1]
     (let [resp (try (http/get url
                               {:client http-client
                                :headers {"User-Agent" ua "Accept" accept}
                                :query-params (or query-params {})
                                :throw false
                                :timeout (* timeout 1000)})
                     (catch Exception _ nil))]
       (if (and resp (= 200 (:status resp)) (not (str/blank? (:body resp))))
         (:body resp)
         (if (< attempt retries)
           (do (Thread/sleep (* attempt 3000))
               (recur (inc attempt)))
           nil))))))

;; ===========================================================================
;;  Phase 1 -- fetch / retry (crt.sh)
;; ===========================================================================

(defn root-domain-dirs
  "All countries/<c>/sources/roots/<root>/ directories (full paths). Promoted
  root domains live under sources/roots/, siblings-free of the enrichment
  sources (iana/cia_factbook/un_desa/oecd/wikidata/curated)."
  []
  (->> (fs/glob "countries" "*/sources/roots/*")
       (filter fs/directory?)
       (map str)))

(defn root-subdomain-csvs
  "All countries/<c>/sources/roots/<root>/subdomains.csv paths.
  Scoped to one country_dir if given."
  ([] (root-subdomain-csvs "*"))
  ([country-glob]
   (fs/glob "countries" (str country-glob "/sources/roots/*/subdomains.csv"))))

(defn resolve-dirs
  "Resolve domain names -> countries/<c>/sources/<d>/ paths. With no args,
  returns all root-domain directories (excluding the enrichment-source
  subdirectories iana/cia_factbook/un_desa/oecd/wikidata)."
  [args]
  (if (seq args)
    (vec (mapcat (fn [d]
                   (let [matches (filter fs/directory?
                                         (fs/glob "countries" (str "*/sources/roots/" d)))]
                     (if (seq matches)
                       (map str matches)
                       (do (err "ERR: no country contains '" d "'") []))))
                 args))
    (vec (root-domain-dirs))))

(defn merge-crt-csv
  "Merge an existing CSV (sub,status) with a fresh list of subdomain names
  (one per line). On duplicates, preserve the non-empty status."
  [existing-path fresh-names]
  (let [existing (when (fs/exists? existing-path)
                   (rest (read-csv-raw existing-path)))
        from-existing (for [[sub status] existing]
                        [sub (or status "")])
        from-fresh (for [n fresh-names] [n ""])]
    (merge-status (concat from-existing from-fresh))))

(defn fetch-one!
  "Fetch subdomains for basename(dir) from crt.sh and merge them into
  dir/subdomains.csv. Returns :ok or :fail."
  [dir]
  (let [domain (str (fs/file-name dir))
        out (str dir "/subdomains.csv")
        url (str "https://crt.sh/?q=%25." domain "&output=json")
        body (http-get url {:timeout 120 :retries 1 :accept "application/json"})]
    (if body
      (let [names (try
                    (->> (json/parse-string body true)
                         (map :name_value)
                         (mapcat #(str/split-lines (str %)))
                         (map #(str/replace % #"^\*\." ""))
                         (filter seq)
                         distinct
                         vec)               ; realize inside the try: cheshire
                                            ; parses top-level arrays lazily, so a
                                            ; truncated crt.sh body would otherwise
                                            ; throw JsonEOFException downstream
                    (catch Exception _ []))
            merged (merge-crt-csv out names)]
        (write-csv-file out ["subdomain" "http_status"] (map vec merged))
        (println (str "OK   " domain " (" (count merged) " lignes)"))
        :ok)
      (do (println (str "FAIL " domain))
          (when-not (fs/exists? out)
            (write-csv-file out ["subdomain" "http_status"] []))
          :fail))))

(defn parallel
  "Read PARALLEL env var, fall back to default-n."
  [default-n]
  (let [v (System/getenv "PARALLEL")]
    (if (and v (re-matches #"\d+" v)) (Integer/parseInt v) default-n)))

(def fetch-fail-log "/tmp/fetch_subdomains.log")

(defn cmd-fetch [args]
  (let [dirs    (resolve-dirs args)
        results (bounded-pmap (parallel 4) (fn [d] [d (fetch-one! d)]) dirs)
        fails   (->> results (filter #(= :fail (second %))) (map (comp str fs/file-name first)))]
    ;; Record this run's failures (fresh, never appended) so `retry` -- whether
    ;; called inside run-collect or as a standalone command -- replays exactly
    ;; the domains that just failed, not a stale log from a previous session.
    (spit fetch-fail-log (str/join "\n" (map #(str "FAIL " %) fails)))
    results))

(defn retry-one!
  [dir]
  (let [domain (str (fs/file-name dir))
        url (str "https://crt.sh/?q=%25." domain "&output=json")]
    (loop [attempt 1]
      (let [body (http-get url {:timeout 180 :retries 1 :accept "application/json"})]
        (if body
          (do (fetch-one! dir)
              (println (str "  [retry=" attempt "] OK " domain)))
          (if (< attempt 3)
            (do (Thread/sleep (* attempt 5000))
                (recur (inc attempt)))
            (println (str "FAIL " domain " after 3 attempts"))))))))

(defn cmd-retry [args]
  (let [domains (if (seq args)
                  args
                  (when (fs/exists? fetch-fail-log)
                    (->> (slurp fetch-fail-log) str/split-lines
                         (filter #(str/starts-with? % "FAIL"))
                         (map #(second (str/split % #"\s+")))
                         (filter seq))))]
    (if (empty? domains)
      (do (err "No FAIL in " fetch-fail-log " (and no argument provided).") 1)
      (do (bounded-pmap (parallel 2) retry-one! (resolve-dirs (vec domains)))
          0))))

;; ===========================================================================
;;  Phase 2 -- normalize
;; ===========================================================================

(defn valid-hostname?
  "True if h is a syntactically valid hostname: dotted labels of a-z0-9 with
  internal hyphens, at least two labels. Rejects URLs, paths, wildcards, email
  addresses and stray punctuation (spaces, quotes, commas, pipes, '?', ...)."
  [h]
  (boolean
    (and h (re-matches #"[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+" h))))

(defn normalize-csv-rows
  "For each subdomain row: lowercase; strip wildcard prefix, URL scheme, path,
  port and trailing dot; keep only syntactically valid hostnames; single-line
  the status; dedup keeping the non-empty status; ASCII sort."
  [rows]
  (let [cleaned
        (for [[dom status] rows
              :let [dom (some-> dom str/trim str/lower-case
                                (str/replace #"^\*\." "")
                                (str/replace #"^https?://" "")
                                (str/replace #"/.*$" "")
                                (str/replace #":\d+$" "")
                                (str/replace #"\.$" ""))]
              :when (valid-hostname? dom)]
          [dom (single-line (or status ""))])]
    (merge-status cleaned)))

(defn cmd-normalize [_]
  (doseq [csv (root-subdomain-csvs)
          :let [path (str csv)
                parent-name (str (fs/file-name (fs/parent csv)))]]
    (let [rows (rest (read-csv-raw path))
          before (count rows)
          normalized (normalize-csv-rows rows)
          after (count normalized)]
      (write-csv-file path ["subdomain" "http_status"]
                      (for [[d s] normalized] [d s]))
      (println (str "[" parent-name "] " before " -> " after)))))

;; ===========================================================================
;;  Phase 3 -- probe
;; ===========================================================================

(defn probe-one!
  "HTTPS HEAD via babashka.http-client. Returns [sub status] where status is the
  HTTP code (e.g. \"200\") on success or a short single-line error message on
  failure (e.g. \"UnknownHostException: foo.gov.fr\")."
  [sub timeout]
  (let [resp (try
               (http/head (str "https://" sub "/")
                          {:client http-client
                           :headers {"User-Agent" ua}
                           :throw false
                           :timeout (* timeout 1000)})
               (catch Exception e
                 (let [msg (single-line (.getMessage e))
                       cls (.getSimpleName (class e))]
                   {:err (if (str/blank? msg) cls (str cls ": " msg))})))
        code (some-> resp :status str)
        status (cond
                 (and code (not (#{"0" "000"} code))) code
                 (:err resp)                          (:err resp)
                 :else                                 "unknown error")]
    [sub status]))

(defn probe-domain! [dir timeout]
  (let [csv (str dir "/subdomains.csv")
        name (str (fs/file-name dir))]
    (when (fs/exists? csv)
      (let [all-rows (rest (read-csv-raw csv))
            to-probe (filter (fn [[_ s]] (str/blank? s)) all-rows)
            kept     (remove (fn [[_ s]] (str/blank? s)) all-rows)]
        (if (empty? to-probe)
          (println (str "[" name "] no empty-status row to probe"))
          (do
            (println (str "[" name "] " (count to-probe) " subdomains to probe"))
            (let [probed (bounded-pmap (parallel 50)
                                       #(probe-one! (first %) timeout)
                                       to-probe)]
              (write-csv-file csv ["subdomain" "http_status"]
                              (sort-by first (concat kept probed))))))))))

(defn cmd-probe [args]
  (let [timeout (Integer/parseInt (or (System/getenv "TIMEOUT") "5"))]
    (doseq [d (resolve-dirs args)] (probe-domain! d timeout))))

;; ===========================================================================
;;  Phase 3b -- MX records (email signal, never a filter)
;; ===========================================================================

(defn mx-lookup
  "MX records of a host as a single-line string (\"prio host; …\"), \"none\" if
  the host has no MX, or a short error tag. Uses the system `dig`. This is a
  recorded signal only -- it never gates inclusion in the consolidated file."
  [host]
  (let [{:keys [out exit]}
        (try (proc/sh "dig" "+short" "+time=2" "+tries=1" "MX" host)
             (catch Exception e {:exit 1 :out (.getSimpleName (class e))}))
        ;; `dig +short MX` on a CNAME host prints the CNAME chain too; keep only
        ;; genuine MX answers, which have the "<priority> <host>" shape.
        lines (->> (str/split-lines (str out))
                   (map str/trim)
                   (filter #(re-matches #"\d+\s+\S+" %)))]
    (cond
      (not (zero? (or exit 1))) "dig error"
      (seq lines)               (single-line (str/join "; " lines))
      :else                     "none")))

(defn mx-domain!
  "Look up MX for every host in dir/subdomains.csv and write dir/mx.csv
  (subdomain,mx). Reuses already-looked-up hosts unless FORCE=1."
  [dir conc]
  (let [sub-csv (str dir "/subdomains.csv")
        out     (str dir "/mx.csv")
        name    (str (fs/file-name dir))]
    (when (fs/exists? sub-csv)
      (let [hosts (->> (rest (read-csv-raw sub-csv))
                       (map first)
                       (remove str/blank?)
                       distinct)
            done  (if (and (not force?) (fs/exists? out))
                    (into {} (for [[h mx] (rest (read-csv-raw out))] [h mx]))
                    {})
            todo  (remove #(contains? done %) hosts)]
        (if (empty? todo)
          (println (str "[" name "] mx: nothing to look up"))
          (let [looked (bounded-pmap conc (fn [h] [h (mx-lookup h)]) todo)
                rows   (sort-by first (concat (map vec done) looked))]
            (write-csv-file out ["subdomain" "mx"] rows)
            (println (str "[" name "] mx: " (count todo) " looked up ("
                          (count (filter #(not (#{"none" "dig error"} (second %))) looked))
                          " with MX)"))))))))

(defn cmd-mx [args]
  (let [conc (parallel 50)]
    (doseq [d (resolve-dirs args)] (mx-domain! d conc))))

;; ===========================================================================
;;  Phase 4 -- collect_200
;; ===========================================================================

(defn- regenerate-country-subdomains!
  "Aggregate countries/<c>/sources/roots/<root>/subdomains.csv into one
  countries/<c>/subdomains.csv with columns subdomain,parent_domain,http_status."
  [country-dir]
  (let [out (str "countries/" country-dir "/subdomains.csv")
        rows (->> (root-subdomain-csvs country-dir)
                  (mapcat (fn [csv]
                            (let [parent (str (fs/file-name (fs/parent csv)))]
                              (for [[sub & rest-cols] (rest (read-csv-raw (str csv)))
                                    :when (and sub (not (str/blank? sub)))]
                                [sub parent (str/join "," (or rest-cols []))]))))
                  (sort-by first))]
    (write-csv-file out ["subdomain" "parent_domain" "http_status"] rows)))

(defn- country-meta-field
  "Read one field from countries/<c>/sources/country_data/info.csv (or \"\")."
  [country-dir field]
  (or (csv-field (country-src country-dir "country_data" "info.csv") field)
      ""))

(defn- country-meta-map
  "Map country_dir -> {:region :langs :gdp} from each country's metadata."
  []
  (into {}
        (for [c (country-dirs)]
          [c {:region (country-meta-field c "region")
              :langs  (country-meta-field c "languages")
              :gdp    (country-meta-field c "gdp_per_capita")}])))

(def public-sector-file "public-sector.csv")
(def central-gov-file   "public-sector-central-gov.csv")

(defn cmd-aggregate [_]
  (let [un-by-country (build-un-status-map)
        meta-by-country (country-meta-map)
        ;; public-sector.csv -- every harvested host (root apex AND subdomains),
        ;; regardless of HTTP/MX. Inclusion criterion: the host existed in DNS at
        ;; least once (it appeared in a source like crt.sh). http_status and mx
        ;; travel along as signals, never as filters.
        rows (->> (root-subdomain-csvs)
                  (mapcat (fn [csv]
                            (let [parent  (str (fs/file-name (fs/parent csv)))
                                  country (str (fs/file-name
                                                 (fs/parent (fs/parent
                                                   (fs/parent (fs/parent csv))))))
                                  un (get un-by-country country "member")
                                  m  (get meta-by-country country)
                                  mx-by-host (read-mx-map (fs/parent csv))]
                              ;; UN-facing output: keep UN members and observers
                              ;; only, never non-UN entities (Taiwan, Kosovo).
                              (when (not= un "non_un")
                                (for [[sub status] (rest (read-csv-raw (str csv)))
                                      :when (not (str/blank? sub))]
                                  [sub parent country un
                                   (:region m) (:langs m) (:gdp m)
                                   (or status "") (get mx-by-host sub "")])))))
                  (sort-by first)
                  distinct)]
    (write-csv-file public-sector-file
                    ["subdomain" "parent_domain" "country" "un_status"
                     "region" "languages" "gdp_per_capita" "http_status" "mx"]
                    rows)
    (doseq [c (country-dirs)] (regenerate-country-subdomains! c))
    (let [counts (frequencies (map #(nth % 3) rows))]
      (println (str "Wrote " public-sector-file " (" (count rows) " hosts)"))
      (println (str "  UN members: " (get counts "member" 0)
                    " ; observers: " (get counts "observer" 0)
                    " ; non-UN: " (get counts "non_un" 0))))))

(defn registry-roots
  "Authoritative root domains declared per country in
  sources/registry/roots.csv (column 1 = domain). Seq of [country domain].
  This is how large official lists (e.g. the US CISA federal .gov registry)
  enter the central-gov file without one directory per domain."
  []
  (for [c (country-dirs)
        :let [path (country-src c "registry" "roots.csv")]
        :when (fs/exists? path)
        row (rest (read-csv-raw path))
        :let [domain (some-> (first row) str/trim str/lower-case)]
        :when (valid-hostname? domain)]
    [c domain]))

(defn- central-root-entries
  "All [country domain http_status mx] feeding the central-gov file: one per
  promoted root directory (carrying its apex signals) plus the per-country
  registry domains. De-duplicated by [country domain], preferring the directory
  entry (which has signals)."
  []
  (let [from-dirs
        (for [dir (root-domain-dirs)
              :let [root    (str (fs/file-name dir))
                    country (str (fs/file-name
                                   (fs/parent (fs/parent (fs/parent dir)))))
                    sub-csv (str dir "/subdomains.csv")
                    apex    (when (fs/exists? sub-csv)
                              (some (fn [[sub st]] (when (= sub root) st))
                                    (rest (read-csv-raw sub-csv))))
                    mx      (get (read-mx-map dir) root "")]]
          [country root (or apex "") (or mx "")])
        from-registry (for [[c d] (registry-roots)] [c d "" ""])]
    (->> (concat from-dirs from-registry)
         (reduce (fn [m [c d s mx]]
                   (cond-> m (not (contains? m [c d])) (assoc [c d] [c d s mx])))
                 {})
         vals)))

(defn cmd-central [_]
  ;; Extract public-sector-central-gov.csv: one row per central-government root
  ;; domain. With suffix matching a root already covers all its subdomains, so
  ;; these roots are the email domains the report's regex needs. Sources: the
  ;; promoted root directories (incl. ones with no harvested host yet, e.g.
  ;; gov.ke) and the per-country registry files. UN-facing: members/observers
  ;; only. Carries the root apex's http_status/mx as signals when available.
  (let [un-by-country  (build-un-status-map)
        meta-by-country (country-meta-map)
        rows
        (->> (central-root-entries)
             (keep (fn [[country domain http mx]]
                     (let [un (get un-by-country country "member")
                           m  (get meta-by-country country)]
                       (when (not= un "non_un")
                         [domain country un (:region m) (:langs m) (:gdp m) http mx]))))
             (sort-by first))]
    (write-csv-file central-gov-file
                    ["domain" "country" "un_status"
                     "region" "languages" "gdp_per_capita" "http_status" "mx"]
                    rows)
    (println (str "Wrote " central-gov-file " (" (count rows) " central-gov root domains)"))))

(defn registrable
  "Best-effort registrable domain: the last two dot-labels of a host
  (insee.fr, culture.gouv.fr -> gouv.fr). Good enough for single-label TLDs."
  [host]
  (when host
    (let [p (str/split host #"\.")]
      (when (>= (count p) 2) (str/join "." (take-last 2 p))))))

(def cisa-federal-url
  "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv")

(defn cmd-cisa [_]
  ;; Fetch CISA's authoritative federal .gov registry and write it as the US
  ;; central-gov root registry (sources/registry/roots.csv). Every entry is a
  ;; verified US federal executive/legislative/judicial domain, so this is the
  ;; clean central-gov source for the US -- preferred over a bare 'gov' suffix,
  ;; which false-matches 'government.com', 'govtech.io', etc.
  (let [body (http-get cisa-federal-url {:timeout 60})]
    (if (str/blank? body)
      (do (err "ERR: CISA fetch failed (" cisa-federal-url ")") 1)
      (let [domains (->> (rest (csv/read-csv (java.io.StringReader. body)))
                         (map (fn [r] [(some-> (nth r 0 "") str/trim str/lower-case)
                                       (nth r 1 "")     ; Domain type
                                       (nth r 2 "")]))  ; Organization name
                         (filter #(valid-hostname? (first %)))
                         (sort-by first)
                         distinct)
            out (country-src "USA_united_states" "registry" "roots.csv")]
        (ensure-dir (fs/parent out))
        (write-csv-file out ["domain" "type" "organization"] domains)
        (println (str "Wrote " out " (" (count domains) " federal .gov domains from CISA)"))
        0))))

(def lannuaire-url
  (str "https://api-lannuaire.service-public.fr/api/explore/v2.1/catalog/"
       "datasets/api-lannuaire-administration/exports/json"))

(defn cmd-lannuaire [_]
  ;; Fetch France's official national administration directory and write the FR
  ;; root registry: distinct .fr registrable domains of the central administrations
  ;; (ministries + central services). The .fr filter drops the international-org
  ;; cross-references (imf.org, wmo.int, ...) that pollute the listed websites.
  (let [where "type_organisme=\"Administration centrale (ou Ministère)\""
        body (http-get lannuaire-url
                       {:timeout 90
                        :query-params {"select" "site_internet" "where" where}})]
    (if (str/blank? body)
      (do (err "ERR: lannuaire fetch failed") 1)
      (let [domains (->> (json/parse-string body true)
                         (mapcat (fn [r]
                                   (when-let [si (:site_internet r)]
                                     (map :valeur (try (json/parse-string si true)
                                                       (catch Exception _ nil))))))
                         (keep extract-host)
                         (keep registrable)
                         (filter #(str/ends-with? % ".fr"))
                         (filter valid-hostname?)
                         distinct
                         sort)
            out (country-src "FRA_france" "registry" "roots.csv")]
        (ensure-dir (fs/parent out))
        (write-csv-file out ["domain" "source"]
                        (for [d domains] [d "lannuaire.service-public.gouv.fr"]))
        (println (str "Wrote " out " (" (count domains) " .fr central-admin domains)"))
        0))))

;; ===========================================================================
;;  Phase 5 -- Wikidata (fetch + diff)
;; ===========================================================================

(def wikidata-endpoint "https://query.wikidata.org/sparql")

;; qid type strictness. :strict applies heavy anti-subdivision filters
;; (P1001 jurisdiction + exclude territorial entities). Useful for high-volume
;; classes that pollute with subnational entities. :light keeps only the
;; "not dissolved" filter -- needed for classes whose strict SPARQL times out
;; (parliament does timeout). The score function applies the subdivision
;; penalty downstream regardless of strictness.
(def wikidata-classes
  [["Q192350" "ministry"             :strict]
   ["Q11204"  "parliament"           :light]
   ["Q193445" "central_bank"         :light]
   ["Q35798"  "constitutional_court" :light]
   ["Q35749"  "supreme_court"        :light]])

(defn wikidata-query [class-qid country-qid strictness]
  (let [strict-filters
        (if (= strictness :strict)
          (str "  FILTER NOT EXISTS { ?org wdt:P1001 ?j . FILTER(?j != wd:" country-qid ") }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q56061 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q10864048 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q13220204 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q1799794 }\n")
          "")]
    (str "SELECT DISTINCT ?org ?orgLabel ?website WHERE {\n"
         "  ?org wdt:P31/wdt:P279* wd:" class-qid " ;\n"
         "       wdt:P17 wd:" country-qid " ;\n"
         "       wdt:P856 ?website .\n"
         "  FILTER NOT EXISTS { ?org wdt:P576 ?d }\n"
         strict-filters
         "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
         "}")))

(defn wikidata-run-query [q]
  (http-get wikidata-endpoint
            {:timeout 180 :retries 3
             :accept "application/sparql-results+json"
             :query-params {"query" q}}))

(defn known-roots
  "Already-covered roots: the basename of every root-domain directory."
  []
  (->> (root-domain-dirs)
       (map (comp str fs/file-name))
       distinct
       sort))

(defn host-covered? [host known]
  (some (fn [k] (or (= host k) (str/ends-with? host (str "." k)))) known))

(defn wikidata-write-missing! [in-path out-path]
  (let [known (known-roots)
        rows (rest (read-csv-raw in-path))
        missing (filter (fn [row]
                          (let [host (last row)]
                            (and (not (str/blank? host))
                                 (not (host-covered? host known)))))
                        rows)]
    (write-csv-file out-path ["type" "label" "website" "hostname"] missing)
    (count missing)))

(defn wikidata-process! [country-qid country-dir]
  (let [out (country-src country-dir "wikidata" "central_admin.csv")
        missing-out (country-src country-dir "wikidata" "missing_domains.csv")]
    (ensure-dir (fs/parent out))
    (if (skip? out)
      (do (println (str "=== " country-dir " (" country-qid ") : SKIP (use FORCE=1 to refetch)"))
          (wikidata-write-missing! out missing-out))
      (do
        (println (str "=== " country-dir " (" country-qid ") ==="))
        (let [all-rows
              (apply concat
                     (for [[qid type strictness] wikidata-classes
                           :let [q (wikidata-query qid country-qid strictness)
                                 body (wikidata-run-query q)]]
                       (if body
                         (try
                           (let [bindings (-> (json/parse-string body true)
                                              :results :bindings)
                                 n (count bindings)
                                 _ (err (str "  [" type "] " n " results"))]
                             (Thread/sleep 1000)
                             (for [b bindings
                                   :let [url (-> b :website :value)
                                         lbl (-> b :orgLabel :value)
                                         host (extract-host url)]
                                   :when host]
                               [type lbl url host]))
                           (catch Exception e
                             (err (str "  [" type "] parse error: " (.getMessage e)))
                             []))
                         (do (err (str "  [" type "] failed after 3 attempts")) []))))]
          (let [merged (merge-rows-union out all-rows 3)]
            (write-csv-file out ["type" "label" "website" "hostname"] merged)
            (println (str "  -> " out " (" (count merged) " entries; "
                          (count all-rows) " from this fetch, rest preserved)")))
          (let [n-miss (wikidata-write-missing! out missing-out)]
            (println (str "  -> " missing-out " (" n-miss " uncovered candidates)"))))))))

(defn cmd-wikidata [args]
  (let [pairs (cond
                (seq args)
                (mapv #(let [[qid c] (str/split % #":" 2)] [qid c]) args)

                (fs/exists? "data/country_qid.csv")
                (mapv (fn [row] [(get row "wikidata_qid") (get row "country_dir")])
                      (read-csv-file "data/country_qid.csv"))

                :else nil)]
    (if (nil? pairs)
      (do (err "ERR: data/country_qid.csv missing. Run 'bb pipeline build-qid' first")
          (System/exit 1))
      (bounded-pmap conc-wikidata
                    (fn [[qid c]] (wikidata-process! qid c))
                    pairs))))

;; ===========================================================================
;;  Phase 6 -- IANA
;; ===========================================================================

(defn iana-portal-for [country-dir]
  (let [target (country-slug country-dir)]
    (some (fn [row]
            (when (= (normalize-name (get row "Country")) target)
              (get row "Government Portal Domain")))
          (read-csv-file "data/world-governments.csv"))))

(defn iana-fetch-html [cctld]
  (http-get (str "https://www.iana.org/domains/root/db/" cctld ".html")
            {:timeout 30}))

(defn iana-extract-field [html h2]
  (when html
    (when-let [match (second
                       (re-find (re-pattern
                                  (str "<h2>" (java.util.regex.Pattern/quote h2)
                                       "</h2>[\\s\\S]*?<b>([^<]+)"))
                                html))]
      (str/trim match))))

(defn iana-extract-url [html label]
  (when html
    (second
      (re-find (re-pattern (str "<b>" (java.util.regex.Pattern/quote label)
                                ":</b> <a href=\"([^\"]+)\""))
               html))))

(defn iana-extract-whois [html]
  (when html
    (some-> (re-find #"WHOIS Server:</b>\s*([^<\s]+)" html)
            second
            str/trim)))

(defn iana-process! [country-dir]
  (let [portal (iana-portal-for country-dir)]
    (cond
      (str/blank? portal)
      (err "  [" country-dir "] no portal in data/world-governments.csv")

      :else
      (let [cctld (-> portal (str/split #"\.") last str/lower-case)
            out (country-src country-dir "iana" "cctld.csv")]
        (cond
          (or (str/blank? cctld) (< (count cctld) 2))
          (err "  [" country-dir "] invalid cctld derived from '" portal "'")

          (skip? out)
          (println (str "=== " country-dir " (." cctld ") : SKIP"))

          :else
          (do
            (println (str "=== " country-dir " (." cctld ") ==="))
            (if-let [html (iana-fetch-html cctld)]
              (let [prev     (vec (when (fs/exists? out) (second (read-csv-raw out))))
                    keep-old (fn [v i] (if (str/blank? v) (nth prev i "") v))
                    manager  (keep-old (or (iana-extract-field html "ccTLD Manager")
                                           (iana-extract-field html "Sponsoring Organisation")
                                           "") 1)
                    registry (keep-old (or (iana-extract-url html "URL for registration services") "") 2)
                    whois    (keep-old (or (iana-extract-whois html) "") 3)]
                (write-csv-file out ["cctld" "manager" "registry_url" "whois_server"]
                                [[(str "." cctld) manager registry whois]])
                (println (str "  -> " out " (manager: " (or manager "?") ")"))
                (Thread/sleep 1000))
              (err "  failed after 3 attempts for ." cctld))))))))

(defn cmd-iana [args] (iter-countries iana-process! args conc-iana))

;; ===========================================================================
;;  Phase 6 -- CIA Factbook
;; ===========================================================================

(def factbook-map-file "data/factbook_gec.csv")
(def factbook-tree-cache "/tmp/world-gov-factbook-tree.json")

(defn cia-build-map! []
  (when-not (and (fs/exists? factbook-map-file)
                 (not force?)
                 (> (dec (count (read-csv-raw factbook-map-file))) 150))
    (err "Building country_dir <-> Factbook GEC map…")
    (when (or (not (fs/exists? factbook-tree-cache)) force?)
      (when-let [body (http-get "https://api.github.com/repos/factbook/factbook.json/git/trees/master?recursive=1"
                                {:timeout 30 :accept "application/json"})]
        (spit factbook-tree-cache body)))
    (let [tree (-> (slurp factbook-tree-cache)
                   (json/parse-string true)
                   :tree)
          json-paths (->> tree
                          (filter #(str/ends-with? (:path %) ".json"))
                          (map :path))
          region-by-gec (into {}
                              (for [p json-paths
                                    :let [[region file] (str/split p #"/")
                                          gec (str/replace file #"\.json$" "")]]
                                [gec region]))
          summary (or (http-get "https://raw.githubusercontent.com/factbook/factbook.json/master/SUMMARY.md")
                      "")
          pairs (for [[_ gec name] (re-seq #"`([a-z]+)` ([^`\n]+)" summary)]
                  [gec name (normalize-name name)])
          slug->dir (into {} (for [c (country-dirs)] [(country-slug c) c]))
          matched (for [[gec _name norm] pairs
                        :let [dir (get slug->dir norm)
                              region (get region-by-gec gec)]
                        :when (and dir region)]
                    [dir gec region])
          aliases (rest (or (read-csv-raw "data/factbook_aliases.csv") []))
          dedup (dedup-by-first (concat matched aliases))]
      (write-csv-file factbook-map-file ["country_dir" "gec" "region"] dedup)
      (err "  -> " factbook-map-file " (" (count dedup) " countries mapped)"))))

(defn decode-html-entities [s]
  (when s
    (-> s
        (str/replace #"&#x([0-9a-fA-F]+);" (fn [[_ hex]] (str (char (Integer/parseInt hex 16)))))
        (str/replace #"&#(\d+);" (fn [[_ n]] (str (char (Integer/parseInt n)))))
        (str/replace #"&amp;" "&")
        (str/replace #"&lt;" "<")
        (str/replace #"&gt;" ">")
        (str/replace #"&quot;" "\"")
        (str/replace #"&#39;" "'")
        (str/replace #"&eacute;" "é") (str/replace #"&egrave;" "è")
        (str/replace #"&ecirc;" "ê")  (str/replace #"&agrave;" "à")
        (str/replace #"&acirc;" "â")  (str/replace #"&ccedil;" "ç")
        (str/replace #"&ouml;" "ö")   (str/replace #"&auml;" "ä")
        (str/replace #"&uuml;" "ü")   (str/replace #"&ntilde;" "ñ")
        (str/replace #"<[^>]+>" "")
        (str/replace #"\s+" " ")
        str/trim)))

(defn cia-extract [gov-json paths]
  (loop [m gov-json [k & ks] paths]
    (cond
      (nil? k) (-> m :text decode-html-entities)
      (map? m) (recur (get m k) ks)
      :else nil)))

(def cia-fields
  [["country_name"            ["Country name" "conventional long form"]]
   ["government_type"         ["Government type"]]
   ["capital"                 ["Capital" "name"]]
   ["chief_of_state"          ["Executive branch" "chief of state"]]
   ["head_of_government"      ["Executive branch" "head of government"]]
   ["legislature"             ["Legislative branch" "description"]]
   ["judicial_highest_courts" ["Judicial branch" "highest court(s)"]]
   ["constitution_history"    ["Constitution" "history"]]])

(defn cia-process! [country-dir]
  (let [map-row (mapping-row factbook-map-file country-dir)]
    (if-not map-row
      (err "  [" country-dir "] no Factbook GEC mapping")
      (let [[_ gec region] map-row
            raw (country-src country-dir "cia_factbook" "government.json")
            out (country-src country-dir "cia_factbook" "summary.csv")]
        (ensure-dir (fs/parent raw))
        (if (skip? raw)
          (println (str "=== " country-dir " (" gec ") : SKIP"))
          (do
            (println (str "=== " country-dir " (" gec ", " region ") ==="))
            (let [url (str "https://raw.githubusercontent.com/factbook/factbook.json/master/"
                           region "/" gec ".json")
                  body (http-get url {:timeout 30 :accept "application/json"})]
              (if (str/blank? body)
                (err "  fetch failed")
                (let [parsed (json/parse-string body true)
                      gov (:Government parsed)]
                  (if (nil? gov)
                    (err "  no Government section in response")
                    (do (spit raw (json/generate-string gov {:pretty true}))
                        (write-csv-file
                          out ["field" "text"]
                          (merge-field-rows
                            out
                            (for [[k path] cia-fields
                                  :let [v (cia-extract gov (map keyword path))]
                                  :when (not (str/blank? v))]
                              [k v])))
                        (println (str "  -> " raw " + " out))
                        (Thread/sleep 1000))))))))))))

(defn cmd-cia [args]
  (cia-build-map!)
  (iter-countries cia-process! args conc-cia))

;; ===========================================================================
;;  Phase 6 -- UN/DESA
;; ===========================================================================

(def un-desa-map-file "data/un_desa_ids.csv")

(defn un-desa-build-map! []
  (when-not (and (fs/exists? un-desa-map-file)
                 (not force?)
                 (> (dec (count (read-csv-raw un-desa-map-file))) 150))
    (err "Building country_dir <-> UN/DESA id map…")
    (let [body (or (http-get "https://publicadministration.un.org/egovkb/en-us/Data-Center"
                             {:timeout 30})
                   "")
          pairs (->> (re-seq #"/Data/Country-Information/id/(\d+)-([A-Za-z-]+)" body)
                     (map (fn [[_ id name]]
                            [id name (normalize-name name)]))
                     distinct)
          slug->dir (into {} (for [c (country-dirs)] [(country-slug c) c]))
          matched (for [[id name norm] pairs
                        :let [dir (get slug->dir norm)]
                        :when dir]
                    [dir id name])
          aliases (rest (or (read-csv-raw "data/un_desa_aliases.csv") []))
          dedup (dedup-by-first (concat matched aliases))]
      (write-csv-file un-desa-map-file ["country_dir" "un_id" "un_name"] dedup)
      (err "  -> " un-desa-map-file " (" (count dedup) " countries mapped)"))))

(defn un-desa-process! [country-dir]
  (let [map-row (mapping-row un-desa-map-file country-dir)]
    (if-not map-row
      (err "  [" country-dir "] no UN/DESA id mapping")
      (let [[_ un-id un-name] map-row
            out (country-src country-dir "un_desa" "summary.csv")]
        (ensure-dir (fs/parent out))
        (if (skip? out)
          (println (str "=== " country-dir " (UN id=" un-id ") : SKIP"))
          (do
            (println (str "=== " country-dir " (UN id=" un-id " " un-name ") ==="))
            (let [url (str "https://publicadministration.un.org/egovkb/en-us/Data/Country-Information/id/"
                           un-id "-" un-name)
                  html (http-get url {:timeout 30})]
              (if (str/blank? html)
                (err "  fetch failed")
                (let [portal (second (re-find #"<a href=\"([^\"]+)\">National Portal</a>" html))
                      rank (re-find #"Rank \d+ of \d+" html)
                      rows (cond-> []
                             portal (conj ["national_portal" portal])
                             rank   (conj ["egdi_rank" rank])
                             true   (conj ["source_url" url]))]
                  (write-csv-file out ["field" "text"] (merge-field-rows out rows))
                  (println (str "  -> " out " (portal: " (or portal "?")
                                ", " (or rank "no rank") ")"))
                  (Thread/sleep 1000))))))))))

(defn cmd-un-desa [args]
  (un-desa-build-map!)
  (iter-countries un-desa-process! args conc-un-desa))

;; ===========================================================================
;;  Phase 6 -- OECD
;; ===========================================================================

(def oecd-gag-url "https://www.oecd.org/en/topics/government-at-a-glance.html")
(def oecd-sdmx-url "https://sdmx.oecd.org/public/rest/dataflow/OECD.GOV.GIP/DSD_GOV@DF_GOV_2025")

;; 38 members as of May 2026 (latest accession: Croatia 2025).
(def oecd-members
  {"AUS" 1971 "AUT" 1961 "BEL" 1961 "CAN" 1961 "CHL" 2010 "COL" 2020
   "CRI" 2021 "CZE" 1995 "DNK" 1961 "EST" 2010 "FIN" 1969 "FRA" 1961
   "DEU" 1961 "GRC" 1961 "HRV" 2025 "HUN" 1996 "ISL" 1961 "IRL" 1961
   "ISR" 2010 "ITA" 1962 "JPN" 1964 "KOR" 1996 "LVA" 2016 "LTU" 2018
   "LUX" 1961 "MEX" 1994 "NLD" 1961 "NZL" 1973 "NOR" 1961 "POL" 1996
   "PRT" 1961 "SVK" 2000 "SVN" 2010 "ESP" 1961 "SWE" 1961 "CHE" 1961
   "TUR" 1961 "GBR" 1961 "USA" 1961})

(defn oecd-process! [country-dir]
  (let [iso3 (first (str/split country-dir #"_"))
        out (country-src country-dir "oecd" "membership.csv")
        since (get oecd-members iso3)]
    (ensure-dir (fs/parent out))
    (cond
      (skip? out)
      (println (str "=== " country-dir " : SKIP"))

      since
      (do (write-csv-file out ["field" "text"]
                          [["oecd_member" "yes"]
                           ["member_since" (str since)]
                           ["gov_at_a_glance" oecd-gag-url]
                           ["sdmx_dataflow" oecd-sdmx-url]])
          (println (str "=== " country-dir " : OECD member (since " since ")")))

      :else
      (do (write-csv-file out ["field" "text"] [["oecd_member" "no"]])
          (println (str "=== " country-dir " : non-member"))))))

(defn cmd-oecd [args] (iter-countries oecd-process! args))

;; ===========================================================================
;;  Phase 6 -- Country metadata (REST Countries + World Bank)
;; ===========================================================================

(def conc-meta (env-int "CONC_META" 4))

(defn meta-rest-countries
  "Fetch region/subregion/languages/currency/population/capital for an ISO3
  code from restcountries.com. Returns a map or nil."
  [iso3]
  (let [body (http-get (str "https://restcountries.com/v3.1/alpha/" iso3
                            "?fields=region,subregion,languages,currencies,population,capital")
                       {:timeout 30 :accept "application/json"})]
    (when body
      (try
        (let [d (json/parse-string body true)]
          {:region     (or (:region d) "")
           :subregion  (or (:subregion d) "")
           :languages  (->> (vals (:languages d)) (str/join "; "))
           :currencies (->> (:currencies d) keys (map name) (str/join "; "))
           :population (str (or (:population d) ""))
           :capital    (->> (:capital d) (str/join "; "))})
        (catch Exception _ nil)))))

(defn meta-world-bank-gdp
  "Fetch most recent GDP per capita (current US$, NY.GDP.PCAP.CD) for an ISO3
  code from the World Bank API. Returns [value year] or nil."
  [iso3]
  (let [body (http-get (str "https://api.worldbank.org/v2/country/" iso3
                            "/indicator/NY.GDP.PCAP.CD?format=json&mrnev=1")
                       {:timeout 30 :accept "application/json"})]
    (when body
      (try
        (let [entry (some-> (json/parse-string body true) second first)]
          (when (:value entry)
            [(:value entry) (:date entry)]))
        (catch Exception _ nil)))))

(defn meta-process! [country-dir]
  (let [iso3 (first (str/split country-dir #"_"))
        out  (country-src country-dir "country_data" "info.csv")]
    (ensure-dir (fs/parent out))
    (if (skip? out)
      (println (str "=== " country-dir " : SKIP"))
      (let [rc  (meta-rest-countries iso3)
            gdp (meta-world-bank-gdp iso3)
            [gdp-val gdp-year] gdp]
        (write-csv-file
          out ["field" "value"]
          (merge-field-rows
            out
            [["region"          (or (:region rc) "")]
             ["subregion"       (or (:subregion rc) "")]
             ["languages"       (or (:languages rc) "")]
             ["currencies"      (or (:currencies rc) "")]
             ["population"      (or (:population rc) "")]
             ["capital"         (or (:capital rc) "")]
             ["gdp_per_capita"  (if gdp-val (format "%.0f" (double gdp-val)) "")]
             ["gdp_year"        (or gdp-year "")]]))
        (println (str "=== " country-dir " : " (or (:region rc) "?")
                      " / GDP " (if gdp-val (format "%.0f" (double gdp-val)) "?")
                      " (" (or gdp-year "?") ")"))
        (Thread/sleep 300)))))

(defn cmd-meta [args] (iter-countries meta-process! args conc-meta))

;; ===========================================================================
;;  enrich = wikidata + (iana + cia + un-desa + oecd + meta in parallel)
;; ===========================================================================

(defn cmd-enrich
  "Run the 5 enrichment sources fully in parallel. Each source manages its
  own intra-source concurrency (see conc-wikidata, conc-iana, …).
  Logs are streamed to temp files, displayed after all sources finish."
  [args]
  (err "-> wikidata + iana + cia + un-desa + oecd (all in parallel)…")
  (let [logs (fs/create-temp-dir)
        spawn (fn [name f]
                (future
                  (try
                    (let [out-file (str logs "/" name ".log")]
                      (with-open [w (io/writer out-file)]
                        (binding [*out* w] (f args)))
                      [name :ok nil])
                    (catch Exception e
                      [name :fail (str (.getMessage e)
                                       " (" (.getName (class e)) ")")]))))
        sources [["wikidata" cmd-wikidata]
                 ["iana"     cmd-iana]
                 ["cia"      cmd-cia]
                 ["un_desa"  cmd-un-desa]
                 ["oecd"     cmd-oecd]
                 ["meta"     cmd-meta]]
        futures (mapv (fn [[n f]] (spawn n f)) sources)
        results (mapv deref futures)]
    (doseq [[name status msg] results]
      (err (str (if (= status :ok) "  ✓ " "  ✗ ") name
                (if (= status :ok) " OK"
                    (str " failed: " (or msg "(no message)"))))))
    (println "\n=== enrich summary ===")
    (doseq [[name _] sources]
      (println (str "--- " name " ---"))
      (let [log-file (str logs "/" name ".log")]
        (when (fs/exists? log-file)
          (doseq [l (take-last 10 (str/split-lines (slurp log-file)))]
            (println l)))))
    (fs/delete-tree logs)))

;; ===========================================================================
;;  Phase 7 -- cross-check (score + rapport)
;; ===========================================================================

(def gov-pattern
  #"(?i)(?:^|\.)(?:gov|bund|govt|gouv|governo|gobierno|kormany|hallinto|riksdag|presidencia|presidence|parlement|parlamento|parliament|admin)(?:\.|$)")

(def subdiv-pattern
  #"(?i)(?:departmental|départemental|departementale|départementale|regional|régional|state ministry|prefecture|préfecture|conseil général|general council|county council|provincial|county of|municipal|metropolitan|community of|communauté|comunidad autónoma|comunità|länder|bundesland|senate department|staatskanzlei|landtag|free state of|land of |bavaria|bavarian|bayer(?:ian|n)|saxony|saxon|sächs|hessian|hesse|hessisch|niedersä|niedersaechs|lower saxony|nordrhein|north rhine|westfalen|westphalia|baden-würt|baden-wuert|saarland|saarl|brandenburg|bremen ministry|free hanseatic|schleswig-holstein|mecklenburg|vorpommern|thüring|thuering|thuringia|rheinland-pfalz|rhineland-palatinate|hamburg ministry|hamburg(?:ische|er) (?:ministerium|behörde)|berlin senate|berlin(?:er) senat)")

(def federal-pattern
  #"(?i)(?:federal|national|sovereign|state of [a-z]+ federation)")

(def secondary-tlds
  {"GBR_united_kingdom" #{"scot" "wales" "im" "je" "gg" "gi" "io"}
   "DNK_denmark"        #{"fo" "gl"}
   "NLD_netherlands"    #{"aw" "cw" "sx"}})

(def multi-tlds
  #{"co.uk" "gov.uk" "ac.uk" "org.uk" "com.au" "gov.au" "org.au"
    "co.nz" "gov.nz" "com.br" "gov.br" "co.za" "gov.za"})

(defn parent-domain [host]
  (or (some #(when (str/ends-with? host (str "." %)) %) multi-tlds)
      (let [parts (str/split host #"\.")
            n (count parts)]
        (when (>= n 2)
          (str (nth parts (- n 2)) "." (last parts))))))

(defn extract-factbook-phrases
  "Split the Factbook description into phrases >= 12 chars, lowercase."
  [s]
  (when s
    (->> (-> s
             (str/replace #"\([^)]*\)" "")
             (str/replace #" or " "\n")
             (str/replace #", " "\n")
             (str/replace #";" "\n"))
         str/split-lines
         (map str/trim)
         (filter #(>= (count %) 12))
         (map str/lower-case))))

(defn read-collected-cache
  "Pre-split public-sector.csv into a map country -> seq of subdomains."
  []
  (when (fs/exists? public-sector-file)
    (->> (rest (read-csv-raw public-sector-file))
         (group-by #(nth % 2))
         (reduce-kv (fn [m k v] (assoc m k (mapv first v))) {}))))

(defn host-collected? [host subs]
  (some (fn [s] (or (= s host) (str/ends-with? s (str "." host)))) subs))

(defn score-candidate
  "Compute the 0-10 confidence score for one candidate hostname.
  Inputs:
    :host            candidate hostname (lowercased)
    :wd-count        # of Wikidata mentions (1+ per distinct entity)
    :label           pipe-joined Wikidata labels for this host
    :fb-phrases      Factbook institution phrases (lowercased)
    :un-portal-host  UN/DESA-declared national portal host (or nil)
    :cctld-primary   country's primary ccTLD (without leading dot)
    :curated?        host comes from the manually-curated source channel"
  [{:keys [host wd-count label fb-phrases un-portal-host cctld-primary curated?]}]
  (let [label (or label "")
        un?           (= host un-portal-host)
        on-cctld?     (and cctld-primary
                           (or (= host cctld-primary)
                               (str/ends-with? host (str "." cctld-primary))))
        gov-host?     (boolean (re-find gov-pattern host))
        fb-match?     (and (seq fb-phrases) (seq label)
                           (let [lc (str/lower-case label)]
                             (boolean (some #(str/includes? lc %) fb-phrases))))
        subdivision?  (and (seq label) (boolean (re-find subdiv-pattern label)))
        many-no-fed?  (and (>= wd-count 3) (seq label)
                           (not (re-find federal-pattern label)))
        score (+ (if un?          5 0)
                 (* 3 (min 2 (max 0 wd-count)))
                 (if curated?     3 0)
                 (if on-cctld?    1 0)
                 (if gov-host?    1 0)
                 (if fb-match?    2 0)
                 (if subdivision? -5 0)
                 (if many-no-fed? -5 0))]
    (-> score (max 0) (min 10))))

(defn score-candidates-for!
  "Compute countries/<c>/candidates.csv (hostname,score,sources,label).
  Aggregates Wikidata mentions, UN/DESA national portal, IANA ccTLD,
  Factbook institution names; applies subdivision penalties."
  [country-dir]
  (let [iana-path (country-src country-dir "iana" "cctld.csv")
        un-path   (country-src country-dir "un_desa" "summary.csv")
        cia-path  (country-src country-dir "cia_factbook" "summary.csv")
        wd-path   (country-src country-dir "wikidata" "central_admin.csv")
        cur-path  (country-src country-dir "curated" "central_admin.csv")
        out       (str "countries/" country-dir "/candidates.csv")
        cctld-primary
        (when (fs/exists? iana-path)
          (some-> (first (second (read-csv-raw iana-path)))    ; row 2, col 1
                  (str/replace #"^\." "")))
        un-portal      (csv-field un-path "national_portal")
        un-portal-host (extract-host un-portal)
        fb-courts      (csv-field cia-path "judicial_highest_courts")
        fb-phrases     (extract-factbook-phrases fb-courts)
        wd-rows        (when (fs/exists? wd-path) (rest (read-csv-raw wd-path)))
        wd-by-host
        (reduce (fn [m row]
                  (let [host (last row) label (second row)]
                    (if (str/blank? host)
                      m
                      (-> m
                          (update-in [host :cnt] (fnil inc 0))
                          (update-in [host :labels]
                                     (fn [ls]
                                       (let [ls (or ls [])]
                                         (if (some #{label} ls) ls (conj ls label)))))))))
                {} wd-rows)
        cur-rows       (when (fs/exists? cur-path) (rest (read-csv-raw cur-path)))
        ;; curated host -> label (schema: type,label,website,hostname,provenance)
        cur-by-host
        (reduce (fn [m row]
                  (let [host (nth row 3 nil) label (nth row 1 nil)]
                    (if (str/blank? host) m (assoc m host (or label "")))))
                {} cur-rows)
        all-hosts (cond-> (into (set (keys wd-by-host)) (keys cur-by-host))
                    un-portal-host (conj un-portal-host))
        known     (set (known-roots))
        candidates
        (->> all-hosts
             (remove #(host-covered? % known))
             (map (fn [h]
                    (let [{:keys [cnt labels] :or {cnt 0 labels []}}
                          (get wd-by-host h)
                          un? (= h un-portal-host)
                          curated? (contains? cur-by-host h)
                          cur-label (get cur-by-host h)
                          labels (cond-> labels
                                   (and curated? (seq cur-label)
                                        (not (some #{cur-label} labels)))
                                   (conj cur-label))
                          label (cond-> (str/join " | " labels)
                                  un? (str (when (seq labels) " | ")
                                           "UN/DESA national portal"))
                          sources (cond-> []
                                    un? (conj "un_desa"))
                          sources (into sources (repeat cnt "wikidata"))
                          sources (cond-> sources curated? (conj "curated"))
                          score (score-candidate
                                  {:host h :wd-count cnt :label label
                                   :fb-phrases fb-phrases
                                   :un-portal-host un-portal-host
                                   :cctld-primary cctld-primary
                                   :curated? curated?})]
                      [h score (str/join ";" sources) label])))
             (sort-by (juxt #(- (nth % 1)) first)))]
    (write-csv-file out ["hostname" "score" "sources" "label"]
                    (for [[h sc src lbl] candidates] [h (str sc) src lbl]))))

(defn truncate [s n]
  (if (> (count s) n) (str (subs s 0 (- n 3)) "...") s))

(defn- section-overview [{:keys [un-st cctld manager oecd-status oecd-since
                                  un-rank fb-govtype fb-capital n-collected
                                  region subregion languages population
                                  gdp-per-capita gdp-year currencies]}]
  (println "## Overview")
  (println)
  (case un-st
    "member"   (println "- UN status: **Member State**")
    "observer" (println "- UN status: **Observer State** (include as observer, not as member)")
    "non_un"   (println "- UN status: **Not recognised by the UN** (exclude from UN-facing report)")
    nil)
  (when (and region (seq region))
    (println (str "- Region: " region
                  (when (seq subregion) (str " / " subregion)))))
  (when (and languages (seq languages)) (println (str "- Languages: " languages)))
  (when (and population (seq population)) (println (str "- Population: " population)))
  (when (and gdp-per-capita (seq gdp-per-capita))
    (println (str "- GDP per capita: " gdp-per-capita " US$"
                  (when (seq gdp-year) (str " (" gdp-year ")")))))
  (when (and currencies (seq currencies)) (println (str "- Currencies: " currencies)))
  (when cctld     (println (str "- ccTLD: `" cctld "` (manager: " (or manager "?") ")")))
  (when (= oecd-status "yes") (println (str "- OECD: member since " oecd-since)))
  (when (= oecd-status "no")  (println "- OECD: non-member"))
  (when un-rank   (println (str "- UN/DESA EGDI: " un-rank)))
  (when fb-govtype (println (str "- Government type: " fb-govtype)))
  (when fb-capital (println (str "- Capital: " fb-capital)))
  (println (str "- Domains collected (HTTP 200): " n-collected))
  (println))

(defn- section-un-portal [country-dir un-portal collected]
  (when un-portal
    (println "## UN/DESA national portal")
    (println)
    (let [host (extract-host un-portal)]
      (println (str "- Declared: [" un-portal "](" un-portal ") (host `" host "`)"))
      (cond
        (host-collected? host collected)
        (println "- ✅ Covered by collected domains")

        :else
        (let [parent (parent-domain host)]
          (if (and parent (fs/directory? (country-src country-dir "roots" parent)))
            (println (str "- ⚠️ Exact hostname not in the 200s, but a `" parent "` root directory exists (to be probed)"))
            (println (str "- ⚠️ ABSENT -- neither `" host "` covered nor `countries/" country-dir "/sources/roots/" (or parent host) "/` directory present"))))))
    (println)))

(defn- section-factbook [{:keys [fb-chief fb-head fb-courts]}]
  (when (or fb-courts fb-chief)
    (println "## Institutions named by CIA Factbook")
    (println)
    (when fb-chief  (println (str "- Chief of state: " fb-chief)))
    (when fb-head   (println (str "- Head of government: " fb-head)))
    (when fb-courts (println (str "- Highest courts: " fb-courts)))
    (println)
    (println "(institution names usable as seeds for further research)")
    (println)))

(defn- section-candidates [cand-path]
  (when (fs/exists? cand-path)
    (let [cands (rest (read-csv-raw cand-path))]
      (println "## Candidate domains ranked by score")
      (println)
      (if (seq cands)
        (do
          (println (str (count cands) " candidate(s). Full list in [`candidates.csv`](candidates.csv)."))
          (println "Top 20 by score (0-10) -- higher = stronger cross-source evidence:")
          (println)
          (println "| score | hostname | sources | label |")
          (println "|------:|----------|---------|-------|")
          (doseq [[h sc src lbl] (take 20 cands)]
            (println (str "| " sc " | `" h "` | " src " | " (truncate (or lbl "") 80) " |"))))
        (println "No remaining candidates (every flagged institution is covered)."))
      (println))))

(defn- section-cctld-anomalies [country-dir cctld collected]
  (when cctld
    (let [primary (str/replace cctld #"^\." "")
          extras  (get secondary-tlds country-dir #{})
          accept  (set (concat [primary "eu" "com" "net" "org" "int"] extras))
          anomalies (->> collected
                         (filter (fn [s]
                                   (let [tld (last (str/split s #"\."))]
                                     (not (accept tld)))))
                         sort
                         distinct)]
      (when (seq anomalies)
        (println "## ccTLD anomalies")
        (println)
        (println (str "Domains outside `" cctld "` (allowed: common gTLDs + `"
                      (str/join " " extras) "`):"))
        (println)
        (println "```")
        (doseq [s (take 20 anomalies)]
          (println (str "." (last (str/split s #"\.")) " " s)))
        (println "```")
        (println)))))

(defn report-country!
  "Generate countries/<c>/summary.md (and candidates.csv) for one country."
  [country-dir collected-by-country un-status-by-country]
  (score-candidates-for! country-dir)
  (let [iana-path (country-src country-dir "iana" "cctld.csv")
        oecd-path (country-src country-dir "oecd" "membership.csv")
        un-path   (country-src country-dir "un_desa" "summary.csv")
        cia-path  (country-src country-dir "cia_factbook" "summary.csv")
        meta-path (country-src country-dir "country_data" "info.csv")
        cand-path (str "countries/" country-dir "/candidates.csv")
        out       (str "countries/" country-dir "/summary.md")
        [cctld manager] (when (fs/exists? iana-path)
                          (let [r (second (read-csv-raw iana-path))]
                            [(nth r 0 nil) (nth r 1 nil)]))
        ctx {:un-st          (get un-status-by-country country-dir)
             :cctld          cctld
             :manager        manager
             :region         (csv-field meta-path "region")
             :subregion      (csv-field meta-path "subregion")
             :languages      (csv-field meta-path "languages")
             :population     (csv-field meta-path "population")
             :gdp-per-capita (csv-field meta-path "gdp_per_capita")
             :gdp-year       (csv-field meta-path "gdp_year")
             :currencies     (csv-field meta-path "currencies")
             :oecd-status    (csv-field oecd-path "oecd_member")
             :oecd-since     (csv-field oecd-path "member_since")
             :un-rank        (csv-field un-path "egdi_rank")
             :fb-govtype     (csv-field cia-path "government_type")
             :fb-capital     (csv-field cia-path "capital")
             :fb-courts      (csv-field cia-path "judicial_highest_courts")
             :fb-chief       (csv-field cia-path "chief_of_state")
             :fb-head        (csv-field cia-path "head_of_government")}
        collected   (get collected-by-country country-dir [])
        un-portal   (csv-field un-path "national_portal")]
    (spit out
          (with-out-str
            (println (str "# " country-dir " -- summary"))
            (println)
            (section-overview (assoc ctx :n-collected (count collected)))
            (section-un-portal country-dir un-portal collected)
            (section-factbook ctx)
            (section-candidates cand-path)
            (section-cctld-anomalies country-dir cctld collected)))
    (println (str "=== " country-dir " -> " out))))

(defn cmd-cross-check [args]
  (let [cache (or (read-collected-cache) {})
        un-status (build-un-status-map)]
    (iter-countries #(report-country! % cache un-status) args)))

;; ===========================================================================
;;  Utilitaire -- build-qid
;; ===========================================================================

(defn cmd-build-qid [_]
  (println "Querying Wikidata…")
  (let [q "SELECT DISTINCT ?country ?iso3 WHERE { ?country wdt:P31 wd:Q6256 ; wdt:P298 ?iso3 . }"
        body (http-get wikidata-endpoint
                       {:timeout 120 :retries 3
                        :accept "application/sparql-results+json"
                        :query-params {"query" q}})]
    (if (str/blank? body)
      (do (err "ERR: empty or invalid Wikidata response") 1)
      (let [iso3->qid (->> (-> body (json/parse-string true) :results :bindings)
                           (map (fn [b]
                                  [(-> b :iso3 :value)
                                   (-> b :country :value (str/replace #"^.*/" ""))]))
                           (into {}))
            rows (for [c (country-dirs)
                       :let [iso3 (first (str/split c #"_"))
                             qid (get iso3->qid iso3)]
                       :when qid]
                   [c iso3 qid])]
        (write-csv-file "data/country_qid.csv"
                        ["country_dir" "iso3" "wikidata_qid"] rows)
        (println (str "Wrote data/country_qid.csv (" (count rows) " countries mapped)"))
        0))))

;; ===========================================================================
;;  Dispatcher
;; ===========================================================================

(defn- run-collect [args]
  (cmd-fetch args)
  (cmd-retry [])
  (cmd-normalize nil)
  (cmd-probe args)
  (cmd-aggregate nil)
  (cmd-central nil))

(def commands
  "Map sub-command name -> handler. Used both by dispatcher and usage banner."
  {"collect"     run-collect
   "enrich"      cmd-enrich
   "report"      cmd-cross-check
   "fetch"       cmd-fetch
   "retry"       cmd-retry
   "normalize"   (fn [_] (cmd-normalize nil))
   "probe"       cmd-probe
   "mx"          cmd-mx
   "aggregate"   (fn [_] (cmd-aggregate nil))
   "central"     (fn [_] (cmd-central nil))
   "cisa"        (fn [_] (cmd-cisa nil))
   "lannuaire"   (fn [_] (cmd-lannuaire nil))
   "wikidata"    cmd-wikidata
   "iana"        cmd-iana
   "cia"         cmd-cia
   "un-desa"     cmd-un-desa
   "oecd"        cmd-oecd
   "meta"        cmd-meta
   "cross-check" cmd-cross-check
   "build-qid"   (fn [_] (cmd-build-qid nil))})

(defn usage []
  (println "Usage: bb scripts/pipeline.clj <command> [args…]")
  (println)
  (println "Main commands:")
  (println "  collect | enrich | report | all")
  (println)
  (println "Targeted commands:")
  (println "  fetch | retry | normalize | probe | mx | aggregate | central")
  (println "  cisa | lannuaire")
  (println "  wikidata | iana | cia | un-desa | oecd | meta | cross-check | build-qid")
  (println)
  (println "Environment variables: FORCE=1, PARALLEL=N, TIMEOUT=Ns"))

(defn dispatch [cmd args]
  (cond
    (= cmd "all") (do (run-collect args) (cmd-enrich args) (cmd-cross-check args))
    (#{"-h" "--help" "help"} cmd) (usage)
    :else
    (if-let [f (get commands cmd)]
      (f args)
      (do (err "ERR: unknown sub-command '" cmd "'")
          (usage)
          (System/exit 1)))))

(let [args *command-line-args*]
  (if (empty? args)
    (do (usage) (System/exit 1))
    (dispatch (first args) (vec (rest args)))))

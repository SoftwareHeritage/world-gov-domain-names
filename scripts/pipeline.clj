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
;;   normalize          clean every harvest file (sources/crtsh/<root>.csv)
;;   probe [DOM…]       HTTPS HEAD probe of rows with empty status; a full
;;                      run also covers the unharvested validated roots
;;   mx [DOM…]          DNS MX lookup per host -> mx column (email signal);
;;                      a full run also covers the validated roots that have
;;                      no harvest file (sources/probes/roots.csv)
;;   probe-proposed [C…] HTTPS HEAD + MX of the proposed hosts
;;                      -> sources/probes/proposed.csv, then proposed.csv
;;   aggregate          aggregate every host -> data/public-sector-domains.csv
;;   central            extract central + central-1 domains
;;                      -> data/public-sector-domains-central+.csv
;;   cisa               fetch CISA federal .gov registry -> sources/cisa/ + validated.csv
;;   lannuaire          fetch FR service-public.gouv.fr directory -> sources/lannuaire/ + validated.csv
;;   govuk              build UK sub-central exclusions -> GBR sources/govuk/
;;   wikidata [Q:C…]    fetch + diff Wikidata (central administration)
;;   iana [C…]          IANA ccTLD registry
;;   cia [C…]           Government section from factbook.json
;;   un-desa [C…]       UN/DESA national portal + EGDI
;;   oecd [C…]          OECD membership flag
;;   meta [C…]          country metadata (REST Countries + World Bank GDP)
;;   cross-check [C…]   alias of report
;;   build-qid          (re)build data/country_qid.csv
;;   validate-un        check un_status against the official UN member list
;;   domains            match policy table -> data/public-sector-domains-central+-policy.csv
;;   indegree [C…]      link-graph in-degree (eu-plus-government-scans)
;;                      -> countries/<c>/sources/linkgraph/indegree.csv
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
            [clj-yaml.core :as yaml]
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
(defn- env-int [env-name default]
  (let [v (System/getenv env-name)]
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
  (let [pool (java.util.concurrent.Executors/newFixedThreadPool (int n))
        ;; convey dynamic bindings (*out* rebinding in cmd-enrich's log
        ;; capture...) to the pool threads; a bare fn would print to the
        ;; real stdout instead
        g (bound-fn* f)]
    (try
      (->> coll
           (mapv #(.submit pool ^Callable (fn [] (g %))))
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
         (and status (<= 400 status 499) (not= 429 status))
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

;; ===========================================================================
;;  Harvest files -- countries/<c>/sources/crtsh/<root>.csv
;; ===========================================================================
;;
;; One file per harvested root domain, columns subdomain,http_status,mx:
;; every host crt.sh ever saw under the root, the root's apex included,
;; with the HTTPS probe and the MX lookup of each. Harvesting a root is a
;; choice distinct from validating it: `fetch <root>` creates the file of a
;; root listed in some validated.csv; validated roots without a file
;; (registry roots, mostly) only get their apex probed, into
;; countries/<c>/sources/probes/roots.csv (domain,http_status,mx).

(def harvest-header ["subdomain" "http_status" "mx"])

(defn harvest-files
  "All harvest files (paths as strings), scoped to one country_dir glob."
  ([] (harvest-files "*"))
  ([country-glob]
   (->> (fs/glob "countries" (str country-glob "/sources/crtsh/*.csv"))
        (map str)
        sort)))

(defn harvest-root
  "The root domain a harvest file covers: its basename."
  [file]
  (str/replace (str (fs/file-name file)) #"\.csv$" ""))

(defn harvest-country
  "The country_dir a harvest file belongs to."
  [file]
  (str (fs/file-name (fs/parent (fs/parent (fs/parent file))))))

(defn harvest-file
  "Path of the harvest file of root in country_dir (existing or not)."
  [country-dir root]
  (country-src country-dir "crtsh" (str root ".csv")))

(defn read-harvest
  "Rows [host http_status mx] of a harvest file; empty when absent."
  [file]
  (for [[h st mx] (rest (read-csv-raw file))
        :when (not (str/blank? h))]
    [h (or st "") (or mx "")]))

(defn write-harvest!
  "Write [host http_status mx] rows to a harvest file: apex row ensured,
  hosts deduped keeping the first non-blank status and mx, ASCII sort."
  [file rows]
  (let [merged (reduce (fn [m [h st mx]]
                         (update m h (fn [[s0 m0]]
                                       [(if (str/blank? s0) (or st "") s0)
                                        (if (str/blank? m0) (or mx "") m0)])))
                       {} (cons [(harvest-root file) "" ""] rows))]
    (write-csv-file file harvest-header
                    (for [[h [st mx]] (sort-by first merged)] [h st mx]))))

(def probes-header ["domain" "http_status" "mx"])

(defn probes-file [country-dir] (country-src country-dir "probes" "roots.csv"))

(defn read-probes
  "{domain [http_status mx]} of countries/<c>/sources/probes/roots.csv: the
  probes of the validated roots that have no harvest file."
  [country-dir]
  (into {} (for [[d st mx] (rest (read-csv-raw (probes-file country-dir)))
                 :when (not (str/blank? d))]
             [d [(or st "") (or mx "")]])))

(defn write-probes! [country-dir probes]
  (write-csv-file (probes-file country-dir) probes-header
                  (for [[d [st mx]] (sort-by first probes)] [d st mx])))

(defn proposed-probes-file [country-dir] (country-src country-dir "probes" "proposed.csv"))

(defn read-proposed-probes
  "{hostname [http_status mx]} of countries/<c>/sources/probes/proposed.csv:
  the probes of the hosts proposed for validation."
  [country-dir]
  (into {} (for [[h st mx] (rest (read-csv-raw (proposed-probes-file country-dir)))
                 :when (not (str/blank? h))]
             [h [(or st "") (or mx "")]])))

(defn write-proposed-probes! [country-dir probes]
  (write-csv-file (proposed-probes-file country-dir) ["hostname" "http_status" "mx"]
                  (for [[h [st mx]] (sort-by first probes)] [h st mx])))

(defn unharvested-roots
  "Validated domains of a country (any level) that have no harvest file:
  their apex is all we know, probed through sources/probes/roots.csv."
  [country-dir]
  (for [[c d] (validated-rows)
        :when (and (= c country-dir) (not (fs/exists? (harvest-file c d))))]
    d))

(defn root-probes
  "[http_status mx] of a validated root: from its harvest file's apex row
  when harvested, else from the probes file; [\"\" \"\"] when unknown."
  [country-dir root]
  (if (fs/exists? (harvest-file country-dir root))
    (or (some (fn [[h st mx]] (when (= h root) [st mx]))
              (read-harvest (harvest-file country-dir root)))
        ["" ""])
    (get (read-probes country-dir) root ["" ""])))

(defn resolve-harvest-files
  "Domain names -> harvest file paths. Existing files first; a root with
  none must be listed in some validated.csv, which tells its country --
  the file is then created by the caller (fetch). With no args, every
  existing harvest file."
  [args]
  (if (seq args)
    (vec (mapcat (fn [d]
                   (let [d (str/lower-case d)
                         existing (map str (fs/glob "countries" (str "*/sources/crtsh/" d ".csv")))
                         countries (for [[c domain] (validated-rows) :when (= domain d)] c)]
                     (cond
                       (seq existing) existing
                       (seq countries) (map #(harvest-file % d) countries)
                       :else (do (err "ERR: '" d "' is in no validated.csv -- validate it first") []))))
                 args))
    (vec (harvest-files))))

;; ===========================================================================
;;  Phase 1 -- fetch / retry (crt.sh)
;; ===========================================================================

(defn fetch-one!
  "Fetch the subdomains of a harvest file's root from crt.sh and merge them
  into the file (probes of known hosts preserved). Returns :ok or :fail."
  [file]
  (let [domain (harvest-root file)
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
            rows (concat (read-harvest file) (for [n names] [n "" ""]))]
        (write-harvest! file rows)
        (println (str "OK   " domain " (" (count (read-harvest file)) " lignes)"))
        :ok)
      (do (println (str "FAIL " domain))
          (when-not (fs/exists? file)
            (write-harvest! file []))
          :fail))))

(defn parallel
  "Read PARALLEL env var, fall back to default-n."
  [default-n]
  (let [v (System/getenv "PARALLEL")]
    (if (and v (re-matches #"\d+" v)) (Integer/parseInt v) default-n)))

(def fetch-fail-log "/tmp/fetch_subdomains.log")

(defn cmd-fetch [args]
  (let [files   (resolve-harvest-files args)
        results (bounded-pmap (parallel 4) (fn [f] [f (fetch-one! f)]) files)
        fails   (->> results (filter #(= :fail (second %))) (map (comp harvest-root first)))]
    ;; Record this run's failures (fresh, never appended) so `retry` -- whether
    ;; called inside run-collect or as a standalone command -- replays exactly
    ;; the domains that just failed, not a stale log from a previous session.
    (spit fetch-fail-log (str/join "\n" (map #(str "FAIL " %) fails)))
    results))

(defn retry-one!
  [file]
  ;; loop on fetch-one! directly: probing the URL first with a separate
  ;; http-get would download the (heavy) crt.sh response twice per success
  (let [domain (harvest-root file)]
    (loop [attempt 1]
      (if (= :ok (fetch-one! file))
        (println (str "  [retry=" attempt "] OK " domain))
        (if (< attempt 3)
          (do (Thread/sleep (* attempt 5000))
              (recur (inc attempt)))
          (println (str "FAIL " domain " after " attempt " attempts")))))))

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
      (do (bounded-pmap (parallel 2) retry-one! (resolve-harvest-files (vec domains)))
          0))))

;; ===========================================================================
;;  Phase 2 -- normalize
;; ===========================================================================

(defn normalize-harvest-rows
  "For each [host status mx] row: lowercase the host; strip wildcard
  prefix, URL scheme, path, port and trailing dot; keep only syntactically
  valid hostnames; single-line the probes. Dedup and sort are left to
  write-harvest!."
  [rows]
  (for [[dom status mx] rows
        :let [dom (some-> dom str/trim str/lower-case
                          (str/replace #"^\*\." "")
                          (str/replace #"^https?://" "")
                          (str/replace #"/.*$" "")
                          (str/replace #":\d+$" "")
                          (str/replace #"\.$" ""))]
        :when (valid-hostname? dom)]
    [dom (single-line (or status "")) (single-line (or mx ""))]))

(defn cmd-normalize [_]
  (doseq [file (harvest-files)]
    (let [rows (read-harvest file)
          before (count rows)]
      (write-harvest! file (normalize-harvest-rows rows))
      (println (str "[" (harvest-root file) "] " before " -> "
                    (count (read-harvest file)))))))

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

(defn probe-domain! [file timeout]
  (let [root-name (harvest-root file)]
    (when (fs/exists? file)
      (let [rows     (read-harvest file)
            to-probe (filter (fn [[_ st]] (str/blank? st)) rows)]
        (if (empty? to-probe)
          (println (str "[" root-name "] no empty-status row to probe"))
          (do
            (println (str "[" root-name "] " (count to-probe) " subdomains to probe"))
            (let [probed (into {} (bounded-pmap (parallel 50)
                                                #(probe-one! (first %) timeout)
                                                to-probe))]
              (write-harvest! file (for [[h st mx] rows]
                                     [h (get probed h st) mx])))))))))

(defn probe-roots!
  "HTTPS HEAD probe of a country's unharvested validated roots whose
  status is still blank, into sources/probes/roots.csv."
  [country-dir timeout]
  (let [probes (read-probes country-dir)
        todo   (for [d (unharvested-roots country-dir)
                     :when (str/blank? (first (get probes d ["" ""])))]
                 d)]
    (when (seq todo)
      (println (str "[" country-dir "/probes] " (count todo) " roots to probe"))
      (let [probed (into {} (bounded-pmap (parallel 50) #(probe-one! % timeout) todo))]
        (write-probes! country-dir
                       (reduce (fn [m [d st]] (assoc m d [st (second (get m d ["" ""]))]))
                               probes probed))))))

(defn cmd-probe [args]
  (let [timeout (Integer/parseInt (or (System/getenv "TIMEOUT") "5"))]
    (doseq [f (resolve-harvest-files args)] (probe-domain! f timeout))
    ;; a domain-scoped run stays scoped; a full run also covers the
    ;; validated roots that have no harvest file
    (when (empty? args)
      (doseq [c (country-dirs)] (probe-roots! c timeout)))))

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
  "Look up MX for every host of a harvest file (apex included -- it IS the
  email domain the central-gov file watches) and fill its mx column.
  Reuses already-looked-up hosts unless FORCE=1."
  [file conc]
  (let [root-name (harvest-root file)
        rows  (read-harvest file)
        todo  (if force?
                (map first rows)
                (for [[h _ mx] rows :when (str/blank? mx)] h))]
    (if (empty? todo)
      (println (str "[" root-name "] mx: nothing to look up"))
      (let [looked (into {} (bounded-pmap conc (fn [h] [h (mx-lookup h)]) todo))]
        (write-harvest! file (for [[h st mx] rows] [h st (get looked h mx)]))
        (println (str "[" root-name "] mx: " (count todo) " looked up ("
                      (count (remove #(#{"none" "dig error"} %) (vals looked)))
                      " with MX)"))))))

(defn mx-roots!
  "Look up MX for a country's unharvested validated roots, into
  sources/probes/roots.csv: they have no harvest file, so the per-file
  pass never sees them -- yet their apexes ARE email domains the
  central-gov file watches. Reuses already-looked-up roots unless FORCE=1."
  [country-dir conc]
  (let [probes (read-probes country-dir)
        todo   (for [d (unharvested-roots country-dir)
                     :when (or force? (str/blank? (second (get probes d ["" ""]))))]
                 d)]
    (when (seq todo)
      (let [looked (bounded-pmap conc (fn [d] [d (mx-lookup d)]) todo)]
        (write-probes! country-dir
                       (reduce (fn [m [d mx]] (assoc m d [(first (get m d ["" ""])) mx]))
                               probes looked))
        (println (str "[" country-dir "/probes] mx: " (count todo) " looked up ("
                      (count (remove #(#{"none" "dig error"} (second %)) looked))
                      " with MX)"))))))

(defn cmd-mx [args]
  (let [conc (parallel 50)]
    (doseq [f (resolve-harvest-files args)] (mx-domain! f conc))
    ;; Unharvested validated roots (registry roots, mostly) have no
    ;; harvest file; cover their apexes on a full run (a domain-scoped
    ;; run stays scoped).
    (when (empty? args)
      (doseq [c (country-dirs)] (mx-roots! c conc)))))

;; ===========================================================================
;;  Phase 4 -- aggregate
;; ===========================================================================

(defn country-hosts
  "[host parent_domain http_status mx] rows of a country: every host of its
  harvest files (their root included), then the apex of every validated
  root without a harvest file, with its probes from sources/probes/ --
  unless some harvest already lists that host. ASCII-sorted by host."
  [country-dir]
  (let [harvested (for [file (harvest-files country-dir)
                        :let [parent (harvest-root file)]
                        [sub st mx] (read-harvest file)]
                    [sub parent st mx])
        seen      (set (map first harvested))
        probes    (read-probes country-dir)
        roots     (for [d (unharvested-roots country-dir)
                        :when (not (seen d))
                        :let [[st mx] (get probes d ["" ""])]]
                    [d d st mx])]
    (sort-by first (concat harvested roots))))

(defn- regenerate-country-subdomains!
  "Aggregate a country's hosts into countries/<c>/subdomains.csv
  (subdomain,parent_domain,http_status,mx): every harvested host, roots
  included, plus the apex of every validated root that has no harvest
  file (see country-hosts)."
  [country-dir]
  (write-csv-file (str "countries/" country-dir "/subdomains.csv")
                  ["subdomain" "parent_domain" "http_status" "mx"]
                  (country-hosts country-dir)))

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

(def public-sector-file "data/public-sector-domains.csv")
(def central-plus-file  "data/public-sector-domains-central+.csv")

(defn cmd-aggregate [_]
  (let [un-by-country (build-un-status-map)
        meta-by-country (country-meta-map)
        ;; data/public-sector-domains.csv -- every harvested host (root apex AND
        ;; subdomains) plus the apex of every validated root, regardless of
        ;; HTTP/MX. Inclusion criterion: the host existed in DNS at least once
        ;; (it appeared in a source like crt.sh) or is a confirmed root.
        ;; http_status and mx travel along as signals, never as filters.
        rows (->> (country-dirs)
                  (mapcat (fn [country]
                            (let [un (get un-by-country country "member")
                                  m  (get meta-by-country country)]
                              ;; UN-facing output: keep UN members and observers
                              ;; only, never non-UN entities (Taiwan, Kosovo).
                              (when (not= un "non_un")
                                (for [[sub parent status mx] (country-hosts country)]
                                  [sub parent country un
                                   (:region m) (:langs m) (:gdp m)
                                   status mx])))))
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

(defn central1-under
  "{[country root] #{label…}}: the label of every validated central-1
  domain sitting directly under a validated central root of the same
  country (sp.gov.br under gov.br -> {[BRA gov.br] #{\"sp\"}}). Such a
  domain is an apex of the policy table (exact row) and its label leaves
  the root's subtree, so no lower-tier host registered under it
  (campinas.sp.gov.br) passes as central."
  []
  (let [rows (validated-rows)
        central (set (for [[c d level] rows :when (= level "central")] [c d]))]
    (reduce (fn [m [c d level]]
              (let [[_ label root] (when (= level "central-1")
                                     (re-matches #"([a-z0-9-]+)\.(.+)" d))]
                (if (and root (contains? central [c root]))
                  (update m [c root] (fnil conj #{}) label)
                  m)))
            {} rows)))

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

(defn excluded-labels
  "Sub-central labels declared under the validated central roots of a
  country, from countries/<c>/excluded.csv (hand-curated) and every
  countries/<c>/sources/*/excluded.csv (generated, e.g. cmd-govuk), both
  with columns domain,name where domain is a full hostname
  (abingdon.gov.uk). Returns {[country root] #{label…}}: the labels
  sitting directly under a validated central root, whose whole subtree
  belongs to a lower government tier (e.g. UK councils under gov.uk).
  Excluded hostnames that do not sit under a central root are not
  labels of anything; they only keep hosts out of proposed.csv. Roots
  without an entry are level-homogeneous: everything under them is
  central government."
  []
  (let [central (set (for [[c d level] (validated-rows)
                           :when (= level "central")]
                       [c d]))]
    (->> (for [c (country-dirs)
               path (cons (str "countries/" c "/excluded.csv")
                          (map str (fs/glob (str "countries/" c "/sources")
                                            "*/excluded.csv")))
               :when (fs/exists? path)
               [domain] (rest (read-csv-raw path))
               :let [domain (some-> domain str/trim str/lower-case)
                     [_ label root] (when (valid-hostname? domain)
                                      (re-matches #"([a-z0-9-]+)\.(.+)" domain))]
               :when (contains? central [c root])]
           [[c root] label])
         (reduce (fn [m [k label]] (update m k (fnil conj #{}) label)) {}))))

(def excluded-domains
  "{country_dir #{domain…}}: every hostname of countries/<c>/excluded.csv
  and countries/<c>/sources/*/excluded.csv. A host equal to or under one
  of them is out of scope, whatever the sources say, so proposed.csv never
  lists it again: excluded.csv is where a reviewer's 'no' is recorded."
  (delay
    (into {}
          (for [c (country-dirs)]
            [c (set (for [path (cons (str "countries/" c "/excluded.csv")
                                     (map str (fs/glob (str "countries/" c "/sources")
                                                       "*/excluded.csv")))
                          :when (fs/exists? path)
                          [domain] (rest (read-csv-raw path))
                          :let [domain (some-> domain str/trim str/lower-case)]
                          :when (valid-hostname? domain)]
                      domain))]))))

(defn sub-central-labels
  "All labels to keep out of a mixed-suffix root's subtree: the excluded
  labels declared for it (exclude rows) and the labels of its validated
  central-1 domains (exact rows, see cmd-domains)."
  [excl c1-under [country domain]]
  (into (get excl [country domain] #{})
        (get c1-under [country domain])))

(defn- central-root-entries
  "All [country domain mx] feeding the central+ file's central rows: the
  level=central rows of validated.csv, with the apex MX (root-probes)."
  []
  (for [[c d level] (validated-rows)
        :when (= level "central")]
    [c d (second (root-probes c d))]))

(defn- central1-entries
  "First-tier (central-1) domains feeding the central+ file: the
  level=central-1 rows of validated.csv. Only CONFIRMED entries pass,
  mirroring the manual gate of the central level (decision of
  2026-08-17); unconfirmed central-1 candidates -- however well scored --
  stay in proposed.csv as the curation worklist.
  Returns [country domain name score sources level]."
  []
  (for [[c domain level _source name] (validated-rows)
        :when (= level "central-1")]
    [c domain name "" "registry" "central-1"]))

(defn cmd-central [_]
  ;; Extract data/public-sector-domains-central+.csv: one row per
  ;; central-government root domain (level central) plus the first-tier
  ;; bodies (level central-1, see central1-entries), for the central +
  ;; first-subdivision report scope. A root stands for all its
  ;; subdomains, so these are the email domains the report needs.
  ;; Sources: the level=central rows of countries/<c>/validated.csv and
  ;; its central-1 rows. UN-facing: members/observers only.
  ;; Carries the domain's MX as an email signal when available.
  (let [un-by-country  (build-un-status-map)
        meta-by-country (country-meta-map)
        with-country-meta (fn [country row-fn]
                            (let [un (get un-by-country country "member")
                                  m  (get meta-by-country country)]
                              (when (not= un "non_un")
                                (row-fn un m))))
        plus-rows
        (->> (concat
              (keep (fn [[country domain mx]]
                      (with-country-meta country
                        (fn [un m]
                          [domain country "central" un (:region m)
                           (:langs m) (:gdp m) "" "" "" mx])))
                    (central-root-entries))
              (keep (fn [[country domain name score sources level]]
                      (with-country-meta country
                        (fn [un m]
                          [domain country level un (:region m)
                           (:langs m) (:gdp m) name score sources
                           (second (root-probes country domain))])))
                    (central1-entries)))
             ;; one row per [domain country]: a central-1 candidate whose
             ;; domain is also a confirmed central root (via a registry)
             ;; must not appear twice; central rows come first in the
             ;; concat, so they win
             (reduce (fn [m [domain country :as row]]
                       (cond-> m
                         (not (contains? m [domain country]))
                         (assoc [domain country] row)))
                     {})
             vals
             (sort-by (juxt first #(nth % 2))))]
    (write-csv-file central-plus-file
                    ["domain" "country" "level" "un_status" "region"
                     "languages" "gdp_per_capita" "name" "score" "sources"
                     "mx"]
                    plus-rows)
    (println (str "Wrote " central-plus-file " ("
                  (count (filter #(= "central" (nth % 2)) plus-rows))
                  " central + "
                  (count (remove #(= "central" (nth % 2)) plus-rows))
                  " central-1 domains)"))))

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
  ;; Fetch CISA's authoritative federal .gov registry into sources/cisa/
  ;; roots.csv and sync the cisa rows of the US validated.csv. Every entry
  ;; is a verified US federal executive/legislative/judicial domain, so
  ;; this is the clean central-gov source for the US -- preferred over a
  ;; bare 'gov' suffix, which false-matches 'government.com', 'govtech.io'…
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
            out (country-src "USA_united_states" "cisa" "roots.csv")]
        (ensure-dir (fs/parent out))
        (write-csv-file out ["domain" "type" "organization"] domains)
        (sync-validated! "USA_united_states" "cisa"
                         (for [[d _type org] domains] [d "central" org]))
        (println (str "Wrote " out " (" (count domains) " federal .gov domains from CISA)"
                      " and synced validated.csv"))
        0))))

(def lannuaire-url
  (str "https://api-lannuaire.service-public.fr/api/explore/v2.1/catalog/"
       "datasets/api-lannuaire-administration/exports/json"))

(defn cmd-lannuaire [_]
  ;; Fetch France's official national administration directory into
  ;; sources/lannuaire/roots.csv and sync the lannuaire rows of the FR
  ;; validated.csv: distinct .fr registrable domains of the central
  ;; administrations (ministries + central services). The .fr filter drops
  ;; the international-org cross-references (imf.org, wmo.int, ...) that
  ;; pollute the listed websites.
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
            out (country-src "FRA_france" "lannuaire" "roots.csv")]
        (ensure-dir (fs/parent out))
        (write-csv-file out ["domain" "source"]
                        (for [d domains] [d "lannuaire.service-public.gouv.fr"]))
        (sync-validated! "FRA_france" "lannuaire"
                         (for [d domains] [d "central" ""]))
        (println (str "Wrote " out " (" (count domains) " .fr central-admin domains)"
                      " and synced validated.csv"))
        0))))

;; ===========================================================================
;;  Phase 5 -- Wikidata (fetch + diff)
;; ===========================================================================

(def wikidata-endpoint "https://query.wikidata.org/sparql")

;; qid type strictness. :strict excludes entities that ARE territorial units
;; (states, municipalities…) -- pure class pollution. Subnational
;; *organisations* are NOT filtered out anymore: the query captures their
;; P1001 jurisdiction and the pipeline derives a level (central vs
;; central-1 vs local): central-1 ones land in proposed.csv with their
;; level, local ones are dropped.
;; :light skips the class exclusions -- needed for classes whose strict
;; SPARQL times out (parliament does timeout).
(def wikidata-classes
  [["Q192350" "ministry"             :strict]
   ["Q11204"  "parliament"           :light]
   ["Q193445" "central_bank"         :light]
   ["Q35798"  "constitutional_court" :light]
   ["Q35749"  "supreme_court"        :light]
   ;; Government agencies cast wide (national research bodies like CNRS or
   ;; CNR are typed here, but so are subnational agencies); the
   ;; P1001-derived level sorts them out. :light -- the strict class
   ;; exclusions time out on a class this large.
   ["Q327333" "government_agency"    :light]])
   ;; Universities are out of scope (see scripts/detect-universities.clj):
   ;; the central+ scope covers central government, national research
   ;; bodies and the first tier below -- not academia.

(defn wikidata-query [class-qid country-qid strictness]
  (let [strict-filters
        (if (= strictness :strict)
          (str "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q56061 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q10864048 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q13220204 }\n"
               "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q1799794 }\n")
          "")]
    (str "SELECT DISTINCT ?org ?orgLabel ?website ?juris WHERE {\n"
         "  ?org wdt:P31/wdt:P279* wd:" class-qid " ;\n"
         "       wdt:P17 wd:" country-qid " ;\n"
         "       wdt:P856 ?website .\n"
         "  FILTER NOT EXISTS { ?org wdt:P576 ?d }\n"
         strict-filters
         "  OPTIONAL { ?org wdt:P1001 ?juris . }\n"
         "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
         "}")))

(defn wikidata-run-query [q]
  (http-get wikidata-endpoint
            {:timeout 180 :retries 3
             :accept "application/sparql-results+json"
             :query-params {"query" q}}))

(defn juris-level
  "Administrative level of an org from its P1001 jurisdiction QIDs:
  \"central\" when one of them is the country itself, \"central-1\" when one
  is a first-level subdivision of the country (Land, state, region… -- the
  P150 values of the country), \"local\" when they all point further down
  (city, county…), \"\" when the property is absent (unknown -- to be
  consolidated over time)."
  [juris-qids country-qid level1-qids]
  (cond
    (empty? juris-qids)                ""
    (contains? juris-qids country-qid) "central"
    (some level1-qids juris-qids)      "central-1"
    :else                              "local"))

(defn wikidata-subdivisions-query [country-qid]
  (str "SELECT DISTINCT ?sub ?subLabel WHERE {\n"
       "  wd:" country-qid " wdt:P150 ?sub .\n"
       "  FILTER NOT EXISTS { ?sub wdt:P576 ?d }\n"     ; skip dissolved ones
       "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
       "}"))

(defn wikidata-fetch-subdivisions!
  "Fetch the country's first-level administrative subdivisions (its P150
  values) into sources/wikidata/subdivisions_level1.csv (qid,label).
  Cached like every other source (FORCE=1 to refetch). Returns the set of
  subdivision QIDs known for the country (empty when nothing could be
  fetched)."
  [country-qid country-dir]
  (let [out (country-src country-dir "wikidata" "subdivisions_level1.csv")]
    (when-not (skip? out)
      (when-let [body (wikidata-run-query (wikidata-subdivisions-query country-qid))]
        (try
          (let [rows (->> (-> (json/parse-string body true) :results :bindings)
                          (keep (fn [b]
                                  (when-let [qid (some-> (get-in b [:sub :value])
                                                         (str/replace #"^.*/" ""))]
                                    [qid (get-in b [:subLabel :value] "")])))
                          distinct
                          (sort-by first))]
            (write-csv-file out ["qid" "label"] rows)
            (err (str "  [subdivisions] " (count rows) " first-level entities"))
            (Thread/sleep 1000))
          (catch Exception e
            (err (str "  [subdivisions] parse error: " (.getMessage e)))))))
    (if (fs/exists? out)
      (into #{} (map first (rest (read-csv-raw out))))
      #{})))

(defn- pad-row
  "Pad (or trim) a CSV row to exactly n columns."
  [n row]
  (vec (take n (concat row (repeat "")))))

(defn- upgrade-wikidata-csv!
  "Migrate a pre-level wikidata CSV (type,label,website,hostname) in place by
  appending an empty level column, so unions with 5-column rows line up."
  [path]
  (when (fs/exists? path)
    (let [[header & rows] (read-csv-raw path)]
      (when (and header (= 4 (count header)))
        (write-csv-file path (conj (vec header) "level")
                        (map #(pad-row 5 %) rows))))))

(defn- dedup-level-rows
  "One row per (type,label,website,hostname), preferring a non-blank level.
  Feed the fresh rows FIRST: a fetch that knows the level then overrides a
  stale one, while an old non-blank level survives a fetch that lost it.
  Sorted by hostname."
  [rows]
  (->> rows
       (reduce (fn [m r]
                 (let [r (pad-row 5 r)
                       k (vec (take 4 r))
                       cur (get m k)]
                   (if (or (nil? cur)
                           (and (str/blank? (nth cur 4))
                                (not (str/blank? (nth r 4)))))
                     (assoc m k r)
                     m)))
               {})
       vals
       (sort-by #(nth % 3))))

(def validated-domains
  "Every domain of every countries/<c>/validated.csv, whatever its level
  and country. A host equal to or under one of them is already covered,
  so the candidate channels and the Wikidata gap list drop it: re-listing
  confirmed domains would only add noise to the manual validation pass.
  Deliberately world-wide: Wikidata attributes embassies to their host
  country (eda.admin.ch under Zimbabwe), and only the Swiss root covers
  them."
  (delay (set (map second (validated-rows)))))

(defn host-covered? [host known]
  (some (fn [k] (or (= host k) (str/ends-with? host (str "." k)))) known))

(defn wikidata-write-missing! [in-path out-path]
  (let [known @validated-domains
        rows (rest (read-csv-raw in-path))
        missing (filter (fn [row]
                          (let [host (nth row 3 nil)]
                            (and (not (str/blank? host))
                                 (not (host-covered? host known)))))
                        rows)]
    (write-csv-file out-path ["type" "label" "website" "hostname" "level"]
                    (map #(pad-row 5 %) missing))
    (count missing)))

(defn wikidata-process! [country-qid country-dir]
  (let [out (country-src country-dir "wikidata" "central_admin.csv")
        missing-out (country-src country-dir "wikidata" "missing_domains.csv")]
    (ensure-dir (fs/parent out))
    (upgrade-wikidata-csv! out)
    (if (skip? out)
      (do (println (str "=== " country-dir " (" country-qid ") : SKIP (use FORCE=1 to refetch)"))
          ;; Still fetch the subdivision list when absent: the report phase
          ;; uses it to tell first-level bodies (central-1) from the rest.
          (wikidata-fetch-subdivisions! country-qid country-dir)
          (wikidata-write-missing! out missing-out))
      (do
        (println (str "=== " country-dir " (" country-qid ") ==="))
        (let [level1 (wikidata-fetch-subdivisions! country-qid country-dir)
              all-rows
              (apply concat
                     (for [[qid type strictness] wikidata-classes
                           :let [q (wikidata-query qid country-qid strictness)
                                 body (or (wikidata-run-query q)
                                          ;; WDQS chokes on the :strict
                                          ;; class-exclusion paths under
                                          ;; load (502): fall back to the
                                          ;; unfiltered query rather than
                                          ;; losing the class entirely.
                                          (when (= strictness :strict)
                                            (err (str "  [" type "] strict"
                                                      " query failed;"
                                                      " retrying light"))
                                            (wikidata-run-query
                                             (wikidata-query qid country-qid
                                                             :light))))]]
                       (if body
                         (try
                           (let [bindings (-> (json/parse-string body true)
                                              :results :bindings)
                                 n (count bindings)
                                 _ (err (str "  [" type "] " n " results"))]
                             (Thread/sleep 1000)
                             ;; One org can bind several ?juris (and several
                             ;; websites): aggregate per org to derive its
                             ;; level, then emit one row per distinct host.
                             (->> (group-by #(get-in % [:org :value]) bindings)
                                  (mapcat
                                    (fn [[_ bs]]
                                      (let [juris (into #{}
                                                        (keep #(some-> (get-in % [:juris :value])
                                                                       (str/replace #"^.*/" ""))
                                                              bs))
                                            level (juris-level juris country-qid level1)]
                                        (for [b bs
                                              :let [url (get-in b [:website :value])
                                                    lbl (get-in b [:orgLabel :value])
                                                    host (extract-host url)]
                                              :when host]
                                          [type lbl url host level]))))
                                  distinct))
                           (catch Exception e
                             (err (str "  [" type "] parse error: " (.getMessage e)))
                             []))
                         (do (err (str "  [" type "] failed after 3 attempts")) []))))]
          (let [existing (when (fs/exists? out) (rest (read-csv-raw out)))
                merged (dedup-level-rows (concat all-rows existing))]
            (write-csv-file out ["type" "label" "website" "hostname" "level"] merged)
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
    (if (str/blank? portal)
      (err "  [" country-dir "] no portal in data/world-governments.csv")
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
    (when-not (fs/exists? factbook-tree-cache)
      (err "ERR: factbook tree unavailable (fetch failed and no cache)")
      (throw (ex-info "factbook tree unavailable" {})))
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
          ;; aliases FIRST: the curated file must be able to correct a
          ;; wrong automatic name match, not only fill gaps
          dedup (dedup-by-first (concat aliases matched))]
      (write-csv-file factbook-map-file ["country_dir" "gec" "region"] dedup)
      (err "  -> " factbook-map-file " (" (count dedup) " countries mapped)"))))

(defn decode-html-entities [s]
  (when s
    ;; &amp; is decoded LAST among entities: decoding it first would turn a
    ;; double-encoded &amp;lt; into &lt; then <, and the tag-strip below
    ;; would swallow the text. Astral codepoints (emojis) need
    ;; Character/toChars -- (char n) throws beyond 0xFFFF.
    (-> s
        (str/replace #"&#x([0-9a-fA-F]+);"
                     (fn [[_ hex]]
                       (String. (Character/toChars (Integer/parseInt hex 16)))))
        (str/replace #"&#(\d+);"
                     (fn [[_ n]]
                       (String. (Character/toChars (Integer/parseInt n)))))
        (str/replace #"&lt;" "<")
        (str/replace #"&gt;" ">")
        (str/replace #"&quot;" "\"")
        (str/replace #"&eacute;" "é") (str/replace #"&egrave;" "è")
        (str/replace #"&ecirc;" "ê")  (str/replace #"&agrave;" "à")
        (str/replace #"&acirc;" "â")  (str/replace #"&ccedil;" "ç")
        (str/replace #"&ouml;" "ö")   (str/replace #"&auml;" "ä")
        (str/replace #"&uuml;" "ü")   (str/replace #"&ntilde;" "ñ")
        (str/replace #"&amp;" "&")
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
    (let [body (or (http-get-curl "https://publicadministration.un.org/egovkb/en-us/Data-Center"
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
                  html (http-get-curl url {:timeout 30})]
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
        ;; when BOTH fetches failed, do not write: an all-blank info.csv
        ;; would satisfy skip? on the next runs and freeze the failure
        (if (and (nil? rc) (nil? gdp))
          (err (str "=== " country-dir " : both metadata fetches failed;"
                    " not writing " out))
          (do
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
                          " (" (or gdp-year "?") ")"))))
        (Thread/sleep 300)))))

(defn cmd-meta [args] (iter-countries meta-process! args conc-meta))

;; ===========================================================================
;;  enrich = wikidata + (iana + cia + un-desa + oecd + meta in parallel)
;; ===========================================================================

(defn cmd-enrich
  "Run the enrichment sources fully in parallel. Each source manages its
  own intra-source concurrency (see conc-wikidata, conc-iana, …).
  Logs are streamed to temp files, displayed after all sources finish."
  [args]
  (err "-> wikidata + iana + cia + un-desa + oecd + meta (all in parallel)…")
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
  #"(?i)(?:departmental|départemental|departementale|départementale|regional|régional|state ministry|prefecture|préfecture|conseil général|general council|county council|provincial|county of|municipal|metropolitan|community of|communauté|comunidad autónoma|comunità|länder|bundesland|senate department|staatskanzlei|landtag|free state of|land of |bavaria|bavarian|bayer(?:ian|n)|saxony|saxon|sächs|hessian|hesse|hessisch|niedersä|niedersaechs|lower saxony|nordrhein|north rhine|westfalen|westphalia|baden-würt|baden-wuert|saarland|saarl|brandenburg|bremen ministry|free hanseatic|schleswig-holstein|mecklenburg|vorpommern|thüring|thuering|thuringia|rheinland-pfalz|rhineland-palatinate|hamburg ministry|hamburg(?:ische|er) (?:ministerium|behörde)|berlin senate|berlin(?:er) senat|state legislature|state senate|state assembly|state house of representatives|city council|city of|town of|village of|borough|township|county executive|school district|office of education|comune di|città metropolitana|citta metropolitana|provincia di|provincia autonoma|unione dei comuni|regione\b|azienda sanitaria|azienda usl|azienda ospedalier)")

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
  "Pre-split data/public-sector-domains.csv into a map country -> seq of subdomains."
  []
  (when (fs/exists? public-sector-file)
    (->> (rest (read-csv-raw public-sector-file))
         (group-by #(nth % 2))
         (reduce-kv (fn [m k v] (assoc m k (mapv first v))) {}))))

(defn host-collected? [host subs]
  (some (fn [s] (or (= s host) (str/ends-with? s (str "." host)))) subs))

(def linkgraph-min-indegree
  "Below this many distinct linking domains a host stays out of
  proposed.csv (single blogroll link, typo'd domain...)."
  3)

(defn score-candidate
  "Compute the 0-10 confidence score for one candidate hostname.
  Inputs:
    :host            candidate hostname (lowercased)
    :wd-count        # of Wikidata mentions (1+ per distinct entity)
    :label           pipe-joined Wikidata labels for this host
    :fb-phrases      Factbook institution phrases (lowercased)
    :un-portal-host  UN/DESA-declared national portal host (or nil)
    :cctld-primary   country's primary ccTLD (without leading dot)
    :indegree        # of distinct same-country public-sector domains
                     linking to the host (nil when absent from the link
                     graph); being linked from many independent government
                     sites is a strong presumption of validity
    :directory-listed? host appears in the country's official directory
                     of public bodies (detect-from-directories.clj,
                     :candidates channel): authoritative existence, unknown level
    :subdiv-penalty? apply the anti-subnational penalties (default true);
                     false for hosts already known as subnational, where
                     penalising again would only bury the central-1 rows"
  [{:keys [host wd-count label fb-phrases un-portal-host cctld-primary
           indegree directory-listed? subdiv-penalty?]
    :or {subdiv-penalty? true}}]
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
        ;; 20+ distinct linking government domains is evidence as strong as
        ;; the UN/DESA portal declaration; scaled down to +3 at the
        ;; linkgraph-min-indegree floor.
        lg-points     (cond (nil? indegree)                       0
                            (>= indegree 20)                      6
                            (>= indegree 10)                      5
                            (>= indegree 5)                       4
                            (>= indegree linkgraph-min-indegree)  3
                            :else                                 0)
        score (+ (if un?          5 0)
                 (* 3 (min 2 (max 0 wd-count)))
                 lg-points
                 ;; an official-directory listing is authoritative on the
                 ;; body's existence but silent on its level
                 (if directory-listed? 4 0)
                 (if on-cctld?    1 0)
                 (if gov-host?    1 0)
                 (if fb-match?    2 0)
                 (if (and subdiv-penalty? subdivision?) -5 0)
                 (if (and subdiv-penalty? many-no-fed?) -5 0))]
    (-> score (max 0) (min 10))))

(defn- level1-label?
  "Does the label name one of the country's first-level subdivisions?"
  [level1-pattern label]
  (boolean (and level1-pattern (seq label) (re-find level1-pattern label))))

(defn- candidate-level
  "Administrative level of a candidate host: the P1001-derived Wikidata
  levels, then the label heuristics (a label matching subdiv-pattern is
  subnational; one naming a first-level subdivision is promoted to
  central-1). Blank when nothing is known."
  [{:keys [wd-by-host level1-pattern]} h label]
  (let [wd-levels (disj (get-in wd-by-host [h :levels] #{}) "")]
    (cond
      (contains? wd-levels "central")   "central"
      (contains? wd-levels "central-1") "central-1"
      (contains? wd-levels "local")     (if (level1-label? level1-pattern label)
                                          "central-1" "local")
      (and (seq label)
           (re-find subdiv-pattern label)) (if (level1-label? level1-pattern
                                                              label)
                                             "central-1" "local")
      :else "")))

(defn- candidate-row
  "Assemble one candidate row [hostname score sources label level] from the
  already-loaded per-country context (pure -- see score-candidates-for!
  for the context construction)."
  [{:keys [wd-by-host lg-by-host dir-by-host un-portal-host
           fb-phrases cctld-primary]
    :as ctx}
   h]
  (let [{:keys [cnt labels] :or {cnt 0 labels []}} (get wd-by-host h)
        un? (= h un-portal-host)
        linked-n (get lg-by-host h)
        dir-info (get dir-by-host h)
        label (cond-> (str/join " | " labels)
                un? (str (when (seq labels) " | ")
                         "UN/DESA national portal"))
        ;; hints for the manual validation pass when the directory or the
        ;; link graph is all we know
        label (cond
                (not (str/blank? label)) label
                (seq (:evidence dir-info)) (:evidence dir-info)
                linked-n (str "Linked from " linked-n
                              " public-sector domains")
                :else label)
        sources (cond-> []
                  un? (conj "un_desa"))
        sources (into sources (repeat cnt "wikidata"))
        sources (cond-> sources
                  linked-n (conj "linkgraph")
                  dir-info (conj "directory"))
        level (candidate-level ctx h label)
        score (score-candidate
                {:host h :wd-count cnt :label label
                 :fb-phrases fb-phrases
                 :un-portal-host un-portal-host
                 :cctld-primary cctld-primary
                 :indegree linked-n
                 :directory-listed? (some? dir-info)
                 ;; the anti-subnational penalties catch entities
                 ;; *pretending* central
                 :subdiv-penalty?
                 (not (contains? #{"local" "central-1"} level))})]
    [h score (str/join ";" sources) label level]))

(defn score-candidates-for!
  "Compute countries/<c>/proposed.csv, the domains proposed for
  validation (columns hostname,score,sources,label,level,http_status,mx),
  best scores first; the two probe columns come from
  sources/probes/proposed.csv (cmd-probe-proposed) and stay blank until
  a host is probed. Aggregates Wikidata mentions
  (incl. their P1001-derived level), UN/DESA national portal, IANA ccTLD,
  Factbook institution names and link-graph in-degree (sources/linkgraph/,
  see cmd-indegree). Loads the per-country sources into a context map,
  then delegates each host to the pure candidate-row/candidate-level
  above: a host is subnational when Wikidata says so, or when
  its label matches the subdivision pattern; a subnational host whose
  jurisdiction (or label) points to a first-level subdivision of the
  country (Land, state, region…) is tagged 'central-1', the rest 'local'.
  Level stays blank when nothing is known. Hosts already validated, hosts
  under an excluded domain (see excluded-domains) and local hosts are
  left out: only central, central-1 and unknown-level hosts are proposed."
  [country-dir]
  (let [iana-path (country-src country-dir "iana" "cctld.csv")
        un-path   (country-src country-dir "un_desa" "summary.csv")
        cia-path  (country-src country-dir "cia_factbook" "summary.csv")
        wd-path   (country-src country-dir "wikidata" "central_admin.csv")
        sub-path  (country-src country-dir "wikidata" "subdivisions_level1.csv")
        out       (str "countries/" country-dir "/proposed.csv")
        excluded  (get @excluded-domains country-dir #{})
        probes    (read-proposed-probes country-dir)
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
                  (let [host  (nth row 3 nil)
                        label (nth row 1 nil)
                        level (str/trim (or (nth row 4 nil) ""))]
                    (if (str/blank? host)
                      m
                      (-> m
                          (update-in [host :cnt] (fnil inc 0))
                          (update-in [host :labels]
                                     (fn [ls]
                                       (let [ls (or ls [])]
                                         (if (some #{label} ls) ls (conj ls label)))))
                          (update-in [host :levels] (fnil conj #{}) level)))))
                {} wd-rows)
        lg-path        (country-src country-dir "linkgraph" "indegree.csv")
        ;; link-graph in-degree (see cmd-indegree): hosts linked from at
        ;; least linkgraph-min-indegree distinct same-country public-sector
        ;; domains enter the candidate pool with a strong score bonus.
        known          @validated-domains
        lg-by-host
        (reduce (fn [m {:strs [hostname indegree]}]
                  (let [n (parse-long (or indegree ""))]
                    (if (and n (>= n linkgraph-min-indegree)
                             (not (host-covered? hostname known)))
                      (assoc m hostname n)
                      m)))
                {} (read-csv-file lg-path))
        dir-path       (country-src country-dir "directory" "orgs.csv")
        ;; official-directory listing (see detect-from-directories.clj,
        ;; :candidates channel): authoritative that the body exists and is anchored to
        ;; the country's administration, silent on its level -- strong
        ;; score bonus, curation decides.
        dir-by-host
        (reduce (fn [m {:strs [hostname mentions evidence]}]
                  (if (host-covered? hostname known)
                    m
                    (assoc m hostname
                           {:n (or (parse-long (or mentions "")) 1)
                            :evidence (or evidence "")})))
                {} (read-csv-file dir-path))
        all-hosts (cond-> (-> (set (keys wd-by-host))
                              (into (keys lg-by-host))
                              (into (keys dir-by-host)))
                    un-portal-host (conj un-portal-host))
        ;; English labels of the country's first-level subdivisions, as a
        ;; word-bounded pattern. Used to promote a subnational host to
        ;; central-1 when its label names such a subdivision -- covers rows
        ;; fetched before levels carried central-1 (no refetch needed).
        level1-pattern
        (when (fs/exists? sub-path)
          (let [labels (->> (rest (read-csv-raw sub-path))
                            (map second)
                            (remove str/blank?)
                            (filter #(>= (count %) 4)))]
            (when (seq labels)
              (re-pattern
                (str "(?iu)\\b(?:"
                     (str/join "|" (map #(java.util.regex.Pattern/quote %) labels))
                     ")\\b")))))
        ctx {:wd-by-host wd-by-host
             :lg-by-host lg-by-host
             :dir-by-host dir-by-host
             :un-portal-host un-portal-host
             :fb-phrases fb-phrases
             :cctld-primary cctld-primary
             :level1-pattern level1-pattern}
        proposed
        (->> all-hosts
             (remove #(host-covered? % known))
             (remove #(host-covered? % excluded))
             (map #(candidate-row ctx %))
             (remove #(= "local" (nth % 4)))
             (sort-by (juxt #(- (nth % 1)) first)))]
    (write-csv-file out ["hostname" "score" "sources" "label" "level"
                         "http_status" "mx"]
                    (for [[h sc src lbl lvl] proposed
                          :let [[st mx] (get probes h ["" ""])]]
                      [h (str sc) src lbl lvl st mx]))))

(defn probe-proposed!
  "HTTPS HEAD and MX lookup of a country's proposed hosts (proposed.csv),
  those not probed yet unless FORCE=1, into sources/probes/proposed.csv --
  pruned to the hosts still proposed -- then proposed.csv is rewritten
  with the probe columns filled."
  [country-dir timeout conc]
  (let [path  (str "countries/" country-dir "/proposed.csv")
        hosts (for [[h] (rest (read-csv-raw path)) :when (valid-hostname? h)] h)
        known (select-keys (read-proposed-probes country-dir) hosts)
        todo-http (for [h hosts :when (or force? (str/blank? (first (get known h ["" ""]))))] h)
        todo-mx   (for [h hosts :when (or force? (str/blank? (second (get known h ["" ""]))))] h)]
    (if (and (empty? todo-http) (empty? todo-mx))
      (println (str "[" country-dir "] proposed: nothing to probe"))
      (let [http (into {} (bounded-pmap conc #(probe-one! % timeout) todo-http))
            mx   (into {} (bounded-pmap conc (fn [h] [h (mx-lookup h)]) todo-mx))
            probes (into {} (for [h hosts
                                  :let [[st0 mx0] (get known h ["" ""])]]
                              [h [(get http h st0) (get mx h mx0)]]))]
        (write-proposed-probes! country-dir probes)
        (score-candidates-for! country-dir)
        (println (str "[" country-dir "] proposed: " (count todo-http) " probed, "
                      (count todo-mx) " MX looked up ("
                      (count (filter #(re-matches #"[23]\d\d" (first %)) (vals probes)))
                      " reachable, "
                      (count (remove #(#{"" "none" "dig error"} (second %)) (vals probes)))
                      " with MX)"))))))

(defn cmd-probe-proposed [args]
  (let [timeout (Integer/parseInt (or (System/getenv "TIMEOUT") "5"))
        conc    (parallel 50)]
    (iter-countries #(probe-proposed! % timeout conc) args)))

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
          (if (and parent (fs/exists? (harvest-file country-dir parent)))
            (println (str "- ⚠️ Exact hostname not in the 200s, but `" parent "` is harvested (to be probed)"))
            (println (str "- ⚠️ ABSENT -- neither `" host "` covered nor `countries/" country-dir "/sources/crtsh/" (or parent host) ".csv` present"))))))
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

(defn- candidate-table [cands]
  (println "| score | hostname | level | http | sources | label |")
  (println "|------:|----------|-------|------|---------|-------|")
  (doseq [[h sc src lbl lvl st] cands]
    (println (str "| " sc " | `" h "` | " (or lvl "") " | "
                  (truncate (or st "") 12) " | " src " | "
                  (truncate (or lbl "") 80) " |"))))

(defn- section-proposed [path]
  (when (fs/exists? path)
    (let [cands (rest (read-csv-raw path))
          n-level1 (count (filter #(= "central-1" (nth % 4 "")) cands))]
      (println "## Proposed domains ranked by score")
      (println)
      (if (seq cands)
        (do
          (println (str (count cands) " domain(s) proposed for validation"
                        (when (pos? n-level1)
                          (str ", of which " n-level1 " at the first subdivision "
                               "level (`central-1`: Land, state, region…)"))
                        ". Full list in [`proposed.csv`](proposed.csv)."))
          (println "Top 20 by score (0-10) -- higher = stronger cross-source evidence:")
          (println)
          (candidate-table (take 20 cands)))
        (println "No remaining proposal (every flagged institution is covered)."))
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
  "Generate countries/<c>/summary.md (and proposed.csv) for one country."
  [country-dir collected-by-country un-status-by-country]
  (score-candidates-for! country-dir)
  (let [iana-path (country-src country-dir "iana" "cctld.csv")
        oecd-path (country-src country-dir "oecd" "membership.csv")
        un-path   (country-src country-dir "un_desa" "summary.csv")
        cia-path  (country-src country-dir "cia_factbook" "summary.csv")
        meta-path (country-src country-dir "country_data" "info.csv")
        prop-path  (str "countries/" country-dir "/proposed.csv")
        out        (str "countries/" country-dir "/summary.md")
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
            (section-proposed prop-path)
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
;;  UN membership validation (UN Digital Library)
;; ===========================================================================

(def un-members-record-url
  "Record page of the UN member states dataset published by the Dag
  Hammarskjöld Library. Links to a dated CSV snapshot whose name changes at
  each refresh."
  "https://digitallibrary.un.org/record/4082085")

(def redirect-http-client
  "The UN Digital Library serves record files behind a 302, which the
  default no-redirect client refuses to follow."
  (http/client (assoc http/default-client-opts
                      :follow-redirects :normal
                      :connect-timeout 15000)))

(defn un-members-csv-url
  "Full URL of the most recent member_states_auths_*.csv snapshot linked
  from the record page, or nil when unreachable."
  []
  (when-let [body (http-get un-members-record-url)]
    (some->> (re-seq #"files/(member_states_auths_[0-9-]+\.csv)" body)
             (map second)
             sort
             last
             (str un-members-record-url "/files/"))))

(defn date-count
  "Number of dates in a comma-separated list ('1945-10-24, 1971-09-02' -> 2)."
  [s]
  (count (remove str/blank? (str/split (str s) #","))))

(defn un-member-iso3s
  "Parse the UN library CSV into the set of ISO3 codes of current members.
  The file is a name authority where Start/End date hold comma-separated
  usage episodes: a name is in current use when it has more start dates
  than end dates (a plain empty-End-date test misses countries that went
  through renames, e.g. Egypt or Cambodia)."
  [body]
  (let [rows (csv/read-csv body)
        headers (first rows)]
    (into #{}
          (for [row (rest rows)
                :let [m (zipmap headers row)
                      iso (str/trim (or (get m "ISO Code") ""))]
                :when (and (not (str/blank? iso))
                           (> (date-count (get m "Start date"))
                              (date-count (get m "End date"))))]
            iso))))

(defn cmd-validate-un
  "Check the un_status column of data/world-governments.csv against the
  official UN member list. Exits 1 on any mismatch or blank un_status
  (blank would silently default to member everywhere else in the pipeline)."
  [_]
  (println "Fetching official UN member list from digitallibrary.un.org…")
  (let [url (un-members-csv-url)
        body (when url (http-get url {:client redirect-http-client}))]
    (if (str/blank? body)
      (do (err "ERR: could not fetch the UN member states CSV")
          (System/exit 1))
      (let [official (un-member-iso3s body)
            local (for [row (or (read-csv-file "data/world-governments.csv") [])]
                    {:name (get row "Country")
                     :iso (str/trim (or (get row "iso3") ""))
                     :status (str/trim (or (get row "un_status") ""))})
            local-iso3s (set (map :iso local))
            problems
            (concat
             (for [{:keys [name status]} local
                   :when (str/blank? status)]
               (str name ": blank un_status (set member/observer/non_un explicitly)"))
             (for [{:keys [name iso status]} local
                   :when (and (= status "member") (not (official iso)))]
               (str name ": marked member but absent from the official UN list"))
             (for [{:keys [name iso status]} local
                   :when (and (#{"observer" "non_un"} status) (official iso))]
               (str name ": marked " status " but the UN lists it as a member"))
             (for [iso (sort official)
                   :when (not (local-iso3s iso))]
               (str iso ": UN member missing from data/world-governments.csv")))]
        (println (str "  official members: " (count official)
                      " ; rows in world-governments.csv: " (count local)))
        (if (seq problems)
          (do (doseq [p problems] (println (str "  MISMATCH " p)))
              (err "ERR: " (count problems) " mismatch(es)")
              (System/exit 1))
          (println "  un_status is consistent with the official UN list."))))))

;; ===========================================================================
;;  Utilitaire -- govuk (UK sub-central exclusions for the gov.uk suffix)
;; ===========================================================================

(def govuk-domains-url
  ;; Official CDDO list of every registered .gov.uk domain (one column).
  ;; Snapshot asset: update the URL when a newer list is published on
  ;; https://www.gov.uk/government/publications/list-of-gov-uk-domain-names
  (str "https://assets.publishing.service.gov.uk/media/"
       "69cbd582024cdf09254f3f7d/"
       "List_of_.gov.uk_domain_names_as_of_31_March_2026_1.csv"))

(def govuk-local-label-patterns
  "Naming conventions of sub-central bodies registered directly under
  gov.uk: parish/town/community and principal councils, the -pc/-tc/…
  council suffixes, police-and-crime commissioners (-pcc), combined
  authorities (-ca), Northern Ireland devolved departments (-ni), fire
  services and national parks. fire.gov.uk and firekills.gov.uk are Home
  Office campaigns, hence the anchored fire patterns."
  [#"parish"
   #"village"
   #"(town|community|county|city|district|borough)-?council"
   #"councils"
   #"-council$"
   #"-(pc|tc|cc|bc|dc|mbc|lbc|rbc|cbc|udc|rdc|gpc|jpc|pcc|pfcc|wcc|ecc|aptc|ca|cca|ni|vjb)$"
   #"[a-z0-9](pc|tc|cc|bc|dc)$"
   #"shire$"
   #"district$"
   #"(alc|lca|ifca)$"
   #"idbs?$|drainageboards?$"
   #"waste$"
   #"foster"
   #"porthealth|crematorium"
   #".+-?fire(-?service|-?control)?$"
   #"fire-?and-?rescue"
   #"^firescotland$"
   #"nationalpark|national-park|-npa$"])

(def govuk-central-allowlist
  "Central-government labels the patterns above would wrongly exclude:
  acronym bodies ending in pc/tc/cc/bc (Regulatory Policy Committee, DECC,
  HMG Communications Centre, Joint Nature Conservation Committee, IPCC,
  Animal Procedures Committee, Agriculture and Environment Biotechnology
  Commission, ...)."
  #{"rpc" "apc" "otc" "decc" "hmgcc" "jncc" "ipcc" "aebc"})

(def govuk-extra-local-labels
  "Sub-central labels neither the patterns nor the Wikidata queries catch."
  {"nidirect" "NI Direct (Northern Ireland citizen portal)"})

(def govuk-national-gss
  ;; Country/UK-level GSS code prefixes: a body anchored to one of these is
  ;; national (Natural England, Charity Commission, NIO…), not sub-central.
  #"^(E92|S92|W92|N92|K0)")

(defn- govuk-label-of-website
  "The registrable gov.uk label of a website URL, whatever the host depth:
  https://beta.xcouncil.gov.uk/ -> xcouncil. Nil when not a gov.uk site."
  [web]
  (some-> (re-find
           #"^https?://(?:[a-z0-9-]+\.)*([a-z0-9-]+)\.gov\.uk(?:[/:?#]|$)"
           (str/lower-case (str web)))
          second))

(defn- govuk-wikidata-locals
  "{gov.uk-label name} of Wikidata entities anchored below country level in
  the UK statistical geography: the entity itself carries a GSS code
  (P836 -- council areas) or its P1001 jurisdiction does (council
  organisations). Country/UK-level GSS codes are filtered out (see
  govuk-national-gss)."
  []
  (let [label-svc "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
        queries
        [(str "SELECT ?itemLabel ?gss ?web WHERE {\n"
              "  ?item wdt:P836 ?gss ; wdt:P856 ?web .\n" label-svc "}")
         (str "SELECT ?itemLabel ?gss ?web WHERE {\n"
              "  ?item wdt:P1001 ?area ; wdt:P856 ?web .\n"
              "  ?area wdt:P836 ?gss .\n" label-svc "}")]]
    (->> queries
         (mapcat (fn [q]
                   ;; A missing (network failure) or truncated (invalid
                   ;; JSON) WDQS answer must not pass for an empty result:
                   ;; up to 3 attempts, then fail loudly rather than
                   ;; silently under-excluding.
                   (loop [attempt 1]
                     (let [body (wikidata-run-query q)
                           _ (Thread/sleep 1000)
                           parsed
                           (if (str/blank? body)
                             (do (err "  [govuk] no WDQS response"
                                      " (attempt " attempt ")")
                                 ::fail)
                             (try (-> (json/parse-string body true)
                                      :results :bindings)
                                  (catch Exception e
                                    (err "  [govuk] truncated WDQS response"
                                         " (attempt " attempt "): "
                                         (.getMessage e))
                                    ::fail)))]
                       (cond (not= parsed ::fail) parsed
                             (< attempt 3) (recur (inc attempt))
                             :else (throw (ex-info (str "WDQS query failed "
                                                        "3 times, aborting")
                                                   {})))))))
         (keep (fn [b]
                 (let [web   (get-in b [:web :value] "")
                       gss   (get-in b [:gss :value] "")
                       label (govuk-label-of-website web)]
                   (when (and label (not (re-find govuk-national-gss gss)))
                     ;; apex? -> the site sits at label.gov.uk itself, so the
                     ;; entity name is the label's canonical owner (a council)
                     ;; rather than some deeper page under its domain (a
                     ;; parish site hosted by its county)
                     [label (get-in b [:itemLabel :value] "")
                      (boolean (re-find #"^https?://(?:www\.)?[a-z0-9-]+\.gov\.uk(?:[/:?#]|$)"
                                        (str/lower-case web)))]))))
         (reduce (fn [m [label name apex?]]
                   (if (or (and apex? (not (str/blank? name)))
                           (not (contains? m label)))
                     (assoc m label name)
                     m))
                 {}))))

(defn cmd-govuk [_]
  ;; Build the GBR sub-central list: every gov.uk label belonging to a
  ;; sub-central body (councils of all tiers, combined authorities, fire
  ;; services, national parks, NI devolved departments…), so the policy
  ;; table keeps only UK central government under the gov.uk suffix.
  ;; Universe: the official CDDO list of registered gov.uk domains,
  ;; classified by Wikidata GSS anchoring plus naming conventions. Local
  ;; bodies go to sources/govuk/excluded.csv, devolved (central-1) ones
  ;; to the govuk rows of validated.csv.
  (let [body (http-get govuk-domains-url {:timeout 90})]
    (if (str/blank? body)
      (do (err "ERR: gov.uk domain list fetch failed (" govuk-domains-url ")") 1)
      (let [universe (->> (csv/read-csv (java.io.StringReader. body))
                          rest
                          (keep #(some->> (first %) str/trim str/lower-case
                                          (re-matches #"([a-z0-9-]+)\.gov\.uk")
                                          second))
                          set)
            wd (govuk-wikidata-locals)
            wd-hits (select-keys wd universe)
            pattern-hit? (fn [l] (and (not (govuk-central-allowlist l))
                                      (boolean (some #(re-find % l)
                                                     govuk-local-label-patterns))))
            named (->> (concat
                        wd-hits
                        (for [l universe :when (pattern-hit? l)] [l ""])
                        (for [[l n] govuk-extra-local-labels
                              :when (universe l)] [l n]))
                       (reduce (fn [m [l n]]
                                 (if (str/blank? (get m l)) (assoc m l n) m))
                               {})
                       (sort-by first))
            central-1? (fn [l] (or (str/ends-with? l "-ni")
                                   (contains? #{"nidirect" "firescotland"} l)))
            {devolved true local false} (group-by (comp boolean central-1? first) named)
            out (country-src "GBR_united_kingdom" "govuk" "excluded.csv")]
        (ensure-dir (fs/parent out))
        (write-csv-file out ["domain" "name"]
                        (for [[l n] local] [(str l ".gov.uk") n]))
        (sync-validated! "GBR_united_kingdom" "govuk"
                         (for [[l n] devolved] [(str l ".gov.uk") "central-1" n]))
        (println (str "Wrote " out " (" (count local) " local labels out"
                      " of " (count universe) " registered gov.uk domains; "
                      (count wd-hits) " matched via Wikidata GSS) and "
                      (count devolved) " central-1 rows into validated.csv"))
        0))))

;; ===========================================================================
;;  Utilitaire -- domains (match policy table over the central+ scope)
;; ===========================================================================

(defn- covered-by?
  "True when domain d is redundant given entry [d2 excl2 apex?]: d sits
  under d2, d2 covers its subtree (apex entries cover nothing but
  themselves) and the label chaining d to d2 is not excluded, so d2
  already covers every host under d."
  [d [d2 excl2 apex?]]
  (and (not apex?)
       (not= d d2)
       (str/ends-with? d (str "." d2))
       (let [prefix (subs d 0 (- (count d) (inc (count d2))))
             label  (last (str/split prefix #"\."))]
         (not (contains? (set excl2) label)))))

(defn- prune-covered
  "Drop [domain excl apex?] entries already covered by another entry."
  [entries]
  (filter (fn [[d _]] (not-any? #(covered-by? d %) entries)) entries))

(defn cmd-domains
  "Compile the central+ scope (data/public-sector-domains-central+.csv:
  central roots plus first-tier bodies) into the match policy table
  data/public-sector-domains-central+-policy.csv (domain,kind,country):
  `subtree` is a domain and everything below it, `exact` is a domain
  alone (nothing below it), `exclude` is a domain and everything below
  it to keep out. When a host falls under several rows
  (campinas.sp.gov.br is under gov.br subtree and under sp.gov.br
  exact), the most specific row, the one with the longest domain,
  decides. The table needs no lookaround and no regex engine, so any
  consumer (RE2, SQL) can apply it.

  It covers central administration plus one tier below it, and nothing
  lower: every excluded label of a mixed-suffix root
  (excluded-labels) becomes an exclude row, and every validated
  central-1 domain directly under a central root (sp.gov.br,
  central1-under) becomes an exact row, so
  lower-tier hosts registered anywhere in their subtree
  (campinas.sp.gov.br) stay out until a central body of the subtree is
  listed explicitly in the central+ scope. Domains already covered by a
  root (e.g. fazenda.gov.br under gov.br) are pruned as redundant."
  [_]
  (if-not (fs/exists? central-plus-file)
    (err "ERR: " central-plus-file " missing. Run 'bb pipeline central' first")
    (let [rows (rest (read-csv-raw central-plus-file))
          excl (excluded-labels)
          c1-under (central1-under)
          apexes (set (for [[[c root] labels] c1-under
                            label labels]
                        [c (str label "." root)]))
          ;; Same-domain rows across countries keep the union of
          ;; exclusions; a domain that is an apex in any country stays
          ;; an apex.
          entries (->> rows
                       (reduce (fn [m [d c]]
                                 (update m d
                                         (fn [[es ap]]
                                           [(into (or es #{})
                                                  (sub-central-labels excl c1-under [c d]))
                                            (or ap (contains? apexes [c d]))])))
                               {})
                       (map (fn [[d [es ap]]] [d es ap]))
                       prune-covered)
          country-of (into {} (map (fn [[d c]] [d c]) (reverse rows)))
          apex-domains (set (for [[d _ apex?] entries :when apex?] d))
          ;; One row per entry, plus one per excluded label under it; an
          ;; exact row already keeps its own subtree out, so central-1
          ;; labels need no exclude twin.
          policy (->> (for [[d excl apex?] entries
                            row (cons [d (if apex? "exact" "subtree")]
                                      (for [l excl
                                            :let [ld (str l "." d)]
                                            :when (not (apex-domains ld))]
                                        [ld "exclude"]))]
                        (conj row (country-of d)))
                      (sort-by (juxt first second)))]
      (write-csv-file "data/public-sector-domains-central+-policy.csv"
                      ["domain" "kind" "country"] policy)
      (println (str "Wrote data/public-sector-domains-central+-policy.csv ("
                    (count policy) " rows)")))))

;; ===========================================================================
;;  Utilitaire -- indegree (link-graph centrality, eu-plus-government-scans)
;; ===========================================================================

(def linkgraph-base-url
  ;; Data published by https://github.com/mgifford/eu-plus-government-scans:
  ;; a crawl of government sites (EU + neighbours) recording outbound links
  ;; and asset dependencies. gov-domains.json maps hostname -> country;
  ;; relationships.jsonl holds one edge per line.
  "https://raw.githubusercontent.com/mgifford/eu-plus-government-scans/main/docs/data/")

(def linkgraph-cache "/tmp/linkgraph")

(defn- linkgraph-fetch!
  "Download one published data file into the /tmp cache (relationships.jsonl
  is ~100 MB, hence curl -o and a long timeout). Returns the path, nil on
  failure. FORCE=1 re-downloads."
  [file]
  (let [path (str linkgraph-cache "/" file)
        tmp  (str path ".tmp")]
    (if (and (fs/exists? path) (not force?))
      path
      (do (ensure-dir linkgraph-cache)
          (println (str "Downloading " file "..."))
          ;; download to a .tmp then rename: an interrupted download must
          ;; not leave a truncated file that the cache would then serve
          (let [{:keys [exit]}
                (try (proc/sh "curl" "-sfL" "--max-time" "600" "-A" ua
                              "-o" tmp (str linkgraph-base-url file))
                     (catch Exception _ nil))]
            (if (and exit (zero? exit))
              (do (fs/move tmp path {:replace-existing true})
                  path)
              (do (err "ERR: could not fetch " linkgraph-base-url file)
                  (when (fs/exists? tmp) (fs/delete tmp))
                  nil)))))))

(def linkgraph-country-aliases
  "Crawler country names whose normalize-name does not equal our slug."
  {"unitedkingdomuk"  "unitedkingdom"
   "republicofcyprus" "cyprus"})

(defn- linkgraph-suffix->dir
  "{domain-suffix country_dir} built from the crawler's inventory. Every
  dotted suffix of each hostname is registered (2+ labels), so the
  domain-level source/target fields of the graph (paris.fr,
  culture.gouv.fr) match; suffixes claimed by two countries are dropped."
  []
  (when-let [path (linkgraph-fetch! "gov-domains.json")]
    (let [by-slug   (into {} (map (juxt country-slug identity)) (country-dirs))
          name->dir (fn [n] (let [n (normalize-name n)]
                              (by-slug (get linkgraph-country-aliases n n))))
          domains   (get (json/parse-string (slurp path)) "domains")
          mapping   (into {} (keep (fn [n] (when-let [d (name->dir n)] [n d])))
                          (distinct (vals domains)))]
      (doseq [n (remove mapping (distinct (vals domains)))]
        (err "WARN: unmapped country in link graph: " n))
      (->> domains
           (reduce (fn [m [host cname]]
                     (if-let [dir (mapping cname)]
                       (let [parts (str/split (str/lower-case host) #"\.")]
                         (reduce (fn [m i]
                                   (update m (str/join "." (subvec parts i))
                                           (fnil conj #{}) dir))
                                 m (range (dec (count parts)))))
                       m))
                   {})
           (keep (fn [[suf dirs]] (when (= 1 (count dirs)) [suf (first dirs)])))
           (into {})))))

(defn- linkgraph-lookup
  "Longest-suffix match of a domain in the {suffix country_dir} map."
  [suffix->dir domain]
  (let [parts (str/split domain #"\.")]
    (some (fn [i] (suffix->dir (str/join "." (subvec parts i))))
          (range (dec (count parts))))))

(defn cmd-indegree
  "For every country covered by the eu-plus-government-scans crawl, compute
  each government domain's in-degree: how many distinct public-sector
  domains of the same country link to it. Editorial links and form
  destinations feed =indegree=; script/stylesheet/media dependencies feed
  =indegree_tech=. Auto-links are excluded, and the crawler's own
  target_category is ignored (it tags well-known government sites as
  external). Writes countries/<c>/sources/linkgraph/indegree.csv; the
  report phase then folds hosts at or above linkgraph-min-indegree into
  proposed.csv with a strong score bonus. Optional args restrict to the
  given country_dirs."
  [args]
  (let [suffix->dir (linkgraph-suffix->dir)
        rel-path    (and suffix->dir (linkgraph-fetch! "relationships.jsonl"))]
    (if-not rel-path
      (err "ERR: link-graph data unavailable")
      (let [only       (when (seq args) (set args))
            editorial? #{"editorial_link" "form_destination"}
            acc        ;; {country_dir {target {:ed #{src...} :tech #{src...}}}}
            (with-open [r (io/reader rel-path)]
              (reduce
               (fn [acc line]
                 (let [row (json/parse-string line)
                       src (get row "source_domain")
                       tgt (get row "target_domain")
                       dir (and (seq src) (seq tgt) (not= src tgt)
                                (linkgraph-lookup suffix->dir src))]
                   (if (and dir
                            (or (nil? only) (contains? only dir))
                            (= dir (linkgraph-lookup suffix->dir tgt)))
                     (let [k (if (editorial? (get row "relationship_type"))
                               :ed :tech)]
                       (update-in acc [dir tgt k] (fnil conj #{}) src))
                     acc)))
               {} (line-seq r)))]
        (doseq [[dir targets] (sort-by key acc)]
          (let [rows   (->> targets
                            (map (fn [[tgt {:keys [ed tech]}]]
                                   [tgt (count ed) (count tech)]))
                            (sort-by (juxt #(- (nth % 1)) first)))
                strong (count (filter #(>= (nth % 1) linkgraph-min-indegree)
                                      rows))]
            (write-csv-file (country-src dir "linkgraph" "indegree.csv")
                            ["hostname" "indegree" "indegree_tech"]
                            (map (fn [[t e c]] [t (str e) (str c)]) rows))
            (println (str dir ": " (count rows) " linked domains ("
                          strong " with indegree >= "
                          linkgraph-min-indegree ")"))))
        (println "Run 'bb pipeline report' to fold them into proposed.csv.")))))

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
   "normalize"   cmd-normalize
   "probe"       cmd-probe
   "mx"          cmd-mx
   "aggregate"   cmd-aggregate
   "central"     cmd-central
   "cisa"        cmd-cisa
   "lannuaire"   cmd-lannuaire
   "govuk"       cmd-govuk
   "wikidata"    cmd-wikidata
   "iana"        cmd-iana
   "cia"         cmd-cia
   "un-desa"     cmd-un-desa
   "oecd"        cmd-oecd
   "meta"        cmd-meta
   "cross-check" cmd-cross-check
   "probe-proposed" cmd-probe-proposed
   "build-qid"   cmd-build-qid
   "validate-un" cmd-validate-un
   "domains"     cmd-domains
   "indegree"    cmd-indegree})

(defn usage []
  (println "Usage: bb scripts/pipeline.clj <command> [args…]")
  (println)
  (println "Main commands:")
  (println "  collect | enrich | report | all")
  (println)
  (println "Targeted commands:")
  (println "  fetch | retry | normalize | probe | mx | aggregate | central")
  (println "  cisa | lannuaire | govuk")
  (println "  wikidata | iana | cia | un-desa | oecd | meta | cross-check | build-qid")
  (println "  validate-un | domains | indegree | probe-proposed")
  (println)
  (println "Directory harvesting moved to scripts/detect-from-directories.clj (bb directories)")
  (println)
  (println "Forge/catalog detection moved to scripts/detect-forges.clj (bb forges)")
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

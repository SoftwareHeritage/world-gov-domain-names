#!/usr/bin/env bb
;; world-gov-domain-names -- full pipeline (Babashka).
;;
;; Four phases, each an aggregated command chaining targeted commands
;; in dependency order; every targeted command writes one kind of file.
;;
;;   collect            the harvest: fetch (+ retry on failure), normalize,
;;                      probe, mx, probe-roots -> sources/harvest/, data/.db
;;   enrich             the per-country sources: build-qid, build-gec,
;;                      build-un-ids, subdivisions, then wikidata (+ academia)
;;                      + ror/iana/cia/un-desa/oecd/meta in parallel
;;                      -> sources/wikidata/, sources/ror/, data/sources/
;;   build              the consolidated files: aggregate, domains -> data/
;;   report             the curation files: propose (+ todo), summary
;;                      -> proposed.csv, curate_todo.csv, summary.md
;;   all                collect + enrich + build + report
;;
;; Targeted commands (collect):
;;   fetch [DOM…]       crt.sh fetch (every harvest root, or those due with
;;                      MAX_AGE) -> outcome and date in data/.db
;;   retry [DOM…]       retry the roots whose last fetch failed (data/.db)
;;   normalize          clean every harvest file (sources/harvest/<root>.csv);
;;                      a host under two harvested roots stays in the deeper file
;;   probe [DOM…]       HTTPS HEAD probe of the harvested hosts not probed yet
;;                      -> data/.db
;;   mx [DOM…]          DNS MX lookup of the harvested hosts -> data/.db
;;                      (email signal)
;;   probe-roots [C…]   HTTPS HEAD + MX of the confirmed roots that have no
;;                      harvest file -> data/.db
;;
;; Targeted commands (enrich):
;;   build-qid          country_dir -> Wikidata QID: data/country_qid.csv
;;   build-gec          country_dir -> Factbook GEC code: data/factbook_gec.csv
;;   build-un-ids       country_dir -> UN/DESA id: data/un_desa_ids.csv
;;   subdivisions [Q:C…] Wikidata first-level subdivisions (P150)
;;                      -> sources/wikidata/subdivisions_level1.csv
;;   wikidata [Q:C…]    Wikidata central administration
;;                      -> sources/wikidata/central_admin.csv (needs subdivisions)
;;   academia [Q:C…]    Wikidata universities and research institutes (level
;;                      central, central-1 or blank) -> sources/wikidata/academia.csv (needs
;;                      subdivisions; enrich runs it after wikidata)
;;   ror [C…]           universities per ror.org -> sources/ror/universities.csv
;;                      (needs subdivisions; candidates, never confirmed)
;;   sopc [C…]          hosts of the code catalogs and GitHub org websites of
;;                      swh-sopc-data-sources (SOPC_DIR) -> sources/sopc/hosts.csv
;;   iana [C…]          IANA ccTLD registry
;;   cia [C…]           Government section from factbook.json
;;   un-desa [C…]       UN/DESA national portal + EGDI
;;   oecd [C…]          OECD membership flag
;;   meta [C…]          country metadata (REST Countries + World Bank GDP)
;;
;; Targeted commands (build):
;;   aggregate          every host -> data/public-sector-domains.csv
;;   domains            central + central-1 scope -> the match policy table
;;                      data/public-sector-domains-central+-policy.csv
;;
;; Targeted commands (report):
;;   propose [C…]       score the candidate hosts -> countries/<c>/proposed.csv,
;;                      then todo
;;   todo [C…]          proposed.csv minus what is confirmed or excluded since,
;;                      in the columns of curated.csv -> curate_todo.csv (offline)
;;   summary [C…]       the per-country report -> countries/<c>/summary.md
;;
;; Other commands:
;;   probe-proposed [C…] HTTPS HEAD + MX of the proposed hosts -> data/.db
;;   wayback DOM…       Wayback Machine CDX harvest of the given roots, merged
;;                      into sources/harvest/<root>.csv (slow: on demand, for the
;;                      roots crt.sh leaves empty)
;;   check [C…]         compile the decision files (C: FRA or FRA_france):
;;                      exit 1 on a contradiction, WARN on a curated row a
;;                      registry already lists
;;   esr                fetch FR public higher-education institutions -> sources/esr/registered.csv
;;   govuk              build UK sub-central list -> GBR sources/govuk/registered.csv
;;   validate-un        check un_status against the official UN member list
;;   indegree [C…]      link-graph in-degree (eu-plus-government-scans)
;;                      -> countries/<c>/sources/linkgraph/indegree.csv
;;
;; Environment variables:
;;   FORCE=1            force-overwrite existing outputs; re-probe every host
;;                      (probe, mx, probe-roots, probe-proposed)
;;   MAX_AGE            days: also re-probe the probes older than that, or
;;                      undated, and fetch only the roots not fetched since
;;   PARALLEL           # concurrent requests (fetch/probe)
;;   TIMEOUT            HTTPS request timeout in seconds (probe), default 5s

(ns pipeline
  (:require [common :refer :all]
            [enrich :as enrich]
            [registries :as registries]
            [indegree :as indegree]
            [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.string :as str]))

(defn- probes-fn
  "The function f of probes.clj, loaded on first use: only the commands
  that probe or fetch load the datalevin pod it needs, not check."
  [f]
  (requiring-resolve (symbol "probes" (name f))))

;; ===========================================================================
;;  Harvest files -- countries/<c>/sources/harvest/<root>.csv
;; ===========================================================================
;;
;; One file per harvested root domain, one column subdomain: every host
;; crt.sh ever saw under the root, the root's apex included. The HTTPS
;; probe and the MX lookup of each host live in data/.db (probes.clj).
;; Harvesting a root is a choice distinct from confirming it: `fetch
;; <root>` creates the file of a confirmed central or central-1 domain (a
;; harvest root, see common/harvest-roots); harvest roots without a file
;; (registry roots, mostly) only get their apex probed. local rows are
;; never harvested.

(def harvest-header ["subdomain"])

(defn harvest-files
  "All harvest files (paths as strings), scoped to one country_dir glob."
  ([] (harvest-files "*"))
  ([country-glob]
   (->> (fs/glob "countries" (str country-glob "/sources/harvest/*.csv"))
        (map str)
        sort)))

(defn harvest-root
  "The root domain a harvest file covers: its basename."
  [file]
  (str/replace (str (fs/file-name file)) #"\.csv$" ""))

(defn harvest-file
  "Path of the harvest file of root in country_dir (existing or not)."
  [country-dir root]
  (country-src country-dir "harvest" (str root ".csv")))

(defn read-harvest
  "Hosts of a harvest file; empty when absent."
  [file]
  (for [[h] (rest (read-csv-raw file))
        :when (not (str/blank? h))]
    h))

(defn normalize-host
  "The host of a harvested name or URL: lowercased, wildcard prefix,
  scheme, path or query, port and trailing dot stripped; nil when what
  remains is no valid hostname. fetch and wayback normalize what they
  add; normalize cleans the files after the fact."
  [s]
  (let [h (some-> s str/trim str/lower-case
                  (str/replace #"^\*\." "")
                  (str/replace #"^https?://" "")
                  (str/replace #"[/?#].*$" "")
                  (str/replace #":\d+$" "")
                  (str/replace #"\.$" ""))]
    (when (valid-hostname? h) h)))

(defn write-harvest!
  "Write hosts to a harvest file: add the apex, dedupe, sort."
  [file hosts]
  (write-csv-file file harvest-header
                  (map vector (sort (set (cons (harvest-root file) hosts))))))

(defn add-hosts!
  "Merge new hosts into a harvest file."
  [file hosts]
  (write-harvest! file (concat (read-harvest file) hosts)))

(defn unharvested-roots
  "Harvest roots of a country without a harvest file."
  [country-dir]
  (remove #(fs/exists? (harvest-file country-dir %)) (harvest-roots country-dir)))

(defn resolve-harvest-files
  "Harvest file paths of the given domains: the existing files, else the
  file a confirmed central or central-1 root would have (ERR and skip
  any other domain). Every existing harvest file when args is empty."
  [args]
  (if (seq args)
    (vec (mapcat (fn [d]
                   (let [d (str/lower-case d)
                         existing (map str (fs/glob "countries" (str "*/sources/harvest/" d ".csv")))
                         countries (for [[c domain level] (confirmed-rows)
                                         :when (and (= domain d) (contains? harvest-levels level))]
                                     c)]
                     (cond
                       (seq existing) existing
                       (seq countries) (map #(harvest-file % d) countries)
                       :else (do (err "ERR: '" d "' is no confirmed central or central-1 domain"
                                      " -- curate it first") []))))
                 args))
    (vec (harvest-files))))

;; ===========================================================================
;;  collect -- fetch / retry (crt.sh)
;; ===========================================================================

(defn fetch-one!
  "Fetch the subdomains of a harvest file's root from crt.sh and merge
  them, normalized, into the file.
  :ok, or :fail with nothing written -- on an unreadable answer, and on
  an empty one while the file holds no host yet."
  [file]
  (let [domain (harvest-root file)
        url (str "https://crt.sh/?q=%25." domain "&output=json")
        body (http-get url {:timeout 120 :retries 1 :accept "application/json"})
        known (read-harvest file)
        names (when body
                (try
                  (->> (json/parse-string body true)
                       (map :name_value)
                       (mapcat #(str/split-lines (str %)))
                       (keep normalize-host)
                       distinct
                       vec)               ; realize inside the try: cheshire
                                          ; parses top-level arrays lazily, so a
                                          ; truncated crt.sh body would otherwise
                                          ; throw JsonEOFException downstream
                  (catch Exception _ nil)))]
    (cond
      (seq names)
      (do (add-hosts! file names)
          (println (str "OK   " domain " (" (count (read-harvest file)) " lignes)"))
          :ok)

      (and names (next known))              ; `[]` (crt.sh timed out?), hosts known
      (do (println (str "OK   " domain " (empty answer, " (count known) " lignes kept)"))
          :ok)

      :else
      (do (println (str "FAIL " domain (cond (nil? body) "" (nil? names) " (unreadable answer)"
                                             :else " (empty answer)")))
          :fail))))

(defn- save-fetches!
  "Record the outcome of [[file :ok|:fail]…] in data/.db: retry replays
  the roots whose last fetch failed."
  [results]
  ((probes-fn 'save-fetches!) (into {} (for [[f outcome] results] [(harvest-root f) outcome])))
  results)

(defn fetch-all!
  "fetch-one! over the harvest files of args -- without args, every one
  whose root is due (probes/due-fetches) -- recording the outcomes:
  [[file :ok|:fail]…]."
  [args]
  (let [files (resolve-harvest-files args)
        due   (set ((probes-fn 'due-fetches) (map harvest-root files)))
        files (if (seq args) files (filter (comp due harvest-root) files))]
    (save-fetches! (bounded-pmap (parallel 4) (fn [f] [f (fetch-one! f)]) files))))

(defn cmd-fetch [args]
  (if (some #(= :fail (second %)) (fetch-all! args)) 1 0))

(defn retry-one!
  "fetch-one! up to three times, with a growing pause: :ok or :fail."
  [file]
  ;; loop on fetch-one! directly: probing the URL first with a separate
  ;; http-get would download the (heavy) crt.sh response twice per success
  (let [domain (harvest-root file)]
    (loop [attempt 1]
      (cond
        (= :ok (fetch-one! file))
        (do (println (str "  [retry=" attempt "] OK " domain)) :ok)

        (< attempt 3)
        (do (Thread/sleep (* attempt 5000))
            (recur (inc attempt)))

        :else
        (do (println (str "FAIL " domain " after " attempt " attempts")) :fail)))))

(defn cmd-retry [args]
  (let [domains (if (seq args) args ((probes-fn 'failed-fetches)))]
    (if (empty? domains)
      (do (err "No failed fetch in data/.db (and no argument provided).") 1)
      (let [files   (resolve-harvest-files (vec domains))
            results (save-fetches! (bounded-pmap (parallel 2) (fn [f] [f (retry-one! f)]) files))]
        (if (some #(= :fail (second %)) results) 1 0)))))

;; ===========================================================================
;;  collect -- normalize
;; ===========================================================================

(defn normalize-harvest-hosts
  "normalize-host the hosts, dropping those it rejects."
  [hosts]
  (keep normalize-host hosts))

(defn split-nested
  "Split the hosts of a harvest root among root and the deeper
  harvested roots of its country: {root hosts…}, each host under the
  longest root it sits on (the apex of a deeper root included). Pure."
  [root deeper hosts]
  (let [deeper (sort-by (comp - count) deeper)]
    (group-by (fn [h]
                (or (some #(when (or (= h %) (str/ends-with? h (str "." %))) %) deeper)
                    root))
              hosts)))

(defn cmd-normalize
  "Clean every harvest file (normalize-harvest-hosts) and move each
  host into the file of its longest harvested root in the country: the
  hacienda.gob.es hosts of gob.es.csv go to hacienda.gob.es.csv."
  [_]
  (doseq [c (country-dirs)
          :let [files (harvest-files c)
                roots (map harvest-root files)]
          file files
          :let [root    (harvest-root file)
                deeper  (filter #(str/ends-with? % (str "." root)) roots)
                rows    (read-harvest file)
                by-root (split-nested root deeper (normalize-harvest-hosts rows))
                moved   (dissoc by-root root)]]
    (doseq [[r rs] moved
            :let [f (harvest-file c r)]]
      (write-harvest! f (concat (read-harvest f) rs)))
    (write-harvest! file (get by-root root []))
    (println (str "[" root "] " (count rows) " -> " (count (read-harvest file))
                  (when (seq moved)
                    (str ", " (reduce + (map count (vals moved))) " moved under "
                         (str/join ", " (sort (keys moved)))))))))

;; ===========================================================================
;;  wayback -- the Wayback Machine's CDX index, on demand
;; ===========================================================================
;;
;; A second harvester for the roots crt.sh leaves empty: every host the
;; Wayback Machine ever captured under the root, merged into the same
;; harvest file. Never part of `collect`: a CDX page takes ~30 s, and
;; gov.uk has 3000 of them. Paged like JustAnotherArchivist's
;; ia-cdx-search: showNumPages, then page=0..N-1 one request at a time,
;; a flat 30 s pause on any failure (the rate limit is a 302 to
;; /429.html, which http-get gives up on like any other failure).

(def wayback-cdx "https://web.archive.org/cdx/search/cdx")

(defn wayback-get
  "GET the CDX index with params: the body, or nil after five attempts
  30 s apart."
  [params]
  (loop [attempt 1]
    (or (http-get wayback-cdx {:timeout 300 :retries 1 :query-params params})
        (when (< attempt 5)
          (Thread/sleep 30000)
          (recur (inc attempt))))))

(defn wayback-hosts
  "The hosts under root of one CDX page (a JSON array of [original-url]
  rows, headed by [\"original\"]), apex included; nil when the page is
  no such array."
  [root page]
  (try
    (->> (rest (json/parse-string page))
         (keep (fn [[url]] (normalize-host url)))
         (filter #(or (= % root) (str/ends-with? % (str "." root))))
         vec)                               ; realize inside the try
    (catch Exception _ nil)))

(defn wayback-one!
  "Harvest a harvest file's root from the CDX index and merge the hosts
  into the file, every ten pages and at the end. :ok, or :fail on the
  first page five attempts could not fetch, or that is no JSON array."
  [file]
  (let [root   (harvest-root file)
        params {"url" root "matchType" "domain" "collapse" "urlkey" "pageSize" "100"}
        pages  (some-> (wayback-get (assoc params "showNumPages" "true")) str/trim parse-long)]
    (if-not pages
      (do (println (str "FAIL " root " (page count)")) :fail)
      (loop [page 0 hosts #{}]
        (let [body       (when (< page pages)
                           (wayback-get (assoc params "output" "json" "fl" "original" "page" (str page))))
              page-hosts (when body (wayback-hosts root body))]
          (if page-hosts
            (let [hosts (into hosts page-hosts)]
              (println (str "[" root "] page " (inc page) "/" pages " (" (count hosts) " hosts)"))
              (when (zero? (mod (inc page) 10)) (add-hosts! file hosts))
              (recur (inc page) hosts))
            (do (add-hosts! file hosts)
                (cond
                  (= page pages)
                  (do (println (str "OK   " root " (" pages " pages, " (count (read-harvest file)) " lignes)"))
                      :ok)

                  body
                  (do (println (str "FAIL " root " (page " (inc page) "/" pages " unreadable)")) :fail)

                  :else
                  (do (println (str "FAIL " root " (page " (inc page) "/" pages ")")) :fail)))))))))

(defn cmd-wayback
  "wayback-one! over the given roots, one after the other (the CDX
  server rate-limits). Roots are required: the index is far too slow to
  sweep every harvest file."
  [args]
  (if (empty? args)
    (do (err "wayback needs one or more harvest roots (a full sweep would take days)") 1)
    (let [files (resolve-harvest-files args)]
      (cond
        (empty? files)                            1
        (some #{:fail} (mapv wayback-one! files)) 1
        :else                                     0))))

;; ===========================================================================
;;  collect -- probe
;; ===========================================================================

(defn probe-one!
  "HTTPS HEAD of sub: [sub status], status being the HTTP code (\"200\")
  or a one-line error such as \"ConnectException >
  UnresolvedAddressException\" (the exception chain and its deepest
  message)."
  [sub timeout]
  (let [resp (try
               (http/head (str "https://" sub "/")
                          {:client http-client
                           :headers {"User-Agent" ua}
                           :throw false
                           :timeout (* timeout 1000)})
               (catch Exception e
                 (let [chain (take-while some? (iterate #(.getCause ^Throwable %) e))
                       cls   (str/join " > " (distinct (map #(.getSimpleName (class %)) chain)))
                       msg   (->> chain (map #(single-line (.getMessage %))) (remove str/blank?) last)]
                   {:err (if msg (str cls ": " msg) cls)})))
        code (some-> resp :status str)
        status (cond
                 (and code (not (#{"0" "000"} code))) code
                 (:err resp)                          (:err resp)
                 :else                                 "unknown error")]
    [sub status]))

(defn- blank-hosts
  "Hosts of a {host [http_status mx]} map whose column idx (0 = HTTPS
  status, 1 = MX) is blank or, with MAX_AGE, stale; every host with
  FORCE=1."
  [probes idx]
  (let [stale? (probes-fn 'stale?)]
    (for [[h v] probes :when (or force? (str/blank? (nth v idx)) (stale? h idx))] h)))

(defn- save-probes!
  "Store column idx of a {host [http_status mx]} map for hosts in data/.db."
  [probes idx hosts]
  ((probes-fn 'save!) idx (into {} (for [h hosts] [h (get-in probes [h idx])]))))

(defn- fill-hosts!
  "Fill column idx of a {host [http_status mx]} map with (lookup host)
  for hosts, conc lookups at a time."
  [probes idx lookup conc hosts]
  (reduce (fn [m [h v]] (assoc-in m [h idx] v))
          probes
          (bounded-pmap conc (fn [h] [h (lookup h)]) hosts)))

(defn- with-mx-count [hosts probes]
  (count (remove #(#{"" "none" "dig error"} (second (get probes %))) hosts)))

(defn probe-domain!
  "Probe the hosts of a harvest file without an HTTPS status and store
  the results in data/.db."
  [file timeout]
  (when (fs/exists? file)
    (let [root-name (harvest-root file)
          probes    ((probes-fn 'probes-of) (read-harvest file))
          todo      (blank-hosts probes 0)]
      (if (empty? todo)
        (println (str "[" root-name "] no unprobed host"))
        (do (println (str "[" root-name "] " (count todo) " subdomains to probe"))
            (save-probes! (fill-hosts! probes 0 #(second (probe-one! % timeout)) (parallel 50) todo)
                          0 todo))))))

(defn cmd-probe
  "probe-domain! over the harvest files of args (every one when empty)."
  [args]
  (let [timeout (env-int "TIMEOUT" 5)]
    (doseq [f (resolve-harvest-files args)] (probe-domain! f timeout))))

;; ===========================================================================
;;  collect -- mx (email signal, never a filter)
;; ===========================================================================

(defn mx-lookup
  "MX records of a host via dig, as one line (\"prio host; …\"), \"none\"
  without MX, \"dig error\" on failure."
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
  "Look up the MX of the hosts of a harvest file without one and store
  them in data/.db."
  [file conc]
  (let [root-name (harvest-root file)
        probes    ((probes-fn 'probes-of) (read-harvest file))
        todo      (blank-hosts probes 1)]
    (if (empty? todo)
      (println (str "[" root-name "] mx: nothing to look up"))
      (let [filled (fill-hosts! probes 1 mx-lookup conc todo)]
        (save-probes! filled 1 todo)
        (println (str "[" root-name "] mx: " (count todo) " looked up ("
                      (with-mx-count todo filled) " with MX)"))))))

(defn cmd-mx
  "mx-domain! over the harvest files of args (every one when empty)."
  [args]
  (let [conc (parallel 50)]
    (doseq [f (resolve-harvest-files args)] (mx-domain! f conc))))

;; ===========================================================================
;;  collect -- probe-roots (the confirmed roots without a harvest file)
;; ===========================================================================
;; Same shape as probe-proposed: country-scoped, both probes at once.

(defn probe-roots!
  "Probe (HTTPS HEAD, MX) the unharvested roots of a country lacking
  either probe and store the results in data/.db."
  [country-dir timeout conc]
  (let [probes    ((probes-fn 'probes-of) (unharvested-roots country-dir))
        todo-http (blank-hosts probes 0)
        todo-mx   (blank-hosts probes 1)]
    (when (or (seq todo-http) (seq todo-mx))
      (let [filled (-> probes
                       (fill-hosts! 0 #(second (probe-one! % timeout)) conc todo-http)
                       (fill-hosts! 1 mx-lookup conc todo-mx))]
        (save-probes! filled 0 todo-http)
        (save-probes! filled 1 todo-mx)
        (println (str "[" country-dir "] roots: " (count todo-http) " probed, "
                      (count todo-mx) " MX looked up ("
                      (with-mx-count todo-mx filled) " with MX)"))))))

(defn cmd-probe-roots [args]
  (let [timeout (env-int "TIMEOUT" 5)
        conc    (parallel 50)]
    (iter-countries #(probe-roots! % timeout conc) args)))

;; ===========================================================================
;;  build -- aggregate
;; ===========================================================================
;; Reads the harvest (collect) and the country metadata of
;; data/sources/country_data.csv (enrich, meta): run after both. The
;; probes stay in data/.db.

(defn country-hosts
  "[host parent_domain] rows of a country, sorted: the hosts of its
  harvest files, each once with its longest parent, plus the
  unharvested roots."
  [country-dir]
  (let [harvested (->> (for [file (harvest-files country-dir)
                             :let [parent (harvest-root file)]
                             sub (read-harvest file)]
                         [sub parent])
                       (sort-by #(- (count (second %))))
                       (reduce (fn [m [sub parent]] (update m sub #(or % parent))) {}))
        roots     (for [d (unharvested-roots country-dir)
                        :when (not (harvested d))]
                    [d d])]
    (sort-by first (concat harvested roots))))

(defn- country-meta-map
  "Map country_dir -> {:region :langs :gdp} from data/sources/country_data.csv."
  []
  (into {}
        (for [c (country-dirs)]
          [c {:region (table-field "country_data" c "region")
              :langs  (table-field "country_data" c "languages")
              :gdp    (table-field "country_data" c "gdp_per_capita")}])))

(def public-sector-file "data/public-sector-domains.csv")
(def policy-file        "data/public-sector-domains-central+-policy.csv")

(defn cmd-aggregate [_]
  (let [un-by-country (build-un-status-map)
        meta-by-country (country-meta-map)
        ;; data/public-sector-domains.csv -- every harvested host (root apex AND
        ;; subdomains) plus the apex of every harvest root, regardless of
        ;; HTTP/MX. Inclusion criterion: the host existed in DNS at least once
        ;; (it appeared in a source like crt.sh) or is a confirmed root.
        rows (->> (country-dirs)
                  (mapcat (fn [country]
                            (let [un (get un-by-country country "member")
                                  m  (get meta-by-country country)]
                              ;; UN-facing output: keep UN members and observers
                              ;; only, never non-UN entities (Taiwan, Kosovo).
                              (when (not= un "non_un")
                                (for [[sub parent] (country-hosts country)]
                                  [sub parent country un
                                   (:region m) (:langs m) (:gdp m)])))))
                  (sort-by first))]
    (write-csv-file public-sector-file
                    ["subdomain" "parent_domain" "country" "un_status"
                     "region" "languages" "gdp_per_capita"]
                    rows)
    (let [counts (frequencies (map #(nth % 3) rows))]
      (println (str "Wrote " public-sector-file " (" (count rows) " hosts)"))
      (println (str "  UN members: " (get counts "member" 0)
                    " ; observers: " (get counts "observer" 0))))))

(defn- central-roots
  "#{[country root]} of the confirmed central domains."
  []
  (set (for [[c d level] (confirmed-rows) :when (= level "central")] [c d])))

(defn- labels-under-central
  "{[country root] #{label…}} of the [country hostname] pairs sitting
  directly under a confirmed central root of their country
  (sp.gov.br -> {[BRA gov.br] #{\"sp\"}})."
  [pairs]
  (let [central (central-roots)]
    (reduce (fn [m [c d]]
              (let [[_ label root] (re-matches #"([a-z0-9-]+)\.(.+)" d)]
                (if (and root (contains? central [c root]))
                  (update m [c root] (fnil conj #{}) label)
                  m)))
            {} pairs)))

(def excluded-domains
  "{country_dir #{domain…}} of every countries/<c>/excluded.csv."
  (delay (into {} (for [c (country-dirs)] [c (set (map first (read-excluded c)))]))))

(defn local-labels
  "{[country root] #{label…}} of the confirmed local domains and the
  excluded hostnames sitting directly under a confirmed central root:
  the exclude rows of the policy table."
  []
  (labels-under-central (concat (for [[c d level] (confirmed-rows) :when (= level "local")] [c d])
                                (for [[c ds] @excluded-domains, d ds] [c d]))))

;; ===========================================================================
;;  report -- propose (score the candidate hosts)
;; ===========================================================================
;; Reads the enrichment sources (wikidata, un-desa, iana, cia), the
;; link graph (indegree), the official directories (bb directories) and
;; the probes of probe-proposed.

(def gov-pattern
  #"(?i)(?:^|\.)(?:gov|bund|govt|gouv|governo|gobierno|kormany|hallinto|riksdag|presidencia|presidence|parlement|parlamento|parliament|admin)(?:\.|$)")

(def subdiv-pattern
  #"(?i)(?:departmental|départemental|departementale|départementale|regional|régional|state ministry|prefecture|préfecture|conseil général|general council|county council|provincial|county of|municipal|metropolitan|community of|communauté|comunidad autónoma|comunità|länder|bundesland|senate department|staatskanzlei|landtag|free state of|land of |bavaria|bavarian|bayer(?:ian|n)|saxony|saxon|sächs|hessian|hesse|hessisch|niedersä|niedersaechs|lower saxony|nordrhein|north rhine|westfalen|westphalia|baden-würt|baden-wuert|saarland|saarl|brandenburg|bremen ministry|free hanseatic|schleswig-holstein|mecklenburg|vorpommern|thüring|thuering|thuringia|rheinland-pfalz|rhineland-palatinate|hamburg ministry|hamburg(?:ische|er) (?:ministerium|behörde)|berlin senate|berlin(?:er) senat|state legislature|state senate|state assembly|state house of representatives|city council|city of|town of|village of|borough|township|county executive|school district|office of education|comune di|città metropolitana|citta metropolitana|provincia di|provincia autonoma|unione dei comuni|regione\b|azienda sanitaria|azienda usl|azienda ospedalier)")

(def federal-pattern
  #"(?i)(?:federal|national|sovereign|state of [a-z]+ federation)")

(def secondary-tlds
  {"GBR_united_kingdom" #{"scot" "wales" "im" "je" "gg" "gi" "io"}
   "DNK_denmark"        #{"fo" "gl"}
   "NLD_netherlands"    #{"aw" "cw" "sx"}
   ;; New Caledonia, French Polynesia, and Corsica's geographic TLD
   "FRA_france"         #{"nc" "pf" "corsica"}})

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
  "{country_dir [host…]} of data/public-sector-domains.csv; nil when absent."
  []
  (when (fs/exists? public-sector-file)
    (->> (rest (read-csv-raw public-sector-file))
         (group-by #(nth % 2))
         (reduce-kv (fn [m k v] (assoc m k (mapv first v))) {}))))

(defn collected-at-or-under?
  "True when a collected host is host or sits under it."
  [host collected]
  (some (fn [s] (or (= s host) (str/ends-with? s (str "." host)))) collected))

(defn score-candidate
  "Score a candidate host from 0 to 10 (the points are in
  scripts/README.org). Keys: :host, :wd-count, :label, :fb-phrases,
  :un-portal-host, :cctld-primary (no dot), :indegree (nil when
  unknown), :directory-listed?, :ror-listed?, :sopc-listed?,
  :subdiv-penalty? (default true; false for a host known as
  subnational)."
  [{:keys [host wd-count label fb-phrases un-portal-host cctld-primary
           indegree directory-listed? ror-listed? sopc-listed? subdiv-penalty?]
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
        ;; indegree/linkgraph-min-indegree floor.
        lg-points     (cond (nil? indegree)                       0
                            (>= indegree 20)                      6
                            (>= indegree 10)                      5
                            (>= indegree 5)                       4
                            (>= indegree indegree/linkgraph-min-indegree)  3
                            :else                                 0)
        score (+ (if un?          5 0)
                 (* 3 (min 2 (max 0 wd-count)))
                 lg-points
                 ;; an official-directory listing is authoritative on the
                 ;; body's existence but silent on its level
                 (if directory-listed? 4 0)
                 ;; ROR names a university but says nothing of its sector
                 (if ror-listed?  2 0)
                 ;; the body publishes code: public, level unknown
                 (if sopc-listed? 3 0)
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
  "Level of a candidate host: its Wikidata levels first, then the
  level ror gave it, then its label (subdiv-pattern means local, a
  first-level subdivision name means central-1). Blank when unknown."
  [{:keys [wd-by-host ror-by-host level1-pattern]} h label]
  (let [wd-levels (disj (get-in wd-by-host [h :levels] #{}) "")
        ror-level (not-empty (get-in ror-by-host [h :level]))]
    (cond
      (contains? wd-levels "central")   "central"
      (contains? wd-levels "central-1") "central-1"
      (contains? wd-levels "local")     (if (level1-label? level1-pattern label)
                                          "central-1" "local")
      ror-level                         ror-level
      (and (seq label)
           (re-find subdiv-pattern label)) (if (level1-label? level1-pattern
                                                              label)
                                             "central-1" "local")
      :else "")))

(defn- candidate-row
  "One [domain score sources name level] row of host from a
  load-candidate-ctx context. Pure."
  [{:keys [wd-by-host ac-by-host lg-by-host dir-by-host ror-by-host sopc-by-host
           un-portal-host fb-phrases cctld-primary]
    :as ctx}
   h]
  (let [{:keys [cnt labels] :or {cnt 0 labels []}} (get wd-by-host h)
        ac-cnt (get-in ac-by-host [h :cnt] 0)
        un? (= h un-portal-host)
        linked-n (get lg-by-host h)
        dir-info (get dir-by-host h)
        ror-info (get ror-by-host h)
        sopc-info (get sopc-by-host h)
        label (cond-> (str/join " | " labels)
                un? (str (when (seq labels) " | ")
                         "UN/DESA national portal"))
        ;; hints for the manual validation pass when the directory or the
        ;; link graph is all we know
        label (cond
                (not (str/blank? label)) label
                (seq (:evidence dir-info)) (:evidence dir-info)
                (seq (:label ror-info)) (:label ror-info)
                (seq (:evidence sopc-info)) (:evidence sopc-info)
                linked-n (str "Linked from " linked-n
                              " public-sector domains")
                :else label)
        ;; each source once: the Wikidata mention count is in the score
        sources (cond-> []
                  un?                (conj "un_desa")
                  (> cnt ac-cnt)     (conj "wikidata")
                  (pos? ac-cnt)      (conj "academia")
                  linked-n           (conj "linkgraph")
                  dir-info           (conj "directory")
                  ror-info           (conj "ror")
                  sopc-info          (conj "sopc"))
        level (candidate-level ctx h label)
        score (score-candidate
                {:host h :wd-count cnt :label label
                 :fb-phrases fb-phrases
                 :un-portal-host un-portal-host
                 :cctld-primary cctld-primary
                 :indegree linked-n
                 :directory-listed? (some? dir-info)
                 :ror-listed? (some? ror-info)
                 :sopc-listed? (some? sopc-info)
                 ;; the anti-subnational penalties catch entities
                 ;; *pretending* central
                 :subdiv-penalty?
                 (not (contains? #{"local" "central-1"} level))})]
    [h score (str/join ";" sources) label level]))

(defn wikidata-by-host
  "{host {:cnt n :labels [..] :levels #{..}}} of the rows of a
  wikidata/central_admin.csv (type,label,website,hostname,level)."
  [rows]
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
          {} rows))

(defn load-candidate-ctx
  "What propose-candidates needs for a country: its Wikidata mentions
  and levels (academia apart too), UN/DESA portal, ccTLD, Factbook
  courts, link-graph in-degrees, directory, ror and sopc hosts,
  subdivision labels, the confirmed domains of every country and its
  excluded ones."
  [country-dir]
  (let [src #(country-src country-dir %1 %2)
        rows-of   #(rest (read-csv-raw (src "wikidata" %)))
        ac-rows   (rows-of "academia.csv")
        sub-path  (src "wikidata" "subdivisions_level1.csv")]
    {:wd-by-host     (wikidata-by-host (concat (rows-of "central_admin.csv") ac-rows))
     :ac-by-host     (wikidata-by-host ac-rows)
     :lg-by-host     (into {} (for [{:strs [hostname indegree]} (read-csv-file (src "linkgraph" "indegree.csv"))
                                    :let [n (parse-long (or indegree ""))]
                                    :when (and n (>= n indegree/linkgraph-min-indegree))]
                                [hostname n]))
     ;; both files may list a host twice (two labels): the first row
     ;; wins, and universities.csv sorts the best level first
     :dir-by-host    (index-first (for [{:strs [hostname evidence]} (read-csv-file (src "directory" "orgs.csv"))]
                                    [hostname {:evidence (or evidence "")}]))
     :ror-by-host    (index-first (for [{:strs [hostname label level]} (read-csv-file (src "ror" "universities.csv"))]
                                    [hostname {:label (or label "") :level (or level "")}]))
     :sopc-by-host   (index-first (for [{:strs [hostname evidence]} (read-csv-file (src "sopc" "hosts.csv"))]
                                    [hostname {:evidence (or evidence "")}]))
     :un-portal-host (extract-host (table-field "un_desa" country-dir "national_portal"))
     :fb-phrases     (extract-factbook-phrases (table-field "cia_factbook" country-dir "judicial_highest_courts"))
     :cctld-primary  (not-empty (str/replace (table-field "iana" country-dir "cctld") #"^\." ""))
     :level1-pattern (when (fs/exists? sub-path)
                       (level1-pattern (map second (rest (read-csv-raw sub-path)))))
     :known          (confirmed-domains)
     :excluded       (get @excluded-domains country-dir #{})}))

(def parent-min-hosts
  "Candidates a registrable domain must hold for propose to suggest it."
  3)

(defn parent-rows
  "A [domain score \"parent\" name level] row per registrable domain
  holding parent-min-hosts candidates or more, unless a candidate
  itself or covered by a confirmed or excluded domain: confirming it
  covers them at once. Score, the best of its hosts; level, their most
  frequent one. Pure."
  [rows known excluded]
  (let [candidates (set (map first rows))]
    (for [[parent hosts] (group-by #(parent-domain (first %)) rows)
          :when (and parent
                     (>= (count hosts) parent-min-hosts)
                     (not (candidates parent))
                     (not (host-covered? parent known))
                     (not (host-covered? parent excluded)))
          :let [levels (remove str/blank? (map #(nth % 4) hosts))]]
      [parent
       (apply max (map second hosts))
       "parent"
       (str (count hosts) " proposed hosts under it: "
            (str/join ", " (take 3 (map first (sort-by (comp - second) hosts)))))
       (if (seq levels) (key (apply max-key val (frequencies levels))) "")])))

(defn propose-candidates
  "Rank the hosts of a context: every valid hostname a source mentions
  that no confirmed or excluded domain covers, scored and levelled by
  candidate-row, local ones dropped, plus the parent-rows over them.
  [domain score sources name level] rows, best scores first. Pure."
  [{:keys [wd-by-host lg-by-host dir-by-host ror-by-host sopc-by-host un-portal-host known excluded] :as ctx}]
  (let [rows (->> (cond-> (-> (set (keys wd-by-host))
                              (into (keys lg-by-host))
                              (into (keys dir-by-host))
                              (into (keys ror-by-host))
                              (into (keys sopc-by-host)))
                    un-portal-host (conj un-portal-host))
                  (filter valid-hostname?)
                  (remove #(host-covered? % known))
                  (remove #(host-covered? % excluded))
                  (map #(candidate-row ctx %))
                  (remove #(= "local" (nth % 4))))]
    (sort-by (juxt #(- (nth % 1)) first)
             (concat rows (parent-rows rows known excluded)))))

(defn- todo-name
  "The name a curate_todo.csv row offers: the first of the labels a
  proposed name joins with \" | \", bare Wikidata QIDs aside."
  [name]
  (or (first (remove #(or (str/blank? %) (re-matches #"Q\d+" %))
                     (map str/trim (str/split (str name) #" \| "))))
      ""))

(defn todo-country!
  "Write countries/<c>/curate_todo.csv (domain,level,name,kind): the
  rows of proposed.csv no confirmed or excluded domain covers yet, in
  the columns of curated.csv, ready to move there. Offline: refreshes
  the list after a curation, proposed.csv untouched."
  [country-dir]
  (let [out     (str "countries/" country-dir "/curate_todo.csv")
        decided (into (set (map first (confirmed-country country-dir)))
                      (map first (read-excluded country-dir)))
        rows    (for [[d level name kind] (rest (read-csv-raw (str "countries/" country-dir "/proposed.csv")))
                      :when (and (valid-hostname? d) (not (host-covered? d decided)))]
                  [d (or level "") (todo-name name) (or kind "")])]
    (write-csv-file out ["domain" "level" "name" "kind"] rows)
    (println (str "=== " country-dir " -> " out " (" (count rows) " to curate)"))))

(defn cmd-todo [args] (iter-countries todo-country! args))

(defn propose-country!
  "Write countries/<c>/proposed.csv
  (domain,level,name,kind,score,sources): the ranked candidates, the
  first four columns as in curated.csv, kind blank (subtree) for the
  curation to set; then curate_todo.csv (todo-country!)."
  [country-dir]
  (let [out  (str "countries/" country-dir "/proposed.csv")
        rows (for [[h sc src lbl lvl] (propose-candidates (load-candidate-ctx country-dir))]
               [h lvl lbl "" (str sc) src])]
    (write-csv-file out ["domain" "level" "name" "kind" "score" "sources"] rows)
    (println (str "=== " country-dir " -> " out " (" (count rows) " proposed)"))
    (todo-country! country-dir)))

(defn cmd-propose [args] (iter-countries propose-country! args))

(defn probe-proposed!
  "Probe (HTTPS HEAD, MX) the hosts of proposed.csv lacking either
  probe and store the results in data/.db."
  [country-dir timeout conc]
  (let [hosts  (for [[h] (rest (read-csv-raw (str "countries/" country-dir "/proposed.csv")))
                     :when (valid-hostname? h)]
                 h)
        probes ((probes-fn 'probes-of) hosts)
        todo-http (blank-hosts probes 0)
        todo-mx   (blank-hosts probes 1)]
    (if (and (empty? todo-http) (empty? todo-mx))
      (println (str "[" country-dir "] proposed: nothing to probe"))
      (let [filled (-> probes
                       (fill-hosts! 0 #(second (probe-one! % timeout)) conc todo-http)
                       (fill-hosts! 1 mx-lookup conc todo-mx))]
        (save-probes! filled 0 todo-http)
        (save-probes! filled 1 todo-mx)
        (println (str "[" country-dir "] proposed: " (count todo-http) " probed, "
                      (count todo-mx) " MX looked up ("
                      (count (filter #(re-matches #"[23]\d\d" (first (get filled %))) todo-http))
                      " reachable, " (with-mx-count todo-mx filled) " with MX)"))))))

(defn cmd-probe-proposed [args]
  (let [timeout (env-int "TIMEOUT" 5)
        conc    (parallel 50)]
    (iter-countries #(probe-proposed! % timeout conc) args)))

;; ===========================================================================
;;  report -- summary (the per-country report)
;; ===========================================================================
;; Reads proposed.csv (propose), data/public-sector-domains.csv
;; (aggregate) and the country metadata of data/sources/ (enrich).

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
  (when region
    (println (str "- Region: " region (when subregion (str " / " subregion)))))
  (when languages      (println (str "- Languages: " languages)))
  (when population     (println (str "- Population: " population)))
  (when gdp-per-capita
    (println (str "- GDP per capita: " gdp-per-capita " US$"
                  (when gdp-year (str " (" gdp-year ")")))))
  (when currencies     (println (str "- Currencies: " currencies)))
  (when cctld     (println (str "- ccTLD: `" cctld "` (manager: " (or manager "?") ")")))
  (when (= oecd-status "yes") (println (str "- OECD: member since " oecd-since)))
  (when (= oecd-status "no")  (println "- OECD: non-member"))
  (when un-rank   (println (str "- UN/DESA EGDI: " un-rank)))
  (when fb-govtype (println (str "- Government type: " fb-govtype)))
  (when fb-capital (println (str "- Capital: " fb-capital)))
  (println (str "- Hosts collected: " n-collected))
  (println))

(defn- section-un-portal [country-dir un-portal collected]
  (when un-portal
    (println "## UN/DESA national portal")
    (println)
    (let [host (extract-host un-portal)]
      (println (str "- Declared: [" un-portal "](" un-portal ") (host `" host "`)"))
      (if (collected-at-or-under? host collected)
        (println "- ✅ Covered by collected domains")
        ;; the nearest harvested root above host (gov.bd for
        ;; bangladesh.gov.bd), else its registrable domain for the message
        (let [harvested (some #(when (fs/exists? (harvest-file country-dir %)) %)
                              (butlast (host-suffixes host)))
              parent    (or harvested (parent-domain host))]
          (if harvested
            (println (str "- ⚠️ Exact hostname not collected, but `" parent "` is harvested (to be probed)"))
            (println (str "- ⚠️ ABSENT -- neither `" host "` covered nor `countries/" country-dir "/sources/harvest/" (or parent host) ".csv` present"))))))
    (println)))

(defn- section-registries
  "The authoritative registries of the country: their domains are
  confirmed without curation."
  [country-dir]
  (when-let [rs (seq (registries country-dir))]
    (println "## Authoritative registries")
    (println)
    (println "Their domains are confirmed as they are, without curation:")
    (println)
    (doseq [r rs]
      (println (str "- `" r "`: `" (registered-file country-dir r) "`")))
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

(defn summary-country!
  "Write countries/<c>/summary.md: metadata, UN/DESA portal coverage,
  Factbook institutions, ccTLD anomalies. The candidates stay in
  proposed.csv, which would only be repeated here."
  [country-dir collected-by-country un-status-by-country]
  (let [out        (str "countries/" country-dir "/summary.md")
        field      (fn [source col] (let [v (table-field source country-dir col)]
                                      (when-not (str/blank? v) v)))
        ctx {:un-st          (get un-status-by-country country-dir)
             :cctld          (field "iana" "cctld")
             :manager        (field "iana" "manager")
             :region         (field "country_data" "region")
             :subregion      (field "country_data" "subregion")
             :languages      (field "country_data" "languages")
             :population     (field "country_data" "population")
             :gdp-per-capita (field "country_data" "gdp_per_capita")
             :gdp-year       (field "country_data" "gdp_year")
             :currencies     (field "country_data" "currencies")
             :oecd-status    (field "oecd" "oecd_member")
             :oecd-since     (field "oecd" "member_since")
             :un-rank        (field "un_desa" "egdi_rank")
             :fb-govtype     (field "cia_factbook" "government_type")
             :fb-capital     (field "cia_factbook" "capital")
             :fb-courts      (field "cia_factbook" "judicial_highest_courts")
             :fb-chief       (field "cia_factbook" "chief_of_state")
             :fb-head        (field "cia_factbook" "head_of_government")}
        collected   (get collected-by-country country-dir [])
        un-portal   (field "un_desa" "national_portal")
        cctld       (:cctld ctx)]
    (spit out
          (with-out-str
            (println (str "# " country-dir " -- summary"))
            (println)
            (section-overview (assoc ctx :n-collected (count collected)))
            (section-registries country-dir)
            (section-un-portal country-dir un-portal collected)
            (section-factbook ctx)
            (section-cctld-anomalies country-dir cctld collected)))
    (println (str "=== " country-dir " -> " out))))

(defn cmd-summary [args]
  (let [cache (or (read-collected-cache) {})
        un-status (build-un-status-map)]
    (iter-countries #(summary-country! % cache un-status) args)))

;; ===========================================================================
;;  build -- domains (match policy table over the central+ scope)
;; ===========================================================================
;; Reads the decision files only (confirmed-rows).

(defn- covered-by?
  "True when entry [d2 excl2 apex?] already covers d: d sits under a
  non-apex d2 through a label d2 does not exclude."
  [d [d2 excl2 apex?]]
  (and (not apex?)
       (not= d d2)
       (str/ends-with? d (str "." d2))
       (let [prefix (subs d 0 (- (count d) (inc (count d2))))
             label  (last (str/split prefix #"\."))]
         (not (contains? (set excl2) label)))))

(defn- prune-covered
  "Drop [domain excl apex?] entries already covered by another entry,
  apexes aside: an exact row under a subtree takes its own subtree out."
  [entries]
  (filter (fn [[d _ apex?]] (or apex? (not-any? #(covered-by? d %) entries))) entries))

(defn- central-plus-rows
  "[domain country level] of every confirmed central and central-1
  domain of a UN member or observer, sorted."
  []
  (let [un-by-country (build-un-status-map)]
    (->> (for [[c d level] (confirmed-rows)
               :when (and (contains? harvest-levels level)
                          (not= (get un-by-country c "member") "non_un"))]
           [d c level])
         (sort-by (juxt first #(nth % 2))))))

(defn cmd-domains
  "Write data/public-sector-domains-central+-policy.csv
  (domain,kind,country) from the confirmed central and central-1
  domains: subtree (the domain and below) unless curated.csv says
  exact (the domain alone), exclude for a local or excluded label
  directly under a central root; the longest match decides, a domain a
  subtree covers is dropped. A domain of several countries goes to the
  one confirming it highest (central first, then ASCII order)."
  [_]
  (let [rows (central-plus-rows)
        excl (local-labels)
        apexes (set (for [[c d _ _ _ kind] (confirmed-rows) :when (= kind "exact")] [c d]))
        ;; Same-domain rows across countries keep the union of
        ;; exclusions; a domain that is an apex in any country stays
        ;; an apex.
        entries (->> rows
                     (reduce (fn [m [d c]]
                               (update m d
                                       (fn [[es ap]]
                                         ;; labels to keep out of the root's
                                         ;; subtree: local and excluded ones
                                         ;; (exclude rows)
                                         [(into (or es #{}) (get excl [c d]))
                                          (or ap (contains? apexes [c d]))])))
                             {})
                     (map (fn [[d [es ap]]] [d es ap]))
                     prune-covered)
        country-of (into {} (map (fn [[d c]] [d c]) (reverse rows)))
        apex-domains (set (for [[d _ apex?] entries :when apex?] d))
        ;; One row per entry, plus one per excluded label under it; an
        ;; exact row already keeps its own subtree out, so it needs no
        ;; exclude twin.
        policy (->> (for [[d excl apex?] entries
                          :let [c (country-of d)]
                          [dom kind] (cons [d (if apex? "exact" "subtree")]
                                           (for [l excl
                                                 :let [ld (str l "." d)]
                                                 :when (not (apex-domains ld))]
                                             [ld "exclude"]))]
                      [dom kind c])
                    (sort-by (juxt first second)))]
    (write-csv-file policy-file ["domain" "kind" "country"] policy)
    (println (str "Wrote " policy-file " (" (count policy) " rows: "
                  (count (filter #(= "central" (nth % 2)) rows)) " central + "
                  (count (filter #(= "central-1" (nth % 2)) rows)) " central-1 domains in scope)"))))

;; ===========================================================================
;;  check -- the decision files compile
;; ===========================================================================

(defn- hostname-hint
  "What to write instead of a first cell that is no hostname, as a
  clause for the check message (a pasted URL gives its host, www.
  aside); empty when there is no better guess."
  [cell]
  (if (re-find #"[;\t]" cell)
    ": separate the fields with commas"
    (if-let [host (some-> (normalize-host cell) (str/replace #"^www\." ""))]
      (str ": write " host)
      "")))

(def ^:private levels-hint "write central, central-1 or local")
(def ^:private kinds-hint "leave it empty (subtree) or write exact")

(defn cmd-check
  "Compile the decision files of the given countries (all by default),
  reporting every problem at once with its fix. ERR and exit 1: a
  first cell that is no hostname, a blank or unknown level, an unknown
  kind or one on a local domain, two levels for a domain, a domain
  both curated and excluded. WARN: a duplicate row, an excluded row
  without a reason, a curated row a registry already lists or a
  confirmed domain of the same level covers (unless harvested on its
  own), a local or excluded domain more than one label under a central
  root."
  [args]
  (if-let [countries (scoped-countries args)]
    (let [check-country
          (fn [c]
            (let [errors   (atom 0)
                  err!     (fn [& msg] (swap! errors inc) (apply err "ERR: " c ": " msg))
                  warn     (fn [& msg] (apply err "WARN: " c ": " msg))
                  curated  (read-curated c)
                  excluded (read-excluded c)]
              (doseq [[file path] [["curated.csv" (curated-file c)] ["excluded.csv" (excluded-file c)]]
                      [kind x] (file-anomalies path)]
                (case kind
                  :invalid-hostname (err! "'" x "' in " file " is not a hostname" (hostname-hint x)
                                          " (row dropped)")
                  :duplicate        (warn x " is listed twice in " file " (first row wins)")))
              (doseq [[d level] curated :when (not (levels level))]
                (if (str/blank? level)
                  (err! d " has no level in curated.csv: " levels-hint)
                  (err! d " has an unknown level '" level "' in curated.csv: " levels-hint)))
              (doseq [[d level _ kind] curated]
                (cond
                  (not (kinds kind))
                  (err! d " has an unknown kind '" kind "' in curated.csv: " kinds-hint)
                  (and (seq kind) (not (harvest-levels level)))
                  (err! d " is " level ": only a central or central-1 domain takes a kind")))
              (doseq [[d rows] (group-by first curated)
                      :let [levels (distinct (map second rows))]
                      :when (next levels)]
                (err! d " is curated with two levels (" (str/join ", " levels) "): keep one row"))
              (doseq [d (sort (filter (set (map first excluded)) (set (map first curated))))]
                (err! d " is in both curated.csv and excluded.csv: keep it in one"))
              (doseq [[d reason] excluded :when (str/blank? reason)]
                (warn d " has no reason in excluded.csv"))
              ;; the compilation stops at its first problem: run it once
              ;; the rows above are clean
              (when (zero? @errors)
                (try
                  (let [confirmed  (confirmed-country c)
                        level-of   (into {} (for [[d level] confirmed] [d level]))
                        source-of  (into {} (for [[d _ source] confirmed] [d source]))
                        ;; first registry wins, as in compile-confirmed
                        registered (index-first (for [r (registries c), [d level] (read-registered c r)]
                                                  [d [r level]]))]
                    (doseq [[d level name] curated
                            :let [[r rlevel] (get registered d)]
                            :when (and (= level rlevel) (str/blank? name))]
                      (warn d " (" level ") is curated but " r " already lists it"))
                    ;; a confirmed domain covers its subdomains: a curated
                    ;; row under a root of the same level adds nothing,
                    ;; unless it is harvested on its own
                    (doseq [[d level _ kind] curated
                            :let [root (some #(when (level-of %) %) (rest (host-suffixes d)))]
                            :when (and root (= level (level-of root)) (not= kind "exact")
                                       (not (fs/exists? (harvest-file c d))))]
                      (warn d " (" level ") is already covered by " root
                            " (" level ", " (source-of root) ")"))
                    ;; the policy table only carves the labels directly under
                    ;; a central root out of its subtree (labels-under-central);
                    ;; under a central-1 root (an exact row) everything is out already
                    (doseq [d (concat (for [[d level] confirmed :when (= level "local")] d)
                                      (map first excluded))
                            :let [root (some #(when (contains? harvest-levels (level-of %)) %)
                                             (rest (host-suffixes d)))]
                            :when (and root (= "central" (level-of root))
                                       (str/includes? (subs d 0 (- (count d) (count root) 1)) "."))]
                      (warn d " sits more than one label under " root
                            " -- no exclude row in the policy table")))
                  (catch clojure.lang.ExceptionInfo e
                    (err! (ex-message e)))))
              (zero? @errors)))
          failed (count (remove true? (doall (map check-country countries))))]
      (if (pos? failed)
        1
        (do (println (str "Decision files of " (count countries)
                          (if (= 1 (count countries)) " country" " countries") " compile"))
            0)))
    1))

;; ===========================================================================
;;  Dispatcher
;; ===========================================================================

(defn- exit-code
  "1 when one of the steps' results is 1, 0 otherwise."
  [& results]
  (if (some #{1} results) 1 0))

;; The four phases. Each chains its targeted commands in dependency
;; order and writes nothing of its own; `all` chains the phases the same
;; way (build reads enrich's country metadata, report reads build's
;; consolidated file).

(defn- run-collect
  "The harvest: fetch (+ retry on failure), normalize, probe, mx, then
  probe-roots on a full run (args are harvest roots, probe-roots is
  country-scoped)."
  [args]
  (exit-code
   (when-let [fails (seq (for [[f outcome] (fetch-all! args) :when (= :fail outcome)]
                           (harvest-root f)))]
     (cmd-retry (vec fails)))
   (cmd-normalize nil)
   (cmd-probe args)
   (cmd-mx args)
   (when (empty? args) (cmd-probe-roots []))))

(defn- run-build
  "The consolidated files: aggregate, domains."
  [_]
  (exit-code (cmd-aggregate nil) (cmd-domains nil)))

(defn- run-report
  "The curation files: propose (which writes curate_todo.csv too), summary."
  [args]
  (exit-code (cmd-propose args) (cmd-summary args)))

(defn- run-all
  "The four phases in sequence. No argument: collect would take harvest
  roots, enrich and report country_dirs."
  [args]
  (if (seq args)
    (do (err "ERR: all takes no argument; run collect, enrich, build or report apart to narrow one") 1)
    (exit-code (run-collect []) (enrich/cmd-enrich []) (run-build []) (run-report []))))

(def commands
  "{sub-command handler}; a handler's integer result is the exit code."
  {"collect"     run-collect
   "enrich"      enrich/cmd-enrich
   "build"       run-build
   "report"      run-report
   "all"         run-all
   ;; collect
   "fetch"       cmd-fetch
   "retry"       cmd-retry
   "normalize"   cmd-normalize
   "probe"       cmd-probe
   "mx"          cmd-mx
   "probe-roots" cmd-probe-roots
   ;; enrich
   "build-qid"   enrich/cmd-build-qid
   "build-gec"   enrich/cmd-build-gec
   "build-un-ids" enrich/cmd-build-un-ids
   "subdivisions" enrich/cmd-subdivisions
   "wikidata"    enrich/cmd-wikidata
   "academia"    enrich/cmd-academia
   "ror"         enrich/cmd-ror
   "sopc"        enrich/cmd-sopc
   "iana"        enrich/cmd-iana
   "cia"         enrich/cmd-cia
   "un-desa"     enrich/cmd-un-desa
   "oecd"        enrich/cmd-oecd
   "meta"        enrich/cmd-meta
   ;; build
   "aggregate"   cmd-aggregate
   "domains"     cmd-domains
   ;; report
   "propose"     cmd-propose
   "todo"        cmd-todo
   "summary"     cmd-summary
   ;; other
   "probe-proposed" cmd-probe-proposed
   "wayback"     cmd-wayback
   "check"       cmd-check
   "esr"         registries/cmd-esr
   "govuk"       registries/cmd-govuk
   "validate-un" registries/cmd-validate-un
   "indegree"    indegree/cmd-indegree})

(defn usage []
  (println "Usage: bb scripts/pipeline.clj <command> [args…]")
  (println)
  (println "Phases (each chains the targeted commands below it):")
  (println "  collect | enrich | build | report | all")
  (println)
  (println "Targeted commands:")
  (println "  collect: fetch | retry | normalize | probe | mx | probe-roots")
  (println "  enrich:  build-qid | build-gec | build-un-ids | subdivisions")
  (println "           wikidata | academia | ror | sopc | iana | cia | un-desa | oecd | meta")
  (println "  build:   aggregate | domains")
  (println "  report:  propose | todo | summary")
  (println "  other:   probe-proposed | wayback | check | esr | govuk | validate-un | indegree")
  (println)
  (println "Directory harvesting moved to scripts/detect-from-directories.clj (bb directories)")
  (println)
  (println "Forge/catalog detection moved to swh-sopc-data-sources (bb forges)")
  (println)
  (println "Environment variables: FORCE=1, MAX_AGE=days, PARALLEL=N, TIMEOUT=Ns"))

;; Run only as a script (bb scripts/pipeline.clj …), not when required
;; from another namespace or loaded in a REPL.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

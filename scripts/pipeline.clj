#!/usr/bin/env bb
;; world-gov-domain-names -- full pipeline (Babashka).
;;
;; Four phases, each an aggregated command chaining targeted commands
;; in dependency order; every targeted command writes one kind of file.
;;
;;   collect            the harvest: fetch (+ retry on failure), normalize,
;;                      probe, mx, probe-roots -> sources/crtsh/,
;;                      sources/probes/roots.csv
;;   enrich             the per-country sources: build-qid, build-gec,
;;                      build-un-ids, subdivisions, then wikidata +
;;                      iana/cia/un-desa/oecd/meta in parallel
;;                      -> sources/wikidata/, data/sources/
;;   build              the consolidated files: aggregate, domains -> data/
;;   report             the curation files: propose, summary -> proposed.csv,
;;                      summary.md
;;   all                collect + enrich + build + report
;;
;; Targeted commands (collect):
;;   fetch [DOM…]       crt.sh fetch (1+ domains)
;;   retry [DOM…]       retry the FAILs from /tmp/fetch_subdomains.log
;;   normalize          clean every harvest file (sources/crtsh/<root>.csv)
;;   probe [DOM…]       HTTPS HEAD probe of the harvest rows with empty status
;;   mx [DOM…]          DNS MX lookup of the harvest rows -> mx column (email signal)
;;   probe-roots [C…]   HTTPS HEAD + MX of the confirmed roots that have no
;;                      harvest file -> sources/probes/roots.csv
;;
;; Targeted commands (enrich):
;;   build-qid          country_dir -> Wikidata QID: data/country_qid.csv
;;   build-gec          country_dir -> Factbook GEC code: data/factbook_gec.csv
;;   build-un-ids       country_dir -> UN/DESA id: data/un_desa_ids.csv
;;   subdivisions [Q:C…] Wikidata first-level subdivisions (P150)
;;                      -> sources/wikidata/subdivisions_level1.csv
;;   wikidata [Q:C…]    Wikidata central administration
;;                      -> sources/wikidata/central_admin.csv (needs subdivisions)
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
;;   propose [C…]       score the candidate hosts -> countries/<c>/proposed.csv
;;   summary [C…]       the per-country report -> countries/<c>/summary.md
;;   cross-check [C…]   alias of report
;;
;; Other commands:
;;   probe-proposed [C…] HTTPS HEAD + MX of the proposed hosts
;;                      -> sources/probes/proposed.csv (propose joins them)
;;   check [C…]         compile the decision files: exit 1 on a contradiction,
;;                      WARN on a curated row a registry already lists
;;   cisa               fetch CISA federal .gov registry -> sources/cisa/registered.csv
;;   lannuaire          fetch FR service-public.gouv.fr directory -> sources/lannuaire/registered.csv
;;   govuk              build UK sub-central list -> GBR sources/govuk/registered.csv
;;   validate-un        check un_status against the official UN member list
;;   indegree [C…]      link-graph in-degree (eu-plus-government-scans)
;;                      -> countries/<c>/sources/linkgraph/indegree.csv
;;
;; Environment variables:
;;   FORCE=1            force-overwrite existing outputs; re-probe every host
;;                      (probe, mx, probe-roots, probe-proposed)
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

;; ===========================================================================
;;  Harvest files -- countries/<c>/sources/crtsh/<root>.csv
;; ===========================================================================
;;
;; One file per harvested root domain, columns subdomain,http_status,mx:
;; every host crt.sh ever saw under the root, the root's apex included,
;; with the HTTPS probe and the MX lookup of each. Harvesting a root is a
;; choice distinct from confirming it: `fetch <root>` creates the file of a
;; confirmed central or central-1 domain (a harvest root, see
;; common/harvest-roots); harvest roots without a file (registry roots,
;; mostly) only get their apex probed, into
;; countries/<c>/sources/probes/roots.csv (domain,http_status,mx). local
;; rows are never harvested.

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
  "Write [host http_status mx] rows to a harvest file: add the apex row,
  keep the first non-blank status and mx per host, sort by host."
  [file rows]
  (let [merged (reduce (fn [m [h st mx]]
                         (update m h (fn [[s0 m0]]
                                       [(if (str/blank? s0) (or st "") s0)
                                        (if (str/blank? m0) (or mx "") m0)])))
                       {} (cons [(harvest-root file) "" ""] rows))]
    (write-csv-file file harvest-header
                    (for [[h [st mx]] (sort-by first merged)] [h st mx]))))

(defn read-probe-file
  "{host [http_status mx]} of a probes file (<key>,http_status,mx)."
  [path]
  (into {} (for [[h st mx] (rest (read-csv-raw path)) :when (not (str/blank? h))]
             [h [(or st "") (or mx "")]])))

(defn write-probe-file! [path key-header probes]
  (write-csv-file path [key-header "http_status" "mx"]
                  (for [[h [st mx]] (sort-by first probes)] [h st mx])))

(defn unharvested-roots
  "Harvest roots of a country without a harvest file."
  [country-dir]
  (remove #(fs/exists? (harvest-file country-dir %)) (harvest-roots country-dir)))

;; sources/probes/roots.csv: the harvest roots that have no harvest file.
(defn read-probes [country-dir] (read-probe-file (country-src country-dir "probes" "roots.csv")))
(defn write-probes!
  "Write sources/probes/roots.csv of a country, keeping only its
  unharvested roots."
  [country-dir probes]
  (write-probe-file! (country-src country-dir "probes" "roots.csv") "domain"
                     (select-keys probes (unharvested-roots country-dir))))

;; sources/probes/proposed.csv: the hosts proposed for validation.
(defn read-proposed-probes [country-dir] (read-probe-file (country-src country-dir "probes" "proposed.csv")))
(defn write-proposed-probes! [country-dir probes]
  (write-probe-file! (country-src country-dir "probes" "proposed.csv") "hostname" probes))

(defn resolve-harvest-files
  "Harvest file paths of the given domains: the existing files, else the
  file a confirmed central or central-1 root would have (ERR and skip
  any other domain). Every existing harvest file when args is empty."
  [args]
  (if (seq args)
    (vec (mapcat (fn [d]
                   (let [d (str/lower-case d)
                         existing (map str (fs/glob "countries" (str "*/sources/crtsh/" d ".csv")))
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
  them into the file, keeping the probes of known hosts. :ok, or :fail
  with nothing written."
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
          :fail))))

(def fetch-fail-log "/tmp/fetch_subdomains.log")

(defn fetch-all!
  "fetch-one! over the harvest files of args: [[file :ok|:fail]…].
  Record this run's failures in fetch-fail-log."
  [args]
  (let [files   (resolve-harvest-files args)
        results (bounded-pmap (parallel 4) (fn [f] [f (fetch-one! f)]) files)
        fails   (->> results (filter #(= :fail (second %))) (map (comp harvest-root first)))]
    ;; Record this run's failures (fresh, never appended) so `retry` -- whether
    ;; called inside run-collect or as a standalone command -- replays exactly
    ;; the domains that just failed, not a stale log from a previous session.
    (spit fetch-fail-log (str/join "\n" (map #(str "FAIL " %) fails)))
    results))

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
  (let [domains (if (seq args)
                  args
                  (when (fs/exists? fetch-fail-log)
                    (->> (slurp fetch-fail-log) str/split-lines
                         (filter #(str/starts-with? % "FAIL"))
                         (map #(second (str/split % #"\s+")))
                         (filter seq))))]
    (if (empty? domains)
      (do (err "No FAIL in " fetch-fail-log " (and no argument provided).") 1)
      (if (some #{:fail} (bounded-pmap (parallel 2) retry-one! (resolve-harvest-files (vec domains))))
        1
        0))))

;; ===========================================================================
;;  collect -- normalize
;; ===========================================================================

(defn normalize-harvest-rows
  "Clean [host status mx] rows: lowercase the host, strip wildcard
  prefix, URL scheme, path, port and trailing dot, drop invalid
  hostnames, single-line the probes."
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
  status, 1 = MX) is blank; every host with FORCE=1."
  [probes idx]
  (for [[h v] probes :when (or force? (str/blank? (nth v idx)))] h))

(defn- fill-hosts!
  "Fill column idx of a {host [http_status mx]} map with (lookup host)
  for hosts, conc lookups at a time."
  [probes idx lookup conc hosts]
  (reduce (fn [m [h v]] (assoc-in m [h idx] v))
          probes
          (bounded-pmap conc (fn [h] [h (lookup h)]) hosts)))

(defn- harvest->probes [rows] (into {} (for [[h st mx] rows] [h [st mx]])))
(defn- probes->harvest [probes] (for [[h [st mx]] probes] [h st mx]))

(defn- with-mx-count [hosts probes]
  (count (remove #(#{"" "none" "dig error"} (second (get probes %))) hosts)))

(defn- roots-probes
  "{root [http_status mx]} of a country's unharvested roots, from
  sources/probes/roots.csv; blank for a root not probed yet."
  [country-dir]
  (let [roots (unharvested-roots country-dir)]
    (merge (zipmap roots (repeat ["" ""]))
           (select-keys (read-probes country-dir) roots))))

(defn probe-domain!
  "Probe the hosts of a harvest file whose status is blank and write
  the results into its http_status column."
  [file timeout]
  (when (fs/exists? file)
    (let [root-name (harvest-root file)
          probes    (harvest->probes (read-harvest file))
          todo      (blank-hosts probes 0)]
      (if (empty? todo)
        (println (str "[" root-name "] no empty-status row to probe"))
        (do (println (str "[" root-name "] " (count todo) " subdomains to probe"))
            (write-harvest! file (probes->harvest
                                   (fill-hosts! probes 0 #(second (probe-one! % timeout)) (parallel 50) todo))))))))

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
  "Look up the MX of the hosts of a harvest file whose mx is blank and
  fill the column."
  [file conc]
  (let [root-name (harvest-root file)
        probes    (harvest->probes (read-harvest file))
        todo      (blank-hosts probes 1)]
    (if (empty? todo)
      (println (str "[" root-name "] mx: nothing to look up"))
      (let [filled (fill-hosts! probes 1 mx-lookup conc todo)]
        (write-harvest! file (probes->harvest filled))
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
;; Same shape as probe-proposed: country-scoped, both probe columns of
;; one probes file at once.

(defn probe-roots!
  "Probe (HTTPS HEAD, MX) the unharvested roots of a country whose
  columns are blank and write sources/probes/roots.csv."
  [country-dir timeout conc]
  (let [probes    (roots-probes country-dir)
        todo-http (blank-hosts probes 0)
        todo-mx   (blank-hosts probes 1)]
    (when (or (seq todo-http) (seq todo-mx))
      (let [filled (-> probes
                       (fill-hosts! 0 #(second (probe-one! % timeout)) conc todo-http)
                       (fill-hosts! 1 mx-lookup conc todo-mx))]
        (write-probes! country-dir filled)
        (println (str "[" country-dir "/probes] roots: " (count todo-http) " probed, "
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
;; data/sources/country_data.csv (enrich, meta): run after both.

(defn country-hosts
  "[host parent_domain http_status mx] rows of a country: every host of
  its harvest files, plus the unharvested roots with their probes from
  sources/probes/roots.csv. Sorted by host."
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
    (let [counts (frequencies (map #(nth % 3) rows))]
      (println (str "Wrote " public-sector-file " (" (count rows) " hosts)"))
      (println (str "  UN members: " (get counts "member" 0)
                    " ; observers: " (get counts "observer" 0)
                    " ; non-UN: " (get counts "non_un" 0))))))

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

(defn central1-under
  "{[country root] #{label…}} of the confirmed central-1 domains sitting
  directly under a confirmed central root: the exact rows of the policy
  table."
  []
  (labels-under-central (for [[c d level] (confirmed-rows) :when (= level "central-1")] [c d])))

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
   "NLD_netherlands"    #{"aw" "cw" "sx"}})

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
  scripts/README.org). Keys: :host, :wd-count (Wikidata mentions),
  :label (pipe-joined Wikidata labels), :fb-phrases (lowercased
  Factbook institution phrases), :un-portal-host, :cctld-primary
  (without the dot), :indegree (nil when absent from the link graph),
  :directory-listed?, :subdiv-penalty? (default true; false for a host
  already known as subnational)."
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
  "Level of a candidate host: its Wikidata levels first, then its
  label (subdiv-pattern means local, a first-level subdivision name
  means central-1). Blank when unknown."
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
  "One [hostname score sources label level] row of host from a
  load-candidate-ctx context. Pure."
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

(defn level1-pattern
  "Word-bounded, case-insensitive pattern matching any of the labels
  (4+ chars) of a country's first-level subdivisions; nil when none."
  [labels]
  (let [labels (->> labels (remove str/blank?) (filter #(>= (count %) 4)))]
    (when (seq labels)
      (re-pattern
        (str "(?iu)\\b(?:"
             (str/join "|" (map #(java.util.regex.Pattern/quote %) labels))
             ")\\b")))))

(defn load-candidate-ctx
  "Read what propose-candidates needs for a country: the Wikidata
  mentions and levels, the UN/DESA portal, the ccTLD, the Factbook
  courts, the link-graph in-degrees, the directory listing, the
  first-level subdivision labels, the confirmed domains of every
  country and the excluded ones of this country."
  [country-dir]
  (let [src #(country-src country-dir %1 %2)
        wd-path   (src "wikidata" "central_admin.csv")
        sub-path  (src "wikidata" "subdivisions_level1.csv")]
    {:wd-by-host     (wikidata-by-host (when (fs/exists? wd-path) (rest (read-csv-raw wd-path))))
     :lg-by-host     (into {} (for [{:strs [hostname indegree]} (read-csv-file (src "linkgraph" "indegree.csv"))
                                    :let [n (parse-long (or indegree ""))]
                                    :when (and n (>= n indegree/linkgraph-min-indegree))]
                                [hostname n]))
     :dir-by-host    (into {} (for [{:strs [hostname evidence]} (read-csv-file (src "directory" "orgs.csv"))]
                                [hostname {:evidence (or evidence "")}]))
     :un-portal-host (extract-host (table-field "un_desa" country-dir "national_portal"))
     :fb-phrases     (extract-factbook-phrases (table-field "cia_factbook" country-dir "judicial_highest_courts"))
     :cctld-primary  (not-empty (str/replace (table-field "iana" country-dir "cctld") #"^\." ""))
     :level1-pattern (when (fs/exists? sub-path)
                       (level1-pattern (map second (rest (read-csv-raw sub-path)))))
     :known          (confirmed-domains)
     :excluded       (get @excluded-domains country-dir #{})}))

(defn propose-candidates
  "Rank the hosts of a context: every valid hostname a source mentions
  that no confirmed or excluded domain covers, scored and levelled by
  candidate-row, local ones dropped. [hostname score sources label
  level] rows, best scores first. Pure."
  [{:keys [wd-by-host lg-by-host dir-by-host un-portal-host known excluded] :as ctx}]
  (->> (cond-> (-> (set (keys wd-by-host))
                   (into (keys lg-by-host))
                   (into (keys dir-by-host)))
         un-portal-host (conj un-portal-host))
       (filter valid-hostname?)
       (remove #(host-covered? % known))
       (remove #(host-covered? % excluded))
       (map #(candidate-row ctx %))
       (remove #(= "local" (nth % 4)))
       (sort-by (juxt #(- (nth % 1)) first))))

(defn propose-country!
  "Write countries/<c>/proposed.csv
  (hostname,score,sources,label,level,http_status,mx): the ranked
  candidates joined with the probes of sources/probes/proposed.csv."
  [country-dir]
  (let [probes (read-proposed-probes country-dir)
        out    (str "countries/" country-dir "/proposed.csv")
        rows   (for [[h sc src lbl lvl] (propose-candidates (load-candidate-ctx country-dir))
                     :let [[st mx] (get probes h ["" ""])]]
                 [h (str sc) src lbl lvl st mx])]
    (write-csv-file out ["hostname" "score" "sources" "label" "level" "http_status" "mx"] rows)
    (println (str "=== " country-dir " -> " out " (" (count rows) " proposed)"))))

(defn cmd-propose [args] (iter-countries propose-country! args))

(defn probe-proposed!
  "Probe (HTTPS HEAD, MX) the hosts of proposed.csv whose columns are
  blank and write sources/probes/proposed.csv, keeping only the hosts
  still proposed. Leave proposed.csv as is."
  [country-dir timeout conc]
  (let [hosts  (for [[h] (rest (read-csv-raw (str "countries/" country-dir "/proposed.csv")))
                     :when (valid-hostname? h)]
                 h)
        probes (merge (zipmap hosts (repeat ["" ""]))
                      (select-keys (read-proposed-probes country-dir) hosts))
        todo-http (blank-hosts probes 0)
        todo-mx   (blank-hosts probes 1)]
    (if (and (empty? todo-http) (empty? todo-mx))
      (println (str "[" country-dir "] proposed: nothing to probe"))
      (let [filled (-> probes
                       (fill-hosts! 0 #(second (probe-one! % timeout)) conc todo-http)
                       (fill-hosts! 1 mx-lookup conc todo-mx))]
        (write-proposed-probes! country-dir filled)
        (println (str "[" country-dir "] proposed: " (count todo-http) " probed, "
                      (count todo-mx) " MX looked up ("
                      (count (filter #(re-matches #"[23]\d\d" (first %)) (vals filled)))
                      " reachable, " (with-mx-count hosts filled) " with MX)"))))))

(defn cmd-probe-proposed [args]
  (let [timeout (env-int "TIMEOUT" 5)
        conc    (parallel 50)
        code    (iter-countries #(probe-proposed! % timeout conc) args)]
    (when (zero? code)
      (println "Run 'bb pipeline propose' to fold the probes into proposed.csv."))
    code))

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
        (let [parent (parent-domain host)]
          (if (and parent (fs/exists? (harvest-file country-dir parent)))
            (println (str "- ⚠️ Exact hostname not collected, but `" parent "` is harvested (to be probed)"))
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

(defn summary-country!
  "Write countries/<c>/summary.md: metadata, UN/DESA portal coverage,
  Factbook institutions, the top of proposed.csv, ccTLD anomalies."
  [country-dir collected-by-country un-status-by-country]
  (let [prop-path  (str "countries/" country-dir "/proposed.csv")
        out        (str "countries/" country-dir "/summary.md")
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
            (section-un-portal country-dir un-portal collected)
            (section-factbook ctx)
            (section-proposed prop-path)
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
  "Drop [domain excl apex?] entries already covered by another entry."
  [entries]
  (filter (fn [[d _]] (not-any? #(covered-by? d %) entries)) entries))

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
  "Write the match policy table
  data/public-sector-domains-central+-policy.csv
  (domain,kind,country,level) from the confirmed central and central-1
  domains: `subtree` covers a domain and everything below it, `exact`
  a domain alone, `exclude` keeps a domain and everything below it
  out; the longest matching domain decides. A central-1 domain
  directly under a central root becomes an exact row, a local or
  excluded label under one an exclude row, and a domain a root already
  covers is dropped. level is the tier of the row's domain, blank for
  a row from excluded.csv. A domain confirmed in several countries gets
  one row, for the first country in ASCII order."
  [_]
  (let [rows (central-plus-rows)
        excl (local-labels)
        c1-under (central1-under)
        apexes (set (for [[[c root] labels] c1-under
                          label labels]
                      [c (str label "." root)]))
        level-of (into {} (for [[c d level] (confirmed-rows)] [[c d] level]))
        ;; Same-domain rows across countries keep the union of
        ;; exclusions; a domain that is an apex in any country stays
        ;; an apex.
        entries (->> rows
                     (reduce (fn [m [d c]]
                               (update m d
                                       (fn [[es ap]]
                                         ;; labels to keep out of the root's
                                         ;; subtree: local and excluded ones
                                         ;; (exclude rows) and confirmed
                                         ;; central-1 ones (exact rows)
                                         [(-> (or es #{})
                                              (into (get excl [c d]))
                                              (into (get c1-under [c d])))
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
                          :let [c (country-of d)]
                          [dom kind] (cons [d (if apex? "exact" "subtree")]
                                           (for [l excl
                                                 :let [ld (str l "." d)]
                                                 :when (not (apex-domains ld))]
                                             [ld "exclude"]))]
                      [dom kind c (get level-of [c dom] "")])
                    (sort-by (juxt first second)))]
    (write-csv-file policy-file ["domain" "kind" "country" "level"] policy)
    (println (str "Wrote " policy-file " (" (count policy) " rows: "
                  (count (filter #(= "central" (nth % 2)) rows)) " central + "
                  (count (filter #(= "central-1" (nth % 2)) rows)) " central-1 domains in scope)"))))

;; ===========================================================================
;;  check -- the decision files compile
;; ===========================================================================

(defn cmd-check
  "Compile the decision files of the given countries (all by default).
  ERR and 1 on a domain both curated and excluded, an unknown level, a
  first cell that is not a hostname or a domain curated with two
  levels; WARN on a domain listed twice with the same level or a
  curated row a registry already lists at that level."
  [args]
  (if-let [countries (scoped-countries args)]
    (let [check-country
          (fn [c]
            (let [errors (atom 0)
                  err!   (fn [& msg] (swap! errors inc) (apply err "ERR: " c ": " msg))]
              (doseq [[file path] [["curated.csv" (curated-file c)] ["excluded.csv" (excluded-file c)]]
                      [kind x] (file-anomalies path)]
                (case kind
                  :invalid-hostname (err! "'" x "' in " file " is not a hostname (row dropped)")
                  :duplicate        (err "WARN: " c ": " x " is listed twice in " file " (first row wins)")))
              (doseq [[d rows] (group-by first (read-curated c))
                      :let [levels (distinct (map second rows))]
                      :when (next levels)]
                (err! d " is curated with two levels (" (str/join ", " levels) ")"))
              (try
                (confirmed-country c)
                (let [registered (into {} (for [r (registries c), [d level] (read-registered c r)]
                                            [d [r level]]))]
                  (doseq [[d level name] (read-curated c)
                          :let [[r rlevel] (get registered d)]
                          :when (and (= level rlevel) (str/blank? name))]
                    (err "WARN: " c ": " d " (" level ") is curated but " r " already lists it")))
                (catch clojure.lang.ExceptionInfo e
                  (err! (ex-message e))))
              (zero? @errors)))
          failed (count (remove true? (doall (map check-country countries))))]
      (if (pos? failed)
        1
        (do (println (str "Decision files of " (count countries) " countries compile")) 0)))
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
   (when (some #(= :fail (second %)) (fetch-all! args))
     (cmd-retry []))
   (cmd-normalize nil)
   (cmd-probe args)
   (cmd-mx args)
   (when (empty? args) (cmd-probe-roots []))))

(defn- run-build
  "The consolidated files: aggregate, domains."
  [_]
  (exit-code (cmd-aggregate nil) (cmd-domains nil)))

(defn- run-report
  "The curation files: propose, summary."
  [args]
  (exit-code (cmd-propose args) (cmd-summary args)))

(defn- run-all [args]
  (exit-code (run-collect args) (enrich/cmd-enrich args) (run-build args) (run-report args)))

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
   "summary"     cmd-summary
   "cross-check" run-report
   ;; other
   "probe-proposed" cmd-probe-proposed
   "check"       cmd-check
   "cisa"        registries/cmd-cisa
   "lannuaire"   registries/cmd-lannuaire
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
  (println "           wikidata | iana | cia | un-desa | oecd | meta")
  (println "  build:   aggregate | domains")
  (println "  report:  propose | summary (cross-check = report)")
  (println "  other:   probe-proposed | check | cisa | lannuaire | govuk | validate-un | indegree")
  (println)
  (println "Directory harvesting moved to scripts/detect-from-directories.clj (bb directories)")
  (println)
  (println "Forge/catalog detection moved to scripts/detect-forges.clj (bb forges)")
  (println)
  (println "Environment variables: FORCE=1, PARALLEL=N, TIMEOUT=Ns"))

;; Run only as a script (bb scripts/pipeline.clj …), not when required
;; from another namespace or loaded in a REPL.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

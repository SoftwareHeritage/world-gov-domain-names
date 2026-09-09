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
  (:require [common :refer :all]
            [enrich :as enrich]
            [registries :as registries]
            [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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

(defn read-probe-file
  "{host [http_status mx]} of a probes file (<key>,http_status,mx)."
  [path]
  (into {} (for [[h st mx] (rest (read-csv-raw path)) :when (not (str/blank? h))]
             [h [(or st "") (or mx "")]])))

(defn write-probe-file! [path key-header probes]
  (write-csv-file path [key-header "http_status" "mx"]
                  (for [[h [st mx]] (sort-by first probes)] [h st mx])))

;; sources/probes/roots.csv: the validated roots that have no harvest file.
(defn read-probes [country-dir] (read-probe-file (country-src country-dir "probes" "roots.csv")))
(defn write-probes! [country-dir probes]
  (write-probe-file! (country-src country-dir "probes" "roots.csv") "domain" probes))

;; sources/probes/proposed.csv: the hosts proposed for validation.
(defn read-proposed-probes [country-dir] (read-probe-file (country-src country-dir "probes" "proposed.csv")))
(defn write-proposed-probes! [country-dir probes]
  (write-probe-file! (country-src country-dir "probes" "proposed.csv") "hostname" probes))

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
  failure: the exception classes from the outermost to the root cause
  (e.g. \"ConnectException > UnresolvedAddressException\" for a name without
  a DNS record, \"ConnectException > ClosedChannelException\" for a refused
  connection) and the deepest message, when any. The Java client wraps the
  cause in a message-less ConnectException, which alone hides it."
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
  (let [timeout (env-int "TIMEOUT" 5)]
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

(defn- central-roots
  "#{[country root]} of the level=central rows of every validated.csv."
  []
  (set (for [[c d level] (validated-rows) :when (= level "central")] [c d])))

(defn- labels-under-central
  "{[country root] #{label…}} from [country hostname] pairs: keep the
  hostnames sitting directly under a validated central root of the same
  country (sp.gov.br under gov.br -> {[BRA gov.br] #{\"sp\"}}), as labels
  of that root."
  [pairs]
  (let [central (central-roots)]
    (reduce (fn [m [c d]]
              (let [[_ label root] (re-matches #"([a-z0-9-]+)\.(.+)" d)]
                (if (and root (contains? central [c root]))
                  (update m [c root] (fnil conj #{}) label)
                  m)))
            {} pairs)))

(defn central1-under
  "{[country root] #{label…}}: the label of every validated central-1
  domain sitting directly under a validated central root. Such a domain
  is an apex of the policy table (exact row) and its label leaves the
  root's subtree, so no lower-tier host registered under it
  (campinas.sp.gov.br) passes as central."
  []
  (labels-under-central (for [[c d level] (validated-rows) :when (= level "central-1")] [c d])))

(defn excluded-hostnames
  "The hostnames of countries/<c>/excluded.csv (hand-curated) and every
  countries/<c>/sources/*/excluded.csv (generated, e.g. cmd-govuk), both
  with columns domain,name where domain is a full hostname."
  [country-dir]
  (for [path (cons (str "countries/" country-dir "/excluded.csv")
                   (map str (fs/glob (str "countries/" country-dir "/sources") "*/excluded.csv")))
        [domain] (rest (read-csv-raw path))
        :let [domain (some-> domain str/trim str/lower-case)]
        :when (valid-hostname? domain)]
    domain))

(defn excluded-labels
  "{[country root] #{label…}}: the excluded hostnames sitting directly
  under a validated central root, whose whole subtree belongs to a lower
  government tier (UK councils under gov.uk): exclude rows of the policy
  table. Excluded hostnames elsewhere only keep hosts out of
  proposed.csv. Roots without an entry are level-homogeneous: everything
  under them is central government."
  []
  (labels-under-central (for [c (country-dirs), d (excluded-hostnames c)] [c d])))

(def excluded-domains
  "{country_dir #{domain…}}: every excluded hostname of a country. A host
  equal to or under one of them is out of scope, whatever the sources
  say, so proposed.csv never lists it again: excluded.csv is where a
  reviewer's 'no' is recorded."
  (delay (into {} (for [c (country-dirs)] [c (set (excluded-hostnames c))]))))

(defn cmd-central [_]
  ;; Extract data/public-sector-domains-central+.csv: one row per
  ;; central-government root domain (level central) plus the first-tier
  ;; bodies (level central-1), for the central +
  ;; first-subdivision report scope. A root stands for all its
  ;; subdomains, so these are the email domains the report needs.
  ;; Source: the central and central-1 rows of countries/<c>/validated.csv
  ;; -- only CONFIRMED domains, mirroring the manual gate of the central
  ;; level (decision of 2026-08-17); unconfirmed central-1 hosts stay in
  ;; proposed.csv as the curation worklist. UN-facing: members/observers
  ;; only. Carries the domain's MX as an email signal when available;
  ;; central-1 rows keep their body's name and the `registry` channel.
  (let [un-by-country  (build-un-status-map)
        meta-by-country (country-meta-map)
        plus-rows
        (->> (for [[c d level _ name] (validated-rows)
                   :when (#{"central" "central-1"} level)
                   :let [un (get un-by-country c "member")
                         m  (get meta-by-country c)
                         c1? (= level "central-1")]
                   :when (not= un "non_un")]
               [d c level un (:region m) (:langs m) (:gdp m)
                (if c1? name "") "" (if c1? "registry" "") (second (root-probes c d))])
             ;; one row per [domain country]; should a domain be listed
             ;; twice, the central row wins ("central" sorts first)
             (sort-by #(nth % 2))
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
        (reduce (fn [m {:strs [hostname evidence]}]
                  (if (host-covered? hostname known)
                    m
                    (assoc m hostname {:evidence (or evidence "")})))
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
             ;; Wikidata websites yield a few non-hostnames (IDN with
             ;; accents, a bare "http", a trailing space): not proposable
             (filter valid-hostname?)
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
  (let [timeout (env-int "TIMEOUT" 5)
        conc    (parallel 50)]
    (iter-countries #(probe-proposed! % timeout conc) args)))

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
  (println (str "- Hosts collected: " n-collected))
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
                                           ;; labels to keep out of the root's
                                           ;; subtree: excluded ones (exclude
                                           ;; rows) and validated central-1
                                           ;; ones (exact rows)
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
    (let [by-slug   @slug->country-dir
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
  (when (some #(= :fail (second %)) (cmd-fetch args))
    (cmd-retry []))
  (cmd-normalize nil)
  (cmd-probe args)
  (cmd-aggregate nil)
  (cmd-central nil))

(def commands
  "Map sub-command name -> handler. Used both by dispatcher and usage banner."
  {"collect"     run-collect
   "enrich"      enrich/cmd-enrich
   "report"      cmd-cross-check
   "fetch"       cmd-fetch
   "retry"       cmd-retry
   "normalize"   cmd-normalize
   "probe"       cmd-probe
   "mx"          cmd-mx
   "aggregate"   cmd-aggregate
   "central"     cmd-central
   "cisa"        registries/cmd-cisa
   "lannuaire"   registries/cmd-lannuaire
   "govuk"       registries/cmd-govuk
   "wikidata"    enrich/cmd-wikidata
   "iana"        enrich/cmd-iana
   "cia"         enrich/cmd-cia
   "un-desa"     enrich/cmd-un-desa
   "oecd"        enrich/cmd-oecd
   "meta"        enrich/cmd-meta
   "cross-check" cmd-cross-check
   "probe-proposed" cmd-probe-proposed
   "build-qid"   registries/cmd-build-qid
   "validate-un" registries/cmd-validate-un
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

(defn dispatch
  "Run cmd. A command that returns an integer sets the exit code (cisa,
  lannuaire, govuk, build-qid and retry return 1 when their fetch failed),
  so a failed refresh is visible to a shell or a cron job; anything else
  exits 0."
  [cmd args]
  (let [result (cond
                 (= cmd "all")
                 (do (run-collect args) (enrich/cmd-enrich args) (cmd-cross-check args))

                 (#{"-h" "--help" "help"} cmd) (usage)

                 :else
                 (if-let [f (get commands cmd)]
                   (f args)
                   (do (err "ERR: unknown sub-command '" cmd "'")
                       (usage)
                       1)))]
    (System/exit (if (integer? result) result 0))))

;; Run only as a script (bb scripts/pipeline.clj …), not when required
;; from another namespace or loaded in a REPL.
(when (= *file* (System/getProperty "babashka.file"))
  (let [args *command-line-args*]
    (if (empty? args)
      (do (usage) (System/exit 1))
      (dispatch (first args) (vec (rest args))))))

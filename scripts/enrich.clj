(ns enrich
  "Enrichment sources: Wikidata (subdivisions, central administration)
  under countries/<c>/sources/wikidata/, and IANA, CIA Factbook, UN/DESA,
  OECD and country metadata as one row per country in
  data/sources/<source>.csv. build-qid, build-gec and build-un-ids
  write the country_dir -> source id files three sources read.
  pipeline.clj dispatches."
  (:require [common :refer :all]
            [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; Per-source concurrency limits. Each thread fires HTTP requests against
;; the same endpoint; numbers are picked to stay well under typical rate
;; limits while still saturating bandwidth. Tunable via env vars.
(def conc-wikidata (env-int "CONC_WIKIDATA" 3))
(def conc-iana     (env-int "CONC_IANA"     4))
(def conc-cia      (env-int "CONC_CIA"      8))
(def conc-un-desa  (env-int "CONC_UN_DESA"  4))
(def conc-meta     (env-int "CONC_META"     4))

;; ===========================================================================
;;  Mapping files -- data/<source>_<id>.csv, country_dir -> source id
;; ===========================================================================

(defn- build-country-map!
  "Write map-file, a country_dir -> source id table: the rows of
  aliases-file (optional, they win) then the [country_dir …] rows of
  (fetch-matches), one per country_dir. Skip it when it already maps
  150+ countries unless FORCE=1. 1 when fetch-matches returns nil
  (nothing written), 0 otherwise."
  [map-file header aliases-file label fetch-matches]
  (if (and (fs/exists? map-file)
           (not force?)
           (> (dec (count (read-csv-raw map-file))) 150))
    (do (println (str map-file " : SKIP (use FORCE=1 to rebuild)")) 0)
    (do (err "Building country_dir <-> " label " map…")
        (if-let [matches (fetch-matches)]
          (let [aliases (rest (or (when aliases-file (read-csv-raw aliases-file)) []))
                dedup   (dedup-by-first (concat aliases matches))]
            (write-csv-file map-file header dedup)
            (println (str "Wrote " map-file " (" (count dedup) " countries mapped)"))
            0)
          (do (err "ERR: could not fetch the " label " list; " map-file " left untouched")
              1)))))

;; ===========================================================================
;;  subdivisions + wikidata -- first-level subdivisions, central administration
;; ===========================================================================

(def wikidata-endpoint "https://query.wikidata.org/sparql")

(def qid-map-file "data/country_qid.csv")

(defn cmd-build-qid
  "Write data/country_qid.csv (country_dir,iso3,wikidata_qid) from the
  P298 ISO3 codes of Wikidata."
  [_]
  (build-country-map!
   qid-map-file ["country_dir" "iso3" "wikidata_qid"] nil "Wikidata QID"
   (fn []
     (let [q "SELECT DISTINCT ?country ?iso3 WHERE { ?country wdt:P31 wd:Q6256 ; wdt:P298 ?iso3 . }"
           body (http-get wikidata-endpoint
                          {:timeout 120 :retries 3
                           :accept "application/sparql-results+json"
                           :query-params {"query" q}})]
       (when-not (str/blank? body)
         (let [iso3->qid (->> (-> body (json/parse-string true) :results :bindings)
                              (map (fn [b]
                                     [(-> b :iso3 :value)
                                      (-> b :country :value (str/replace #"^.*/" ""))]))
                              (into {}))]
           (for [c (country-dirs)
                 :let [iso3 (first (str/split c #"_"))
                       qid (get iso3->qid iso3)]
                 :when qid]
             [c iso3 qid])))))))

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
  "Level of an org from its P1001 jurisdiction QIDs: \"central\" when one
  is the country, \"central-1\" when one is a first-level subdivision,
  \"local\" otherwise, \"\" when there is none."
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

(defn- wikidata-bindings
  "The :bindings of a SPARQL JSON body; nil when the body is missing or
  truncated."
  [body]
  (when body
    (try (-> (json/parse-string body true) :results :bindings)
         (catch Exception _ nil))))

(defn- qid-pairs
  "[[qid country_dir]…] of args, each a country_dir (resolved through
  data/country_qid.csv) or a QID:country_dir pair; every country of the
  file when args is empty. ERR and nil on a missing file or an unknown
  country."
  [args]
  (let [rows   (read-csv-file qid-map-file)
        qid-of (into {} (for [row rows] [(get row "country_dir") (get row "wikidata_qid")]))
        pairs  (if (seq args)
                 (for [a args :let [[x c] (str/split a #":" 2)]]
                   (if c [x c] [(get qid-of x) x]))
                 (for [row rows] [(get row "wikidata_qid") (get row "country_dir")]))
        unknown (for [[qid c] pairs :when (str/blank? qid)] c)]
    (cond
      (empty? rows) (err "ERR: " qid-map-file " missing. Run 'bb pipeline build-qid' first")
      (seq unknown) (err "ERR: not in " qid-map-file ": " (str/join ", " unknown)
                         " (expected a country_dir or QID:country_dir)")
      :else         (vec pairs))))

(defn- run-per-qid
  "Run (f qid country_dir) over the qid-pairs of args, conc-wikidata at
  a time. 1 when the pairs could not be resolved or f returned :fail
  for a country, 0 otherwise."
  [args f]
  (if-let [pairs (qid-pairs args)]
    (if (some #{:fail} (bounded-pmap conc-wikidata (fn [[qid c]] (f qid c)) pairs)) 1 0)
    1))

(defn subdivisions-file [country-dir]
  (country-src country-dir "wikidata" "subdivisions_level1.csv"))

(defn subdivisions-process!
  "Write sources/wikidata/subdivisions_level1.csv (qid,label): the
  country's P150 values, dissolved ones excluded. Skip an existing file
  unless FORCE=1. :ok, :skip or :fail (nothing written)."
  [country-qid country-dir]
  (let [out (subdivisions-file country-dir)]
    (if (skip? out)
      (do (println (str "=== " country-dir " (" country-qid ") subdivisions : SKIP (use FORCE=1 to refetch)"))
          :skip)
      (if-let [bindings (wikidata-bindings (wikidata-run-query (wikidata-subdivisions-query country-qid)))]
        (let [rows (->> bindings
                        (keep (fn [b]
                                (when-let [qid (some-> (get-in b [:sub :value])
                                                       (str/replace #"^.*/" ""))]
                                  [qid (get-in b [:subLabel :value] "")])))
                        distinct
                        (sort-by first))]
          (write-csv-file out ["qid" "label"] rows)
          (println (str "=== " country-dir " (" country-qid ") subdivisions -> " out
                        " (" (count rows) " first-level entities)"))
          (Thread/sleep 1000)
          :ok)
        (do (err "ERR: " country-dir ": subdivisions query failed; " out " left untouched")
            :fail)))))

(defn cmd-subdivisions
  "subdivisions-process! over the countries of args (qid-pairs)."
  [args]
  (run-per-qid args subdivisions-process!))

(defn wikidata-bindings->rows
  "[type label website hostname level] rows of the bindings of one class
  query, one per distinct host, the level derived per org from all its
  jurisdictions. Pure."
  [type bindings country-qid level1]
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

(defn- wikidata-class-rows!
  "Run one class query for a country and return its rows
  (wikidata-bindings->rows); retry a failed :strict query as :light.
  nil when the query failed or its answer was truncated."
  [[qid type strictness] country-qid level1]
  (let [body (or (wikidata-run-query (wikidata-query qid country-qid strictness))
                 (when (= strictness :strict)
                   (err (str "  [" type "] strict query failed; retrying light"))
                   (wikidata-run-query (wikidata-query qid country-qid :light))))]
    (if-let [bindings (wikidata-bindings body)]
      (do (err (str "  [" type "] " (count bindings) " results"))
          (Thread/sleep 1000)
          (wikidata-bindings->rows type bindings country-qid level1))
      (do (err (str "  [" type "] " (if body "truncated answer" "failed after 3 attempts")))
          nil))))

(defn wikidata-process!
  "Write sources/wikidata/central_admin.csv
  (type,label,website,hostname,level), one query per class of
  wikidata-classes, from this fetch only. Leave the file untouched when
  a class query fails. Skip an existing file unless FORCE=1; need the
  subdivisions file. :ok, :skip or :fail."
  [country-qid country-dir]
  (let [out (country-src country-dir "wikidata" "central_admin.csv")
        sub (subdivisions-file country-dir)]
    (cond
      (skip? out)
      (do (println (str "=== " country-dir " (" country-qid ") : SKIP (use FORCE=1 to refetch)"))
          :skip)

      (not (fs/exists? sub))
      (do (err "ERR: " country-dir ": " sub " missing. Run 'bb pipeline subdivisions' first")
          :fail)

      :else
      (let [_         (println (str "=== " country-dir " (" country-qid ") ==="))
            level1    (into #{} (map first (rest (read-csv-raw sub))))
            per-class (mapv #(wikidata-class-rows! % country-qid level1) wikidata-classes)]
        (if (some nil? per-class)
          (do (err "ERR: " country-dir ": a class query failed; " out " left untouched")
              :fail)
          (let [rows (->> (apply concat per-class) distinct (sort-by #(nth % 3)))]
            (write-csv-file out ["type" "label" "website" "hostname" "level"] rows)
            (println (str "  -> " out " (" (count rows) " entries)"))
            :ok))))))

(defn cmd-wikidata
  "wikidata-process! over the countries of args (qid-pairs)."
  [args]
  (run-per-qid args wikidata-process!))

;; ===========================================================================
;;  iana -- ccTLD and its manager
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

(def iana-header ["country_dir" "cctld" "manager"])

(defn iana-process! [country-dir]
  (let [portal (iana-portal-for country-dir)]
    (if (str/blank? portal)
      (err "  [" country-dir "] no portal in data/world-governments.csv")
      (let [cctld (-> portal (str/split #"\.") last str/lower-case)]
        (cond
          (or (str/blank? cctld) (< (count cctld) 2))
          (err "  [" country-dir "] invalid cctld derived from '" portal "'")

          (table-row-done? "iana" country-dir)
          (println (str "=== " country-dir " (." cctld ") : SKIP"))

          :else
          (do
            (println (str "=== " country-dir " (." cctld ") ==="))
            (if-let [html (iana-fetch-html cctld)]
              (let [manager (or (iana-extract-field html "ccTLD Manager")
                                (iana-extract-field html "Sponsoring Organisation")
                                "")]
                (upsert-table-row! "iana" iana-header country-dir
                                   {"cctld" (str "." cctld) "manager" manager})
                (println (str "  -> " (source-table "iana") " (manager: " (or (not-empty manager) "?") ")"))
                (Thread/sleep 1000))
              (err "  failed after 3 attempts for ." cctld))))))))

(defn cmd-iana [args] (iter-countries iana-process! args conc-iana))

;; ===========================================================================
;;  cia -- Government section of the CIA World Factbook
;; ===========================================================================

(def factbook-map-file "data/factbook_gec.csv")
(def factbook-tree-cache "/tmp/world-gov-factbook-tree.json")

(defn cmd-build-gec
  "Write data/factbook_gec.csv (country_dir,gec,region) from the file
  tree and SUMMARY.md of factbook.json, corrected by
  data/factbook_aliases.csv."
  [_]
  (build-country-map!
   factbook-map-file ["country_dir" "gec" "region"] "data/factbook_aliases.csv" "Factbook GEC"
   (fn []
     (when (or (not (fs/exists? factbook-tree-cache)) force?)
       (when-let [body (http-get "https://api.github.com/repos/factbook/factbook.json/git/trees/master?recursive=1"
                                 {:timeout 30 :accept "application/json"})]
         (spit factbook-tree-cache body)))
     (when-let [summary (when (fs/exists? factbook-tree-cache)
                          (http-get "https://raw.githubusercontent.com/factbook/factbook.json/master/SUMMARY.md"))]
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
             pairs (for [[_ gec name] (re-seq #"`([a-z]+)` ([^`\n]+)" summary)]
                     [gec name (normalize-name name)])
             slug->dir @slug->country-dir]
         (for [[gec _name norm] pairs
               :let [dir (get slug->dir norm)
                     region (get region-by-gec gec)]
               :when (and dir region)]
           [dir gec region]))))))

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
  "[column path-in-the-Government-section] of the Factbook fields kept."
  [["government_type"         ["Government type"]]
   ["capital"                 ["Capital" "name"]]
   ["chief_of_state"          ["Executive branch" "chief of state"]]
   ["head_of_government"      ["Executive branch" "head of government"]]
   ["judicial_highest_courts" ["Judicial branch" "highest court(s)"]]])

(def cia-header (into ["country_dir"] (map first cia-fields)))

(defn cia-process! [country-dir]
  (let [map-row (mapping-row factbook-map-file country-dir)]
    (if-not map-row
      (err "  [" country-dir "] no Factbook GEC mapping")
      (let [[_ gec region] map-row]
        (if (table-row-done? "cia_factbook" country-dir)
          (println (str "=== " country-dir " (" gec ") : SKIP"))
          (do
            (println (str "=== " country-dir " (" gec ", " region ") ==="))
            (let [url (str "https://raw.githubusercontent.com/factbook/factbook.json/master/"
                           region "/" gec ".json")
                  body (http-get url {:timeout 30 :accept "application/json"})]
              (if (str/blank? body)
                (err "  fetch failed")
                (let [gov (:Government (json/parse-string body true))]
                  (if (nil? gov)
                    (err "  no Government section in response")
                    (do (upsert-table-row! "cia_factbook" cia-header country-dir
                                           (into {} (for [[k path] cia-fields]
                                                      [k (cia-extract gov (map keyword path))])))
                        (println (str "  -> " (source-table "cia_factbook")))
                        (Thread/sleep 1000))))))))))))

(defn cmd-cia [args]
  (if (fs/exists? factbook-map-file)
    (iter-countries cia-process! args conc-cia)
    (do (err "ERR: " factbook-map-file " missing. Run 'bb pipeline build-gec' first") 1)))

;; ===========================================================================
;;  un-desa -- national portal and EGDI rank
;; ===========================================================================

(def un-desa-map-file "data/un_desa_ids.csv")

(defn cmd-build-un-ids
  "Write data/un_desa_ids.csv (country_dir,un_id,un_name) from the
  UN/DESA Data-Center page, corrected by data/un_desa_aliases.csv."
  [_]
  (build-country-map!
   un-desa-map-file ["country_dir" "un_id" "un_name"] "data/un_desa_aliases.csv" "UN/DESA id"
   (fn []
     (when-let [body (http-get-curl "https://publicadministration.un.org/egovkb/en-us/Data-Center"
                                    {:timeout 30})]
       (let [pairs (->> (re-seq #"/Data/Country-Information/id/(\d+)-([A-Za-z-]+)" body)
                        (map (fn [[_ id name]] [id name (normalize-name name)]))
                        distinct)
             slug->dir @slug->country-dir]
         (for [[id name norm] pairs
               :let [dir (get slug->dir norm)]
               :when dir]
           [dir id name]))))))

(def un-desa-header ["country_dir" "national_portal" "egdi_rank"])

(defn un-desa-process! [country-dir]
  (let [map-row (mapping-row un-desa-map-file country-dir)]
    (if-not map-row
      (err "  [" country-dir "] no UN/DESA id mapping")
      (let [[_ un-id un-name] map-row]
        (if (table-row-done? "un_desa" country-dir)
          (println (str "=== " country-dir " (UN id=" un-id ") : SKIP"))
          (do
            (println (str "=== " country-dir " (UN id=" un-id " " un-name ") ==="))
            (let [url (str "https://publicadministration.un.org/egovkb/en-us/Data/Country-Information/id/"
                           un-id "-" un-name)
                  html (http-get-curl url {:timeout 30})]
              (if (str/blank? html)
                (err "  fetch failed")
                (let [portal (second (re-find #"<a href=\"([^\"]+)\">National Portal</a>" html))
                      rank (re-find #"Rank \d+ of \d+" html)]
                  (upsert-table-row! "un_desa" un-desa-header country-dir
                                     {"national_portal" (or portal "") "egdi_rank" (or rank "")})
                  (println (str "  -> " (source-table "un_desa") " (portal: " (or portal "?")
                                ", " (or rank "no rank") ")"))
                  (Thread/sleep 1000))))))))))

(defn cmd-un-desa [args]
  (if (fs/exists? un-desa-map-file)
    (iter-countries un-desa-process! args conc-un-desa)
    (do (err "ERR: " un-desa-map-file " missing. Run 'bb pipeline build-un-ids' first") 1)))

;; ===========================================================================
;;  oecd -- membership flag
;; ===========================================================================

;; 38 members as of May 2026 (latest accession: Croatia 2025).
(def oecd-members
  {"AUS" 1971 "AUT" 1961 "BEL" 1961 "CAN" 1961 "CHL" 2010 "COL" 2020
   "CRI" 2021 "CZE" 1995 "DNK" 1961 "EST" 2010 "FIN" 1969 "FRA" 1961
   "DEU" 1961 "GRC" 1961 "HRV" 2025 "HUN" 1996 "ISL" 1961 "IRL" 1961
   "ISR" 2010 "ITA" 1962 "JPN" 1964 "KOR" 1996 "LVA" 2016 "LTU" 2018
   "LUX" 1961 "MEX" 1994 "NLD" 1961 "NZL" 1973 "NOR" 1961 "POL" 1996
   "PRT" 1961 "SVK" 2000 "SVN" 2010 "ESP" 1961 "SWE" 1961 "CHE" 1961
   "TUR" 1961 "GBR" 1961 "USA" 1961})

(def oecd-header ["country_dir" "oecd_member" "member_since"])

(defn oecd-process! [country-dir]
  (let [iso3 (first (str/split country-dir #"_"))
        since (get oecd-members iso3)]
    (cond
      (table-row-done? "oecd" country-dir)
      (println (str "=== " country-dir " : SKIP"))

      since
      (do (upsert-table-row! "oecd" oecd-header country-dir
                             {"oecd_member" "yes" "member_since" (str since)})
          (println (str "=== " country-dir " : OECD member (since " since ")")))

      :else
      (do (upsert-table-row! "oecd" oecd-header country-dir {"oecd_member" "no"})
          (println (str "=== " country-dir " : non-member"))))))

(defn cmd-oecd [args] (iter-countries oecd-process! args))

;; ===========================================================================
;;  meta -- country metadata (REST Countries + World Bank)
;; ===========================================================================

(defn meta-rest-countries
  "Fetch {:region :subregion :languages :currencies :population} of an
  ISO3 code from restcountries.com; nil on failure."
  [iso3]
  (let [body (http-get (str "https://restcountries.com/v3.1/alpha/" iso3
                            "?fields=region,subregion,languages,currencies,population")
                       {:timeout 30 :accept "application/json"})]
    (when body
      (try
        (let [d (json/parse-string body true)]
          {:region     (or (:region d) "")
           :subregion  (or (:subregion d) "")
           :languages  (->> (vals (:languages d)) (str/join "; "))
           :currencies (->> (:currencies d) keys (map name) (str/join "; "))
           :population (str (or (:population d) ""))})
        (catch Exception _ nil)))))

(defn meta-world-bank-gdp
  "Fetch the latest GDP per capita (current US$) of an ISO3 code from
  the World Bank API: [value year], nil on failure."
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

(def meta-header ["country_dir" "region" "subregion" "languages" "currencies"
                  "population" "gdp_per_capita" "gdp_year"])

(defn meta-process! [country-dir]
  (let [iso3 (first (str/split country-dir #"_"))]
    (if (table-row-done? "country_data" country-dir)
      (println (str "=== " country-dir " : SKIP"))
      (let [rc  (meta-rest-countries iso3)
            gdp (meta-world-bank-gdp iso3)
            [gdp-val gdp-year] gdp]
        ;; when BOTH fetches failed, do not write: an all-blank row would
        ;; satisfy table-row-done? on the next runs and freeze the failure.
        ;; A half-filled row (one source down) is written and kept: the
        ;; missing half is only refetched with FORCE=1
        (if (and (nil? rc) (nil? gdp))
          (err (str "=== " country-dir " : both metadata fetches failed;"
                    " not writing " (source-table "country_data")))
          (do
            (upsert-table-row! "country_data" meta-header country-dir
                               {"region"         (or (:region rc) "")
                                "subregion"      (or (:subregion rc) "")
                                "languages"      (or (:languages rc) "")
                                "currencies"     (or (:currencies rc) "")
                                "population"     (or (:population rc) "")
                                "gdp_per_capita" (if gdp-val (format "%.0f" (double gdp-val)) "")
                                "gdp_year"       (or gdp-year "")})
            (println (str "=== " country-dir " : " (or (:region rc) "?")
                          " / GDP " (if gdp-val (format "%.0f" (double gdp-val)) "?")
                          " (" (or gdp-year "?") ")"))))
        (Thread/sleep 300)))))

(defn cmd-meta [args] (iter-countries meta-process! args conc-meta))

;; ===========================================================================
;;  enrich = build-qid + build-gec + build-un-ids + subdivisions, then
;;  the six sources in parallel
;; ===========================================================================

(defn cmd-enrich
  "Run build-qid, build-gec, build-un-ids and subdivisions, then the
  six sources in parallel, their logs shown once all finish. 1 when a
  step failed, 0 otherwise."
  [args]
  (err "-> build-qid + build-gec + build-un-ids + subdivisions…")
  (let [built (conj (mapv #(% nil) [cmd-build-qid cmd-build-gec cmd-build-un-ids])
                    (cmd-subdivisions args))
        _     (err "-> wikidata + iana + cia + un-desa + oecd + meta (all in parallel)…")
        logs  (fs/create-temp-dir)
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
    (fs/delete-tree logs)
    (if (or (some #{1} built) (some #(= :fail (second %)) results)) 1 0)))

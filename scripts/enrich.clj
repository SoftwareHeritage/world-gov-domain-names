(ns enrich
  "Enrichment sources: Wikidata (subdivisions, central administration,
  academia), ROR and the sopc hosts under countries/<c>/sources/, and
  IANA, CIA Factbook and UN/DESA in
  data/sources/<source>.csv; build-qid, build-gec and build-un-ids map
  country_dir to the ids of three of them. pipeline.clj dispatches."
  (:require [common :refer :all]
            [babashka.fs :as fs]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; Per-source concurrency limits. Each thread fires HTTP requests against
;; the same endpoint; numbers are picked to stay well under typical rate
;; limits while still saturating bandwidth. Tunable via env vars.
(def conc-wikidata (env-int "CONC_WIKIDATA" 3))
(def conc-iana     (env-int "CONC_IANA"     4))
(def conc-cia      (env-int "CONC_CIA"      8))
(def conc-un-desa  (env-int "CONC_UN_DESA"  4))

;; ===========================================================================
;;  Mapping files -- data/<source>_<id>.csv, country_dir -> source id
;; ===========================================================================

(defn- build-country-map!
  "Write map-file (country_dir -> source id): the rows of aliases-file,
  which win, then those of (fetch-matches). Skipped once it maps 150+
  countries, unless FORCE=1. 1 when fetch-matches returns nil (nothing
  written), else 0."
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
       ;; no bindings (failed, unreadable or empty answer): nil, so that
       ;; build-country-map! leaves the file untouched
       (when-let [bindings (seq (-> (parse-json body true) :results :bindings))]
         (let [iso3->qid (->> bindings
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
   ;; Universities and research institutes: academia-classes below.

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
  (some-> (parse-json body true) :results :bindings))

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
        unknown (for [[qid c] pairs :when (str/blank? qid)] c)
        dirs    (set (country-dirs))
        ;; an explicit pair must still name a QID and a country directory
        invalid (for [[qid c] pairs
                      :when (and (not (str/blank? qid))
                                 (or (not (re-matches #"Q\d+" qid)) (not (dirs c))))]
                  (str qid ":" c))]
    (cond
      (empty? rows) (err "ERR: " qid-map-file " missing. Run 'bb pipeline build-qid' first")
      (seq unknown) (err "ERR: not in " qid-map-file ": " (str/join ", " unknown)
                         " (expected a country_dir or QID:country_dir)")
      (seq invalid) (err "ERR: not a QID:country_dir pair: " (str/join ", " invalid))
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
  wikidata-classes, untouched when a query fails. Skip an existing
  file unless FORCE=1; needs the subdivisions file. :ok, :skip or
  :fail."
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
;;  academia -- public universities and research institutes
;; ===========================================================================
;; The universities and research institutes of the central government
;; or of the first tier below it. Wikidata seldom says who runs them
;; (P137 operator, P749 parent, P127 owner, P1001 jurisdiction: 9 of the
;; 264 public universities of Germany, none in France), so academia-level
;; falls back on the class, the label and how the country organises its
;; universities (academia-tiers). An org it places at no level keeps a
;; blank level, for the curation to see it in proposed.csv; one its
;; authorities place below the first tier is left out.

(def academia-classes
  "[class-qid type kind extra]: extra, SPARQL patterns narrowing a broad
  class."
  [["Q875538"   "public_university"   :university]
   ["Q62078547" "public_university"   :university]
   ;; public educational institution of the United States: the public
   ;; universities Wikidata types nowhere else, but also 180 schools
   ;; (K-12) and 600 community colleges, mostly run by local districts
   ["Q23002039" "public_university"   :university
    (str "  ?org wdt:P31/wdt:P279* wd:Q38723 .\n"
         "  FILTER NOT EXISTS { ?org wdt:P31 wd:Q1336920 }\n")]
   ["Q1145118"  "national_university" :university]
   ["Q265662"   "national_university" :university]
   ["Q31855"    "research_institute"  :research]])

(def national-university-classes
  (for [[qid type] academia-classes :when (= type "national_university")] qid))

(def academia-excluded-classes
  ;; private and religious typings under the public subtrees
  ["Q902104"    ; private university
   "Q23002042"  ; private educational institution
   "Q23002054"  ; private not-for-profit educational institution
   "Q557206"    ; Catholic university
   "Q2120466"   ; pontifical university
   "Q14911880"  ; seminary
   "Q1322589"]) ; Roman College

(def academia-excluded-types
  ;; direct P31 only: the subclass closure of these times out
  ["Q4830453"   ; business
   "Q783794"    ; company
   "Q2467461"]) ; academic department (a research institute subclass)

(defn academia-query [class-qid country-qid & [extra]]
  (str "SELECT DISTINCT ?org ?orgLabel ?website ?juris ?auth ?authJuris WHERE {\n"
       "  ?org wdt:P31/wdt:P279* wd:" class-qid " ;\n"
       "       wdt:P17 wd:" country-qid " ;\n"
       "       wdt:P856 ?website .\n"
       extra
       "  FILTER NOT EXISTS { ?org wdt:P576 ?d }\n"
       (apply str
              (for [q academia-excluded-classes]
                (str "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:" q " }\n")))
       (apply str
              (for [q academia-excluded-types]
                (str "  FILTER NOT EXISTS { ?org wdt:P31 wd:" q " }\n")))
       ;; the territory an org answers to (P1001) and the organisations
       ;; running it (operator, parent, owner) with their own territory
       "  OPTIONAL { ?org wdt:P1001 ?juris . }\n"
       "  OPTIONAL { ?org wdt:P137|wdt:P749|wdt:P127 ?auth .\n"
       "             OPTIONAL { ?auth wdt:P1001 ?authJuris . } }\n"
       "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
       "}"))

(def academia-tiers
  "How a country runs its public universities, by ISO3, when not the
  unitary {:default \"central\"}: :default, the level of a university
  nothing else places (nil: left out); :subdiv? and :state?, central-1
  for a label naming a subdivision or the word state; :national?
  false, the word national says nothing (National University is
  private)."
  {"USA" {:default "central-1" :subdiv? true :state? true :national? false}
   "DEU" {:default "central-1" :subdiv? true}
   "CAN" {:default "central-1" :subdiv? true}
   "BEL" {:default "central-1" :subdiv? true}
   "ESP" {:default "central-1" :subdiv? true}
   "CHE" {:default "central-1" :subdiv? true}
   "BIH" {:default "central-1" :subdiv? true}
   ;; 116 universities answer to central ministries (教育部, 工信部,
   ;; 国家民委, 中科院…), the 1,000+ others to a province
   "CHN" {:default "central-1" :subdiv? true}
   "GBR" {:default "central"   :subdiv? true}
   ;; ~90 national universities (国立), ~100 public ones run by a
   ;; prefecture or a city (公立), 600+ private ones
   "JPN" {:default nil :subdiv? true}
   "AUS" {:default nil :subdiv? true :state? true}
   "IND" {:default nil :subdiv? true :state? true}
   "BRA" {:default nil :subdiv? true :state? true}
   "MEX" {:default nil :subdiv? true :state? true}
   "NGA" {:default nil :subdiv? true :state? true}
   "PAK" {:default nil :subdiv? true}
   "ARE" {:default nil :subdiv? true}})

(def central-label-pattern
  #"(?iu)\b(?:federal|fédérale?|bundes\p{L}*|central university)\b")

(def national-label-pattern
  #"(?iu)\b(?:nationale?|nacional|nazionale)\b")

(def subnational-label-pattern
  #"(?iu)\b(?:estadual|estatal|provincial|provinciale|regional|régionale?|cantonal|kantonale?|landes\p{L}*)\b")

(def federal-district-pattern
  ;; a federal district is a first-level subdivision, not the federation
  #"(?iu)\b(?:distrito federal|federal district|district fédéral)\b")

(defn academia-level
  "Level of a university or research institute, \"local\" below the first
  tier, nil when no signal places it. In order: its territories
  (:juris, the P1001 of the org and of its authorities), its
  authorities (:auths) when the country or a subdivision, the national
  university class, a central or national label, then for a university
  only the subnational labels and the country's default
  (academia-tiers). Pure."
  [{:keys [kind type label juris auths country-qid level1 level1-pattern tiers]}]
  (let [juris-lvl  (juris-level (set juris) country-qid level1)
        auth-level (if (= juris-lvl "")
                     (juris-level (set (filter #(or (= % country-qid) (contains? level1 %)) auths))
                                  country-qid level1)
                     juris-lvl)
        raw        (or label "")
        label      (str/replace raw federal-district-pattern "")
        university? (= kind :university)
        {:keys [default subdiv? state? national?] :or {national? true}} tiers]
    (cond
      ;; run by a city or a county: local, whatever the label says
      (#{"central" "central-1" "local"} auth-level) auth-level
      (= type "national_university")                "central"
      (re-find central-label-pattern label)         "central"
      (and national?
           (re-find national-label-pattern label))  "central"
      ;; the place a research institute's label names is where it
      ;; stands, not who runs it: the subnational signals below only
      ;; place universities
      (not university?)                             nil
      (not= raw label)                              "central-1"
      (re-find subnational-label-pattern label)     "central-1"
      (and state? (re-find #"(?iu)\bstate\b" label)) "central-1"
      (and subdiv? level1-pattern
           (re-find level1-pattern label))          "central-1"
      :else                                         default)))

(def level-rank
  "Sort key of a level in the academia and ror files: central first, a
  blank (unknown) level last, so that the dedup per host and label
  keeps the most precise row."
  {"central" 0 "central-1" 1})

(defn- dedup-host-label
  "The rows minus those repeating the [hostname label] pair of an
  earlier one (label at index 1, hostname at index 3: academia and ror
  rows alike). Sort by level-rank first so the best level stays."
  [rows]
  (second (reduce (fn [[seen acc] [_ label _ host :as row]]
                    (if (seen [host label])
                      [seen acc]
                      [(conj seen [host label]) (conj acc row)]))
                  [#{} []] rows)))

(defn- qid-of [v] (some-> v (str/replace #"^.*/" "")))

(defn academia-bindings->rows
  "[type label website hostname level] rows of the bindings of one
  academia class query, one per distinct host; level blank for an org
  academia-level places nowhere, the local ones dropped. ctx:
  :country-qid :level1 :level1-pattern :tiers. Pure."
  [[_ type kind] bindings ctx]
  (->> (group-by #(get-in % [:org :value]) bindings)
       (mapcat
         (fn [[_ bs]]
           (let [values (fn [& ks] (into #{} (for [b bs, k ks
                                                  :let [q (qid-of (get-in b [k :value]))]
                                                  :when q]
                                              q)))
                 juris (values :juris :authJuris)
                 auths (values :auth)
                 label (get-in (first bs) [:orgLabel :value] "")
                 level (academia-level (assoc ctx :kind kind :type type :label label
                                              :juris juris :auths auths))]
             (when (not= level "local")
               (for [b bs
                     :let [url  (get-in b [:website :value])
                           host (extract-host url)]
                     :when host]
                 [type label url host (or level "")])))))
       distinct))

(defn academia-process!
  "Write sources/wikidata/academia.csv
  (type,label,website,hostname,level): the country's universities and
  research institutes, level central, central-1 or blank (unknown),
  the local ones left out, one query per class of academia-classes,
  untouched when a query fails. Skip an existing file unless FORCE=1;
  needs the subdivisions file. :ok, :skip or :fail."
  [country-qid country-dir]
  (let [out (country-src country-dir "wikidata" "academia.csv")
        sub (subdivisions-file country-dir)]
    (cond
      (skip? out)
      (do (println (str "=== " country-dir " (" country-qid ") academia : SKIP (use FORCE=1 to refetch)"))
          :skip)

      (not (fs/exists? sub))
      (do (err "ERR: " country-dir ": " sub " missing. Run 'bb pipeline subdivisions' first")
          :fail)

      :else
      (let [_    (println (str "=== " country-dir " (" country-qid ") academia ==="))
            subs (rest (read-csv-raw sub))
            ctx  {:country-qid    country-qid
                  :level1         (into #{} (map first subs))
                  :level1-pattern (level1-pattern (map second subs))
                  :tiers          (get academia-tiers (first (str/split country-dir #"_"))
                                       {:default "central"})}
            per-class
            (mapv (fn [[qid type :as cls]]
                    (if-let [bindings (wikidata-bindings
                                        (wikidata-run-query (academia-query qid country-qid (nth cls 3 nil))))]
                      (do (err (str "  [" type " " qid "] " (count bindings) " results"))
                          (Thread/sleep 1000)
                          (academia-bindings->rows cls bindings ctx))
                      (do (err (str "  [" type " " qid "] failed or truncated"))
                          nil)))
                  academia-classes)]
        (if (some nil? per-class)
          (do (err "ERR: " country-dir ": an academia query failed; " out " left untouched")
              :fail)
          ;; one row per org and host: an org typed under two classes or
          ;; with two websites on one host would count twice in propose;
          ;; central wins over central-1, a blank level loses (level-rank)
          (let [rows (->> (apply concat per-class)
                          (sort-by (juxt #(nth % 3) #(nth % 1) #(get level-rank (nth % 4) 2)))
                          dedup-host-label)]
            (write-csv-file out ["type" "label" "website" "hostname" "level"] rows)
            (println (str "  -> " out " (" (count rows) " entries)"))
            :ok))))))

(defn cmd-academia
  "academia-process! over the countries of args (qid-pairs)."
  [args]
  (run-per-qid args academia-process!))

;; ===========================================================================
;;  ror -- universities per the Research Organization Registry
;; ===========================================================================
;; ror.org (CC0) lists the world's research organisations with a
;; website, types and a country. Its education type takes in schools,
;; school boards and académies, and ROR never says who runs an
;; organisation nor whether it is public: the rows are candidates for
;; propose (source ror, +2), never a registry. Kept: the organisations
;; a name of which calls a university or an institute of higher
;; education (ror-university-pattern; colleges and academies are too
;; broad). Wikidata, through the QID ROR gives most of them, tells the
;; private ones, left out (private classes; 私立, 民办 or 사립 in the
;; description: classes alone miss most private universities of Japan),
;; the national ones (class, or 国立/國立/국립 in the description) and
;; the authorities and territories; academia-level then levels them, a
;; local one left out, a blank level when no signal places it.

(def conc-ror (env-int "CONC_ROR" 2))

(def ror-api-url "https://api.ror.org/v2/organizations")

(def ror-university-pattern
  #"(?iu)universit|univerz|uniwersytet|universidad|yliopisto|háskól|egyetem|πανεπιστήμ|университет|університет|üniversite|大学|대학|جامعة|دانشگاه|विश्वविद्यालय|polytech|politecn|politécn|politeknik|hochschule|institute of technology|instituto tecnol|école (?:nationale|normale|centrale|polytechnique|des mines|des ponts|supérieure)")

(def iso3->iso2
  "ISO3 -> ISO2 of data/world-governments.csv, plus Kosovo's user-assigned XK."
  (delay (into {"XKS" "XK"}
               (for [row (read-csv-file "data/world-governments.csv")
                     :let [iso3 (get row "iso3") iso2 (get row "iso2")]
                     :when (not (or (str/blank? iso3) (str/blank? iso2)))]
                 [iso3 iso2]))))

(defn ror-country-code [country-dir]
  (get @iso3->iso2 (first (str/split country-dir #"_"))))

(def ror-id-alphabet
  "The characters a ROR id draws on after its leading 0 (Crockford's
  base32: no i, l, o, u)."
  "0123456789abcdefghjkmnpqrstvwxyz")

(defn ror-slice
  "The active education organisations of a country whose ROR id starts
  with prefix, parsed: {:items [...] :number_of_results n}, items capped
  at one page of 20; nil on failure or on an unreadable answer."
  [iso2 prefix]
  (parse-json (http-get ror-api-url
                        {:timeout 60
                         :query-params {"filter" (str "types:education,status:active,"
                                                      "locations.geonames_details.country_code:" iso2)
                                        ;; the slash needs escaping in the Lucene syntax
                                        "query.advanced" (str "id:*ror.org\\/" prefix "*")}})
              true))

(defn ror-fetch-all
  "Every active education organisation of a country, nil when a request
  fails or a slice comes back short. The API pages in no stable order
  (a quarter of Iraq's orgs lost in 10/2026): the ids are split by
  prefix until each slice fits in one page."
  ([iso2] (ror-fetch-all iso2 "0"))
  ([iso2 prefix]
   (when-let [{:keys [items number_of_results]} (ror-slice iso2 prefix)]
     (Thread/sleep 250)
     (let [n (or number_of_results 0)]
       (if (<= n 20)
         (vec items)
         (let [orgs (reduce (fn [acc c]
                              (if-let [more (ror-fetch-all iso2 (str prefix c))]
                                (into acc more)
                                (reduced nil)))
                            [] ror-id-alphabet)]
           (if (= n (count orgs))
             orgs
             (do (err "ERR: ROR " iso2 ": " n " orgs under id prefix " prefix
                      ", " (count orgs) " fetched")
                 nil))))))))

(defn- ror-org-wikidata
  "The Wikidata QID of a ROR organisation, nil when it has none."
  [{:keys [external_ids]}]
  (some #(when (= "wikidata" (:type %)) (first (:all %))) external_ids))

(def private-description-pattern #"私立|民办|民辦|사립")
(def national-description-pattern #"国立|國立|국립")

(defn ror-wikidata-query
  "What Wikidata says of the qids: private (a private class or a
  business), national (a national university class), the descriptions
  in Chinese, Japanese and Korean, the territories and authorities."
  [qids]
  (let [values #(str/join " " (map (fn [q] (str "wd:" q)) %))]
    (str "SELECT ?org ?private ?national ?desc ?juris ?auth ?authJuris WHERE {\n"
         "  VALUES ?org { " (values qids) " }\n"
         "  BIND((EXISTS { ?org wdt:P31/wdt:P279* ?p . VALUES ?p { " (values academia-excluded-classes) " } }\n"
         "        || EXISTS { ?org wdt:P31 ?b . VALUES ?b { wd:Q4830453 wd:Q783794 } }) AS ?private)\n"
         "  BIND(EXISTS { ?org wdt:P31/wdt:P279* ?n . VALUES ?n { " (values national-university-classes) " } } AS ?national)\n"
         "  OPTIONAL { ?org schema:description ?desc . FILTER(lang(?desc) IN (\"ja\", \"zh\", \"zh-hant\", \"ko\")) }\n"
         "  OPTIONAL { ?org wdt:P1001 ?juris . }\n"
         "  OPTIONAL { ?org wdt:P137|wdt:P749|wdt:P127 ?auth .\n"
         "             OPTIONAL { ?auth wdt:P1001 ?authJuris . } }\n"
         "}")))

(defn ror-wikidata-facts
  "{qid {:private? :national? :juris :auths}} of qids, 100 per query;
  nil when a query failed."
  [qids]
  (reduce (fn [acc batch]
            (if-let [bindings (wikidata-bindings (wikidata-run-query (ror-wikidata-query batch)))]
              (do (Thread/sleep 1000)
                  (into acc
                        (for [[org bs] (group-by #(qid-of (get-in % [:org :value])) bindings)
                              :let [values (fn [k] (into #{} (keep #(qid-of (get-in % [k :value])) bs)))
                                    flag?  (fn [k] (some #(= "true" (get-in % [k :value])) bs))
                                    descs  (keep #(get-in % [:desc :value]) bs)
                                    desc?  (fn [re] (some #(re-find re %) descs))]]
                          [org {:private?  (boolean (or (flag? :private) (desc? private-description-pattern)))
                                :national? (boolean (or (flag? :national) (desc? national-description-pattern)))
                                :juris     (into (values :juris) (values :authJuris))
                                :auths     (values :auth)}])))
              (reduced nil)))
          {} (partition-all 100 (distinct qids))))

(defn ror-org->row
  "[ror label website hostname wikidata level] of a ROR organisation;
  nil without a university name or a website, or when the Wikidata
  facts of its QID say private or local. Level blank when
  academia-level places it nowhere. ctx: :country-qid :level1
  :level1-pattern :tiers. Pure."
  [ctx facts {:keys [id names links] :as org}]
  (let [all-names (keep :value names)
        label (or (some #(when (some #{"ror_display"} (:types %)) (:value %)) names)
                  (first all-names) "")
        url   (some #(when (= "website" (:type %)) (:value %)) links)
        host  (extract-host url)
        wd    (ror-org-wikidata org)
        {:keys [private? national? juris auths]} (get facts wd)
        university? (some #(re-find ror-university-pattern %) all-names)
        level (when university?
                (academia-level (assoc ctx :kind :university
                                       :type (if national? "national_university" "education")
                                       :label label :juris (or juris #{}) :auths (or auths #{}))))]
    (when (and host university? (valid-hostname? host)
               (not private?) (not= level "local"))
      [(str/replace id #"^https://ror\.org/" "") label url host (or wd "") (or level "")])))

(def country-qids
  "{country_dir wikidata_qid} of data/country_qid.csv."
  (delay (into {} (for [row (read-csv-file qid-map-file)]
                    [(get row "country_dir") (get row "wikidata_qid")]))))

(defn ror-process!
  "Write sources/ror/universities.csv
  (ror,label,website,hostname,wikidata,level): the country's public
  universities per ROR (ror-org->row), one row per org and host, the
  most precise level winning. Skip an existing file unless FORCE=1;
  needs the subdivisions file. :ok, :skip or :fail (nothing written)."
  [country-dir]
  (let [out  (country-src country-dir "ror" "universities.csv")
        sub  (subdivisions-file country-dir)
        iso2 (ror-country-code country-dir)]
    (cond
      (skip? out)
      (do (println (str "=== " country-dir " ror : SKIP (use FORCE=1 to refetch)"))
          :skip)

      (nil? iso2)
      (do (err "ERR: " country-dir ": no ISO2 code in data/world-governments.csv") :fail)

      (not (fs/exists? sub))
      (do (err "ERR: " country-dir ": " sub " missing. Run 'bb pipeline subdivisions' first")
          :fail)

      :else
      (let [_     (println (str "=== " country-dir " (" iso2 ") ror ==="))
            subs  (rest (read-csv-raw sub))
            ctx   {:country-qid    (get @country-qids country-dir)
                   :level1         (into #{} (map first subs))
                   :level1-pattern (level1-pattern (map second subs))
                   :tiers          (get academia-tiers (first (str/split country-dir #"_"))
                                        {:default "central"})}
            orgs  (ror-fetch-all iso2)
            facts (when orgs (ror-wikidata-facts (keep ror-org-wikidata orgs)))]
        (cond
          (nil? orgs)
          (do (err "ERR: " country-dir ": a ROR request failed; " out " left untouched") :fail)

          (nil? facts)
          (do (err "ERR: " country-dir ": a Wikidata query failed; " out " left untouched") :fail)

          :else
          (let [rows (->> orgs
                          (keep #(ror-org->row ctx facts %))
                          (sort-by (juxt #(nth % 3) #(get level-rank (nth % 5) 2) #(nth % 1)))
                          dedup-host-label)]
            (write-csv-file out ["ror" "label" "website" "hostname" "wikidata" "level"] rows)
            (println (str "  -> " out " (" (count rows) " universities of "
                          (count orgs) " education orgs)"))
            :ok))))))

(defn cmd-ror
  "ror-process! over the countries of args (all by default)."
  [args]
  (iter-countries ror-process! args conc-ror))

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
              (do (err "  failed after 3 attempts for ." cctld) :fail))))))))

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
                                       :let [[region file] (str/split p #"/")]
                                       ;; package.json sits at the root: no region
                                       :when file
                                       :let [gec (str/replace file #"\.json$" "")]]
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

(defn cia-process! [gec-of country-dir]
  (let [map-row (get gec-of country-dir)]
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
                (do (err "  fetch failed") :fail)
                (let [gov (:Government (parse-json body true))]
                  (if (nil? gov)
                    (do (err "  no Government section in response (or unreadable answer)") :fail)
                    (do (upsert-table-row! "cia_factbook" cia-header country-dir
                                           (into {} (for [[k path] cia-fields]
                                                      [k (cia-extract gov (map keyword path))])))
                        (println (str "  -> " (source-table "cia_factbook")))
                        (Thread/sleep 1000))))))))))))

(defn cmd-cia [args]
  (if (fs/exists? factbook-map-file)
    (iter-countries (partial cia-process! (mapping-rows factbook-map-file)) args conc-cia)
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
     (when-let [body (http-get-curl "https://publicadministration.un.org/egovkb/en-us/Data-Center")]
       (let [pairs (->> (re-seq #"/Data/Country-Information/id/(\d+)-([A-Za-z-]+)" body)
                        (map (fn [[_ id name]] [id name (normalize-name name)]))
                        distinct)
             slug->dir @slug->country-dir]
         (for [[id name norm] pairs
               :let [dir (get slug->dir norm)]
               :when dir]
           [dir id name]))))))

(def un-desa-header ["country_dir" "national_portal" "egdi_rank"])

(defn un-desa-process! [un-id-of country-dir]
  (let [map-row (get un-id-of country-dir)]
    (if-not map-row
      (err "  [" country-dir "] no UN/DESA id mapping")
      (let [[_ un-id un-name] map-row]
        (if (table-row-done? "un_desa" country-dir)
          (println (str "=== " country-dir " (UN id=" un-id ") : SKIP"))
          (do
            (println (str "=== " country-dir " (UN id=" un-id " " un-name ") ==="))
            (let [url (str "https://publicadministration.un.org/egovkb/en-us/Data/Country-Information/id/"
                           un-id "-" un-name)
                  html (http-get-curl url)]
              (if (str/blank? html)
                (do (err "  fetch failed") :fail)
                (let [portal (second (re-find #"<a href=\"([^\"]+)\">National Portal</a>" html))
                      rank (re-find #"Rank \d+ of \d+" html)]
                  (upsert-table-row! "un_desa" un-desa-header country-dir
                                     {"national_portal" (or portal "") "egdi_rank" (or rank "")})
                  (println (str "  -> " (source-table "un_desa") " (portal: " (or portal "?")
                                ", " (or rank "no rank") ")"))
                  (Thread/sleep 1000))))))))))

(defn cmd-un-desa [args]
  (if (fs/exists? un-desa-map-file)
    (iter-countries (partial un-desa-process! (mapping-rows un-desa-map-file)) args conc-un-desa)
    (do (err "ERR: " un-desa-map-file " missing. Run 'bb pipeline build-un-ids' first") 1)))

;; ===========================================================================
;;  sopc -- hosts of the public-sector code sources of swh-sopc-data-sources
;; ===========================================================================

(def sopc-files ["data/sources_code.yaml" "data/sources_code_maybe.yaml"])

(def sopc-shared-hosts
  "Code platforms shared by everyone: their hosts name no public body."
  #{"github.com" "github.io" "githubusercontent.com" "gitlab.com" "gitlab.io"
    "bitbucket.org" "codeberg.org" "framagit.org" "gitee.com" "sourceforge.net"})

(defn sopc-source-hosts
  "[host evidence] of one source of a sources_code YAML: the hosts of its
  url, api and website (the organization's own, for a GitHub org), off
  the shared platforms. nil for a community-run source or an
  unverified affiliation. Pure."
  [section {:strs [name region url api website maintainer notes]}]
  (when-not (or maintainer (re-find #"(?i)unverified" (str notes)))
    (let [evidence (str name (when region (str " (" region ")"))
                        (if (= section "github_orgs") " -- GitHub org" " -- code catalog"))]
      (for [u (concat [url website] (if (sequential? api) api [api]))
            :let [h (extract-host (str u))]
            :when (and h (valid-hostname? h)
                       (not (host-covered? h sopc-shared-hosts)))]
        [h evidence]))))

(def sopc-hosts-by-iso2
  "{ISO2 [[host evidence] …]} of every source of sopc-files; nil when
  the checkout is missing."
  (delay
    (let [paths (map #(str (fs/path sopc-dir %)) sopc-files)]
      (when (every? fs/exists? paths)
        (apply merge-with into
               (for [path paths
                     [iso2 sections] (yaml/parse-string (slurp path) :keywords false)
                     [section sources] sections]
                 {(str iso2) (vec (mapcat #(sopc-source-hosts section %) sources))}))))))

(defn sopc-process!
  "Write countries/<c>/sources/sopc/hosts.csv (hostname,evidence): the
  hosts of the country's code sources, deleted when it has none."
  [country-dir]
  (let [out  (country-src country-dir "sopc" "hosts.csv")
        rows (->> (get @sopc-hosts-by-iso2 (ror-country-code country-dir))
                  index-first
                  (sort-by key))]
    (if (empty? rows)
      (fs/delete-if-exists out)
      (do (write-csv-file out ["hostname" "evidence"] rows)
          (println (str "=== " country-dir " -> " out " (" (count rows) " hosts)"))))))

(defn cmd-sopc
  "sopc-process! over the countries of args (all by default). 0 with a
  warning, every file left as is, without a swh-sopc-data-sources
  checkout at SOPC_DIR."
  [args]
  (if @sopc-hosts-by-iso2
    (iter-countries sopc-process! args)
    (do (err "WARN: no " (str/join ", " sopc-files) " under " sopc-dir
             " (set SOPC_DIR); sources/sopc/ left as is")
        0)))

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
        _     (err "-> wikidata + ror + sopc + iana + cia + un-desa (all in parallel)…")
        logs  (fs/create-temp-dir)
        spawn (fn [name f]
                (future
                  (try
                    (let [out-file (str logs "/" name ".log")]
                      (with-open [w (io/writer out-file)]
                        ;; a source returns 1 when a country failed
                        (if (= 1 (binding [*out* w] (f args)))
                          [name :fail "a country failed (see its log below)"]
                          [name :ok nil])))
                    (catch Exception e
                      [name :fail (str (.getMessage e)
                                       " (" (.getName (class e)) ")")]))))
        ;; academia after wikidata, not beside it: both query the same
        ;; SPARQL endpoint, conc-wikidata at a time
        sources [["wikidata" #(max (cmd-wikidata %) (cmd-academia %))]
                 ["ror"      cmd-ror]
                 ["sopc"     cmd-sopc]
                 ["iana"     cmd-iana]
                 ["cia"      cmd-cia]
                 ["un_desa"  cmd-un-desa]]
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

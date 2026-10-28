#!/usr/bin/env bb
;; detect-from-directories -- harvest the official directories of public
;; bodies (Babashka).
;;
;; Fetches the machine-readable per-country directories of public bodies
;; listed in data/sources_domains.yaml of swh-sopc-data-sources (SOPC_DIR):
;; an entry carrying a 'harvest:' spec, or a list of them, is fetched
;; from its 'api:' or 'data:' URL, which must be a single one. Each spec
;; belongs to one channel, and each channel is one command writing one
;; kind of file:
;;   registry    countries/<c>/sources/<registry>/registered.csv --
;;               authoritative central scoping (organisation-form filter,
;;               federal-only export): the domains are confirmed
;;               directly. host_filter guards against off-TLD entries (a
;;               stray sites.google.com must never become a confirmed
;;               root).
;;   candidates  countries/<c>/sources/directory/orgs.csv -- mixed levels
;;               or types without a reliable marker: the hosts feed
;;               proposed.csv with a strong score bonus and the
;;               'directory' source tag; curation decides
;; A country has at most one spec per channel (both would write the same
;; file). It lives apart only to keep pipeline.clj lean; pipeline.clj
;; consumes its outputs.
;;
;; The keys of a spec, snake_case in the YAML; 'format' picks the
;; fetcher and the keys it reads:
;;   csv         name_col website_col, optional encoding (default
;;               UTF-8), separator (default ","), col_filters
;;   xlsx        name_col website_col, optional col_filters (first
;;               sheet, header on the first row)
;;   json        items_path ([] for a top-level array) name_field
;;               website_field, optional item_filters, query_params and
;;               website_json_key (the website field is a JSON-encoded
;;               list of objects, the website under that key)
;;   json_pages  the same, a 'page' query parameter added (from 0 until
;;               an empty page)
;;   xml         org_tag name_tag url_tag, optional type_tag type_filter
;;   govuk       content_url (gov.uk organisations API)
;;   ptv         detail_url type_filter (Suomi.fi PTV API)
;; Every format takes the optional host_filter, exclude_name and
;; registrable (true: hosts cut to their registrable domain). Regexes
;; are strings, compiled at load: the filters (col_filters and
;; item_filters as [column-or-field, regex] pairs, type_filter,
;; host_filter, exclude_name) match with re-find.
;;
;; Usage: bb scripts/detect-from-directories.clj registry|candidates [C…]
;;        bb scripts/detect-from-directories.clj harvest   (both, every spec)
;;
;; Environment variables: SOPC_DIR (default ../swh-sopc-data-sources).

(ns detect-from-directories
  (:require [common :refer :all]
            [babashka.fs :as fs]
            [clj-yaml.core :as yaml]
            [clojure.data.csv :as csv]
            [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Specs (sources_domains.yaml of swh-sopc-data-sources)
;; ---------------------------------------------------------------------------

(def sources-yaml
  (str (fs/path sopc-dir "data" "sources_domains.yaml")))

(defn- kebab
  "The keyword of a snake_case YAML string: name_col -> :name-col. Pure."
  [s]
  (keyword (str/replace (str (cond-> s (keyword? s) name)) "_" "-")))

(defn- snake
  "The snake_case YAML name of a kebab keyword: :name-col -> name_col. Pure."
  [k]
  (str/replace (name k) "-" "_"))

(def ^:private format-keys
  "{format {:required [key …] :optional [key …]}}: the keys each fetcher
  reads, besides common-keys."
  (let [json {:required [:items-path :name-field :website-field]
              :optional [:item-filters :query-params :website-json-key]}]
    {:csv        {:required [:name-col :website-col]
                  :optional [:encoding :separator :col-filters]}
     :xlsx       {:required [:name-col :website-col] :optional [:col-filters]}
     :json       json
     :json-pages json
     :xml        {:required [:org-tag :name-tag :url-tag] :optional [:type-tag :type-filter]}
     :govuk      {:required [:content-url]}
     :ptv        {:required [:detail-url :type-filter]}}))

(def ^:private common-keys
  #{:channel :registry :format :host-filter :exclude-name :registrable})

(defn- regex-problem
  "Why the string s does not compile as a regex, nil when it does. Pure."
  [s]
  (try (re-pattern s) nil
       (catch Exception e (first (str/split-lines (str (.getMessage e)))))))

(defn- shape-problem
  "Why the value v of the spec key k has not the shape the fetchers
  read, nil when it has: a list of strings, a map, a list of [string
  string] pairs, a boolean, else a string. Pure."
  [k v]
  (case k
    :items-path   (when-not (and (sequential? v) (every? string? v))
                    "must be a list of strings")
    :query-params (when-not (map? v) "must be a map")
    (:col-filters :item-filters)
    (when-not (and (sequential? v)
                   (every? #(and (sequential? %) (= 2 (count %)) (every? string? %)) v))
      "must be a list of [column-or-field, regex] pairs")
    :registrable  (when-not (boolean? v) "must be true or false")
    (when-not (string? v) "must be a string")))

(defn- spec-problems
  "The problems of a harvest spec as read from sources-yaml, its keys
  kebab keywords: a channel, format or key unknown, a required key
  missing, a value of the wrong shape, a regex that does not compile.
  Empty when it is valid. Pure."
  [spec]
  (let [channel (kebab (:channel spec))
        format  (kebab (:format spec))
        {:keys [required optional]} (format-keys format)
        known   (into common-keys (concat required optional))
        shapes  (into {} (for [[k v] spec
                               :when (known k)
                               :let [p (shape-problem k v)]
                               :when p]
                           [k p]))]
    (concat
     (when-not (#{:registry :candidates} channel)
       [(str "channel '" (:channel spec) "' is neither registry nor candidates")])
     (when (and (= :registry channel) (empty? (str (:registry spec))))
       ["a registry spec needs a registry"])
     (if-not (format-keys format)
       [(str "format '" (:format spec) "' is none of "
             (str/join ", " (map snake (keys format-keys))))]
       (concat
        (for [k required :when (nil? (get spec k))]
          (str "format " (snake format) " needs " (snake k)))
        (for [k (keys spec) :when (not (known k))]
          (str "unknown key " (snake k) " for format " (snake format)))))
     (for [[k p] (sort-by key shapes)] (str (snake k) " " p))
     ;; the regexes, of the keys whose shape is right
     (for [[k re] (concat (for [k [:host-filter :exclude-name :type-filter]
                                :when (and (get spec k) (not (shapes k)))]
                            [k (get spec k)])
                          (for [k [:col-filters :item-filters]
                                :when (and (get spec k) (not (shapes k)))
                                [_ re] (get spec k)]
                            [k re]))
           :let [why (regex-problem re)]
           :when why]
       (str (snake k) ": " why)))))

(defn- compile-spec
  "A harvest spec, valid per spec-problems and its keys kebab keywords,
  made ready for the fetchers: the channel and format kebab keywords,
  the JSON fields keywords (parse-json keywordizes), the regex strings
  compiled, the separator string turned into a char, the query
  parameters strings, :url the fetched URL and :source its host. Pure."
  [spec url]
  (let [pairs (fn [f ps] (mapv (fn [[k re]] [(f k) (re-pattern re)]) ps))]
    (cond-> (assoc spec :url url :source (extract-host url)
                   :channel (kebab (:channel spec)) :format (kebab (:format spec)))
      (:items-path spec)       (update :items-path #(mapv keyword %))
      (:name-field spec)       (update :name-field keyword)
      (:website-field spec)    (update :website-field keyword)
      (:website-json-key spec) (update :website-json-key keyword)
      (:query-params spec)     (update :query-params #(update-vals (update-keys % name) str))
      (:host-filter spec)      (update :host-filter re-pattern)
      (:exclude-name spec)     (update :exclude-name re-pattern)
      (:type-filter spec)      (update :type-filter re-pattern)
      (:col-filters spec)      (update :col-filters #(pairs str %))
      (:item-filters spec)     (update :item-filters #(pairs keyword %))
      (string? (:separator spec)) (update :separator first))))

(defn- iso2->country-dir
  "{ISO2 country_dir} of data/world-governments.csv and countries/."
  []
  (let [dirs (country-dirs)]
    (into {} (for [row (read-csv-file "data/world-governments.csv")
                   :let [iso3 (get row "iso3")
                         dir  (when (seq iso3)
                                (some #(when (str/starts-with? % (str iso3 "_")) %) dirs))]
                   :when dir]
               [(get row "iso2") dir]))))

(defn- entry-urls
  "The 'api:' and 'data:' URLs of a sources-yaml entry. Pure."
  [entry]
  (for [k ["api" "data"]
        u (let [v (get entry k)] (if (sequential? v) v [v]))
        :when (seq (str u))]
    u))

(def directory-specs
  "{:specs {country_dir [spec …]} :rejected [channel …]} of the
  'harvest:' specs of sources-yaml, each compiled and carrying its
  :iso2. A spec is reported and rejected when its entry has no single
  api/data URL, its country no country_dir, or spec-problems finds
  any; the specs of a country with two of one channel too (both would
  write the same file). :rejected holds the channel of each rejected
  spec, as read (an invalid one included). No specs, with an error,
  without the file."
  (delay
    (if-not (fs/exists? sources-yaml)
      (do (err "ERR: " sources-yaml " not found (set SOPC_DIR)")
          {:specs {} :rejected []})
      (let [by-iso2 (iso2->country-dir)
            checked (for [[iso2 sections] (yaml/parse-string (slurp sources-yaml) :keywords false)
                          [_ entries] sections
                          entry entries
                          :let [h (get entry "harvest")]
                          :when h
                          raw (if (sequential? h) h [h])
                          :let [spec (when (map? raw) (update-keys raw kebab))
                                dir  (by-iso2 iso2)
                                urls (entry-urls entry)
                                problems (concat
                                          (when-not (= 1 (count urls))
                                            [(str (count urls) " api/data URLs, a harvest spec needs one")])
                                          (when-not dir ["no country_dir"])
                                          (if spec
                                            (spec-problems spec)
                                            ["harvest must be a map, or a list of maps"]))]]
                      (if (seq problems)
                        (do (doseq [p problems]
                              (err "ERR: " sources-yaml ": " iso2 " '" (get entry "name") "': " p))
                            {:rejected (kebab (:channel spec))})
                        {:dir dir :spec (assoc (compile-spec spec (first urls)) :iso2 iso2)}))
            by-dir  (reduce (fn [m {:keys [dir spec]}] (update m dir (fnil conj []) spec))
                            (sorted-map) (filter :spec checked))
            doubled (for [[dir specs] by-dir
                          [channel n] (frequencies (map :channel specs))
                          :when (< 1 n)]
                      (do (err "ERR: " sources-yaml ": " dir " has " n " " (name channel) " specs")
                          [dir channel n]))
            drop?   (set (for [[dir channel] doubled] [dir channel]))]
        {:specs    (into (sorted-map)
                         (for [[dir specs] by-dir
                               :let [kept (vec (remove #(drop? [dir (:channel %)]) specs))]
                               :when (seq kept)]
                           [dir kept]))
         :rejected (into (vec (keep :rejected checked))
                         (mapcat (fn [[_ channel n]] (repeat n channel)) doubled))}))))

(defn- channel-specs
  "{country_dir spec} of the specs of channel."
  [channel]
  (into (sorted-map)
        (for [[dir specs] (:specs @directory-specs)
              spec specs
              :when (= channel (:channel spec))]
          [dir spec])))

;; ---------------------------------------------------------------------------
;; Fetchers, one per :format
;; ---------------------------------------------------------------------------

(defn- table->rows
  "[[name website] …] of a table (header row then data rows, strings),
  keeping the rows matching every [column regex] of :col-filters. nil
  on an empty table or a missing column (name, website or a filtered
  one: a renamed filter column must not let every row through)."
  [{:keys [name-col website-col col-filters]} [header & rows]]
  (let [;; a UTF-8 BOM would glue itself to the first header cell
        idx     (zipmap (map #(str/replace (str %) "\uFEFF" "") header) (range))
        name-i  (get idx name-col)
        web-i   (get idx website-col)
        filters (for [[col re] col-filters] [(get idx col) re])
        cell    (fn [r i] (str/trim (or (nth r i nil) "")))]
    (when (and header name-i web-i (every? first filters))
      (for [r rows
            :let [web (cell r web-i)]
            :when (and (seq web)
                       (every? (fn [[i re]] (re-find re (cell r i))) filters))]
        [(cell r name-i) web]))))

(defn- directory-fetch-csv
  "[[name website] …] of a CSV directory export, decoded per :encoding
  and filtered by table->rows. nil on a fetch failure."
  [{:keys [url encoding separator] :as spec}]
  (when-let [body (curl-get-bytes url)]
    (table->rows spec (csv/read-csv (String. ^bytes body (or encoding "UTF-8"))
                                    :separator (or separator \,)))))

(defn- xml-texts
  "The text contents of the descendants of el tagged tag-name
  (namespace ignored)."
  [el tag-name]
  (for [node (tree-seq :content :content el)
        :when (and (map? node) (= tag-name (name (:tag node))))
        s (:content node)
        :when (string? s)]
    s))

(defn- xml-children
  "The child elements of el tagged tag-name (namespace ignored)."
  [el tag-name]
  (filter #(and (map? %) (= tag-name (name (:tag %)))) (:content el)))

(defn- xlsx-rows
  "The rows of the first sheet of the .xlsx file at path, as vectors of
  strings placed by column letter (shared strings resolved, \"\" for the
  cells left out). nil without a first sheet."
  [path]
  (let [dir (fs/create-temp-dir)]
    (try
      (fs/unzip path dir)
      (let [part   (fn [& names]
                     (let [f (apply fs/file dir "xl" names)]
                       (when (fs/exists? f) (xml/parse-str (slurp f)))))
            shared (mapv #(apply str (xml-texts % "t"))
                         (xml-children (part "sharedStrings.xml") "si"))
            value  (fn [c]
                     (let [v (first (xml-texts c "v"))]
                       (case (get-in c [:attrs :t])
                         "s"         (get shared (some-> v parse-long) "")
                         "inlineStr" (apply str (xml-texts c "t"))
                         (or v ""))))
            ;; "AB12" -> 27
            column (fn [c]
                     (dec (reduce #(+ (* 26 %1) (- (int %2) 64)) 0
                                  (re-find #"^[A-Z]+" (get-in c [:attrs :r] "A")))))]
        (when-let [sheet (part "worksheets" "sheet1.xml")]
          (vec (for [row (tree-seq :content :content sheet)
                     :when (and (map? row) (= "row" (name (:tag row))))]
                 (reduce (fn [v c]
                           (let [i (column c)]
                             (assoc (into v (repeat (- (inc i) (count v)) "")) i (value c))))
                         [] (xml-children row "c"))))))
      (finally (fs/delete-tree dir)))))

(defn- directory-fetch-xlsx
  "[[name website] …] of an XLSX directory export (first sheet, header
  on the first row), filtered by table->rows. nil on a fetch failure."
  [{:keys [url] :as spec}]
  (let [tmp (fs/create-temp-file {:suffix ".xlsx"})]
    (try (when (curl-download! url tmp) (table->rows spec (xlsx-rows tmp)))
         (finally (fs/delete-if-exists tmp)))))

(defn- json-items->rows
  "[[name website] …] of JSON items, keeping those matching every
  [field regex] of :item-filters. With :website-json-key, the website
  field is a JSON-encoded list of objects: one row per object, its
  website under that key."
  [{:keys [name-field website-field website-json-key item-filters]} items]
  (for [it items
        :when (every? (fn [[k re]] (re-find re (str (get it k)))) item-filters)
        web (if website-json-key
              (map #(get % website-json-key)
                   (parse-json (get it website-field) true))
              [(get it website-field)])
        :when (seq (str web))]
    [(str (get it name-field)) (str web)]))

(defn- directory-fetch-json
  "[[name website] …] of a JSON document listing every organisation
  under :items-path ([] for a top-level array), fetched with the
  optional :query-params. nil on a fetch failure or an empty list."
  [{:keys [url items-path query-params] :as spec}]
  (let [data (parse-json (http-get url (cond-> {:accept "application/json" :timeout 120}
                                         query-params (assoc :query-params query-params)))
                         true)]
    (seq (json-items->rows spec (get-in data items-path)))))

(defn- directory-fetch-json-pages
  "[[name website] …] of a paginated JSON API (a 'page' query parameter
  added to :query-params), fetched until an empty page. nil when a page
  could not be fetched or read (a missing page must not pass for the
  last one)."
  [{:keys [url items-path query-params] :as spec}]
  (loop [page 0 acc []]
    (let [data  (parse-json (http-get url {:accept "application/json"
                                           :query-params (assoc query-params "page" (str page))})
                            true)
          items (get-in data items-path)]
      (cond
        (nil? data)    nil
        (empty? items) (seq acc)
        :else (recur (inc page) (into acc (json-items->rows spec items)))))))

(defn- directory-fetch-xml
  "[[name website] …] of an XML directory dump: the :org-tag elements
  with a :type-tag text matching :type-filter, their :name-tag and
  first :url-tag texts. nil on a fetch failure."
  [{:keys [url org-tag name-tag type-tag url-tag type-filter]}]
  (let [tmp (fs/create-temp-file)]
    (try
      (when (curl-download! url tmp)
        (with-open [r (io/reader (fs/file tmp))]
          (doall
           (for [org (tree-seq :content :content (xml/parse r))
                 :when (and (map? org) (= org-tag (name (:tag org))))
                 :let [types (xml-texts org type-tag)]
                 :when (or (nil? type-filter)
                           (some #(re-find type-filter %) types))
                 :let [org-name (first (xml-texts org name-tag))
                       web (first (xml-texts org url-tag))]
                 :when (seq (str web))]
             [(str org-name) web]))))
      (finally (fs/delete-if-exists tmp)))))

(defn- directory-fetch-govuk
  "[[name website] …] of the 'exempt' organisations of the gov.uk
  register (:url, paginated), each website read from its :content-url
  page; nil when a listing page fails or the listing is empty."
  [{:keys [url content-url]}]
  (let [exempt
        (loop [page 1 acc []]
          (let [d (parse-json (http-get url {:accept "application/json"
                                             :query-params {"page" (str page)}})
                              true)
                results (:results d)]
            (cond
              (nil? d)         nil
              (empty? results) (seq acc)
              :else
              (let [acc (into acc
                              (for [o results
                                    :when (= "exempt"
                                             (get-in o [:details :govuk_status]))]
                                [(str (:title o)
                                      (when (seq (str (:format o)))
                                        (str " (" (:format o) ")")))
                                 (get-in o [:details :slug])]))]
                (if (>= page (or (:pages d) page))
                  acc
                  (recur (inc page) acc))))))]
    (when exempt
      (err (str "  " (count exempt) " exempt organisations; fetching their"
                " external URLs (one call each)"))
      (doall
       (for [[title slug] exempt
             :let [url (get-in (parse-json (http-get (str content-url slug)
                                                     {:accept "application/json"})
                                           true)
                               [:details :organisation_govuk_status :url])
                   _ (Thread/sleep 200)]
             :when (seq (str url))]
         [title url])))))

(defn- directory-hosts
  "{host {:n mentions :names #{…}}} of [name website] rows, keeping the
  hosts matching :host-filter and dropping the names matching
  :exclude-name (both optional). With :registrable, each host is cut
  to its registrable domain first (engees.unistra.fr -> unistra.fr)."
  [{:keys [host-filter exclude-name registrable]} rows]
  (reduce (fn [m [org-name web]]
            (let [h (cond-> (extract-host web)
                      registrable (some-> parent-domain))]
              (if (and h (valid-hostname? h)
                       (or (nil? host-filter) (re-find host-filter h))
                       (or (nil? exclude-name)
                           (not (re-find exclude-name (str org-name)))))
                (-> m
                    (update-in [h :n] (fnil inc 0))
                    (update-in [h :names] (fnil conj (sorted-set))
                               (single-line org-name)))
                m)))
          {} rows))

(defn- directory-fetch-ptv
  "[[name website] …] of the Finnish Service Catalogue (PTV): the ids
  of the paginated :url, then the records by 100 from :detail-url,
  kept when their organizationType matches :type-filter (Finnish name,
  first web page); nil when a page fails."
  [{:keys [url detail-url type-filter]}]
  (let [ids (loop [page 1 acc []]
              (let [d (parse-json (http-get url {:accept "application/json"
                                                 :query-params {"page" (str page)}})
                                  true)]
                (cond
                  (nil? d) nil
                  :else (let [acc (into acc (map :id (:itemList d)))]
                          (if (>= page (or (:pageCount d) page))
                            (seq acc)
                            (recur (inc page) acc))))))]
    (when ids
      (err (str "  " (count ids) " organisations; fetching their records (100 per call)"))
      (let [batches (doall (for [chunk (partition-all 100 ids)
                                 :let [_ (Thread/sleep 200)]]
                             (parse-json (http-get (str detail-url (str/join "," chunk))
                                                   {:accept "application/json" :timeout 90})
                                         true)))]
        (when (every? some? batches)
          (for [org (apply concat batches)
                :when (re-find type-filter (str (:organizationType org)))
                :let [names (:organizationNames org)
                      web   (some-> org :webPages first :url)]
                :when (seq (str web))]
            [(str (or (some #(when (= "fi" (:language %)) (:value %)) names)
                      (:value (first names))))
             web]))))))

(defn- directory-fetch
  "Fetch the [name website] rows of a spec per its :format; nil on failure."
  [{:keys [format] :as spec}]
  (case format
    :csv        (directory-fetch-csv spec)
    :xlsx       (directory-fetch-xlsx spec)
    :json       (directory-fetch-json spec)
    :json-pages (directory-fetch-json-pages spec)
    :xml        (directory-fetch-xml spec)
    :govuk      (directory-fetch-govuk spec)
    :ptv        (directory-fetch-ptv spec)))

(defn- write-registry!
  "Write hosts as central domains of sources/<registry>/registered.csv.
  true (failed, nothing written) when hosts is empty, false otherwise."
  [country {:keys [registry source]} rows hosts]
  (if (empty? hosts)
    (do (err "ERR: the " country " directory (" source ") yields no domain;"
             " sources/" registry "/registered.csv left untouched")
        true)
    (let [out (registered-file country registry)]
      (write-registered! country registry (for [h (sort (keys hosts))] [h "central"]))
      (println (str country ": " (count hosts) " domains -> " out
                    " (" (count rows) " orgs listed)"))
      false)))

(defn- write-candidates!
  "Write hosts to sources/directory/orgs.csv (hostname,mentions,evidence).
  true (failed, nothing written) when hosts is empty, false otherwise:
  a directory that yields nothing is a broken fetch, not an empty
  country."
  [country {:keys [source]} rows hosts]
  (if (empty? hosts)
    (do (err "ERR: the " country " directory (" source ") yields no host;"
             " sources/directory/orgs.csv left untouched")
        true)
    (let [out (country-src country "directory" "orgs.csv")]
      (write-csv-file out ["hostname" "mentions" "evidence"]
                      (for [[h {:keys [n names]}] (sort-by key hosts)]
                        [h (str n) (truncate (str/join " | " names) 150)]))
      (println (str country ": " (count hosts) " hosts -> " out
                    " (" (count rows) " orgs listed)"))
      false)))

(defn- rejected-specs
  "The number of rejected specs of channel, those of no valid channel
  included: either may be one of them."
  [channel]
  (count (filter #(or (= channel %) (not (#{:registry :candidates} %)))
                 (:rejected @directory-specs))))

(defn- harvest-channel!
  "Fetch the directories of one channel (every spec of it, or the given
  country_dirs) and write their hosts with write!. 1 on a channel
  without specs, a country outside the channel (before any fetch), a
  fetch failure or a write! failure, and over every spec on a rejected
  spec of the channel (after the others are fetched), 0 otherwise."
  [channel write! args]
  (let [specs    (channel-specs channel)
        unknown  (remove specs args)
        rejected (if (empty? args) (rejected-specs channel) 0)]
    (cond
      (empty? specs)
      (do (err "ERR: no " (name channel) " directory spec in " sources-yaml) 1)

      (seq unknown)
      (do (err "ERR: no " (name channel) " directory spec for: " (str/join ", " unknown))
          1)

      :else
      (let [failed (for [[country {:keys [source] :as spec}] specs
                         :when (or (empty? args) (some #{country} args))
                         :let [rows  (directory-fetch spec)
                               hosts (when rows (directory-hosts spec rows))]
                         :when (if (nil? rows)
                                 (do (err "ERR: could not fetch the " country " directory (" source ")") true)
                                 (write! country spec rows hosts))]
                     country)]
        (when (pos? rejected)
          (err "ERR: " rejected " " (name channel) " spec(s) rejected, see above"))
        (if (or (seq (doall failed)) (pos? rejected)) 1 0)))))

(defn cmd-registry
  "Confirm the domains of the authoritative directories:
  sources/<registry>/registered.csv."
  [args] (harvest-channel! :registry write-registry! args))

(defn cmd-candidates
  "Propose the hosts of the mixed directories: sources/directory/orgs.csv."
  [args] (harvest-channel! :candidates write-candidates! args))

(defn cmd-harvest
  "registry then candidates over every spec; refuse a country argument."
  [args]
  (if (seq args)
    (do (err "ERR: harvest takes no country; use 'registry [C…]' or 'candidates [C…]'") 1)
    (if (some #{1} [(cmd-registry []) (cmd-candidates [])]) 1 0)))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(defn- specs-of [channel]
  (keys (channel-specs channel)))

(defn usage []
  (println "Usage: bb scripts/detect-from-directories.clj <command> [C…]")
  (println)
  (println "  registry [C…]    authoritative directories -> sources/<registry>/registered.csv (confirmed domains)")
  (println "  candidates [C…]  mixed directories -> sources/directory/orgs.csv (fed to proposed.csv)")
  (println "  harvest          registry + candidates, every spec")
  (println)
  (println (str "registry specs:   " (str/join " " (specs-of :registry))))
  (println (str "candidates specs: " (str/join " " (specs-of :candidates)))))

(def commands {"registry"   cmd-registry
               "candidates" cmd-candidates
               "harvest"    cmd-harvest})

;; Run only as a script (bb scripts/detect-from-directories.clj …), not
;; when load-file'd from a REPL: the hyphen in the file name keeps the
;; namespace out of require anyway.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

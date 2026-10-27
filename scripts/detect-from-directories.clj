#!/usr/bin/env bb
;; detect-from-directories -- harvest the official directories of public
;; bodies (Babashka).
;;
;; Fetches the machine-readable per-country directories described in
;; data/directories.edn (the specs: channel, format, columns, filters),
;; themselves listed in sources_domains.yaml of swh-sopc-data-sources.
;; Each spec belongs to one :channel, and each channel is one command
;; writing one kind of file:
;;   registry    countries/<c>/sources/<registry>/registered.csv --
;;               authoritative central scoping: the domains are
;;               confirmed directly
;;   candidates  countries/<c>/sources/directory/orgs.csv -- mixed levels
;;               or types: the hosts feed proposed.csv with a strong
;;               score bonus and the 'directory' source tag; curation
;;               decides
;; It lives apart only to keep pipeline.clj lean; pipeline.clj consumes
;; its outputs.
;;
;; Usage: bb scripts/detect-from-directories.clj registry|candidates [C…]
;;        bb scripts/detect-from-directories.clj harvest   (both, every spec)
;;        bb scripts/detect-from-directories.clj check [sources_domains.yaml]
;;
;; Environment variables: none.

(ns detect-from-directories
  (:require [common :refer :all]
            [babashka.fs :as fs]
            [clj-yaml.core :as yaml]
            [clojure.data.csv :as csv]
            [clojure.data.xml :as xml]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Specs (data/directories.edn)
;; ---------------------------------------------------------------------------

(def directories-file "data/directories.edn")

(def default-sources-yaml
  "sources_domains.yaml of a swh-sopc-data-sources checkout next to this
  repository."
  "../swh-sopc-data-sources/data/sources_domains.yaml")

(defn- compile-spec
  "A spec as read from directories-file, made ready for the fetchers:
  the regex strings compiled, the separator string turned into a char
  and :source set to the host of its URL. Pure."
  [spec]
  (let [pairs (fn [ps] (mapv (fn [[k re]] [k (re-pattern re)]) ps))]
    (cond-> (assoc spec :source (extract-host (or (:url spec) (:list-url spec))))
      (:host-filter spec)    (update :host-filter re-pattern)
      (:exclude-name spec)   (update :exclude-name re-pattern)
      (:type-filter spec)    (update :type-filter re-pattern)
      (:col-filters spec)    (update :col-filters pairs)
      (:item-filters spec)   (update :item-filters pairs)
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

(def directory-specs
  "{country_dir [spec …]} of directories-file (a key maps to one spec or
  a vector of them), each spec compiled and carrying its :iso2. An ISO2
  key without a country_dir is reported and left out."
  (delay
    (let [by-iso2 (iso2->country-dir)]
      (into (sorted-map)
            (for [[iso2 v] (edn/read-string (slurp directories-file))
                  :let [dir (by-iso2 iso2)]
                  :when (or dir (err "ERR: " directories-file ": no country_dir for " iso2))]
              [dir (mapv #(assoc (compile-spec %) :iso2 iso2)
                         (if (map? v) [v] v))])))))

(defn- channel-specs
  "{country_dir spec} of the specs of channel. A country with two specs
  of one channel is reported and left out: both would write the same
  file."
  [channel]
  (into (sorted-map)
        (for [[dir specs] @directory-specs
              :let [of-channel (filter #(= channel (:channel %)) specs)]
              :when (seq of-channel)
              :when (or (= 1 (count of-channel))
                        (err "ERR: " directories-file ": " dir " has "
                             (count of-channel) " " (name channel) " specs"))]
          [dir (first of-channel)])))

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
  "[[name website] …] of a paginated JSON API (:url ends in 'page='),
  fetched until an empty page. nil when a page could not be fetched or
  read (a missing page must not pass for the last one)."
  [{:keys [url items-path] :as spec}]
  (loop [page 0 acc []]
    (let [data  (parse-json (http-get (str url page) {:accept "application/json"}) true)
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
  register (:list-url, paginated), each website read from its
  :content-url page; nil when a listing page fails or the listing is
  empty."
  [{:keys [list-url content-url]}]
  (let [exempt
        (loop [page 1 acc []]
          (let [d (parse-json (http-get (str list-url page) {:accept "application/json"}) true)
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
  of the paginated :list-url, then the records by 100 from
  :detail-url, kept when their organizationType matches :type-filter
  (Finnish name, first web page); nil when a page fails."
  [{:keys [list-url detail-url type-filter]}]
  (let [ids (loop [page 1 acc []]
              (let [d (parse-json (http-get (str list-url page) {:accept "application/json"}) true)]
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

(defn- harvest-channel!
  "Fetch the directories of one channel (every spec of it, or the given
  country_dirs) and write their hosts with write!. 1 on a country
  outside the channel (before any fetch), a fetch failure or a write!
  failure, 0 otherwise."
  [channel write! args]
  (let [specs   (channel-specs channel)
        unknown (remove specs args)]
    (if (seq unknown)
      (do (err "ERR: no " (name channel) " directory spec for: " (str/join ", " unknown))
          1)
      (let [failed (for [[country {:keys [source] :as spec}] specs
                         :when (or (empty? args) (some #{country} args))
                         :let [rows  (directory-fetch spec)
                               hosts (when rows (directory-hosts spec rows))]
                         :when (if (nil? rows)
                                 (do (err "ERR: could not fetch the " country " directory (" source ")") true)
                                 (write! country spec rows hosts))]
                     country)]
        (if (seq (doall failed)) 1 0)))))

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

(defn- strip-query
  "url without its query string nor trailing slash. Pure."
  [url]
  (-> (str url) str/trim (str/replace #"\?.*$" "") (str/replace #"/+$" "")))

(defn- yaml-fetch-urls
  "The 'api:' and 'data:' URLs (each a string or a list) of every
  section of one country of sources_domains.yaml, query strings
  stripped; 'dataset:' names a portal page, never fetched. Pure."
  [country]
  (set (for [[_ sources] country
             src sources
             k ["api" "data"]
             u (let [v (get src k)] (if (sequential? v) v [v]))
             :when (seq (str u))]
         (strip-query u))))

(defn cmd-check
  "Check that the :url (or :list-url) of every spec is an 'api:' or
  'data:' entry of its country in sources_domains.yaml
  (default-sources-yaml, or the path given), query strings aside. 1 on a missing file or a mismatch,
  0 otherwise."
  [[path]]
  (let [path (or path default-sources-yaml)]
    (if-not (fs/exists? path)
      (do (err "ERR: " path " not found; pass the path of sources_domains.yaml") 1)
      (let [doc      (yaml/parse-string (slurp path) :keywords false)
            specs    (for [[dir specs] @directory-specs, spec specs] [dir spec])
            problems (for [[dir {:keys [iso2 url list-url]}] specs
                           :let [u    (strip-query (or url list-url))
                                 data (yaml-fetch-urls (get doc iso2))]
                           :when (not (data u))]
                       (str dir " (" iso2 "): " u
                            (if (seq data)
                              (str " is not among " (str/join ", " (sort data)))
                              " has no 'api:' nor 'data:' entry")))]
        (doseq [p problems] (println (str "  MISMATCH " p)))
        (if (seq problems)
          (do (err "ERR: " (count problems) " spec(s) out of step with " path) 1)
          (do (println (str "  " (count specs) " specs match " path)) 0))))))

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
  (println (str "  check [YAML]     every spec URL is an 'api:' or 'data:' entry of sources_domains.yaml (default "
                default-sources-yaml ")"))
  (println)
  (println (str "registry specs:   " (str/join " " (specs-of :registry))))
  (println (str "candidates specs: " (str/join " " (specs-of :candidates)))))

(def commands {"registry"   cmd-registry
               "candidates" cmd-candidates
               "harvest"    cmd-harvest
               "check"      cmd-check})

;; Run only as a script (bb scripts/detect-from-directories.clj …), not
;; when load-file'd from a REPL: the hyphen in the file name keeps the
;; namespace out of require anyway.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

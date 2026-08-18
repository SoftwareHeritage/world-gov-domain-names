#!/usr/bin/env bb
;; detect-from-directories -- harvest the official directories of public
;; bodies (Babashka).
;;
;; Fetches the machine-readable per-country directories listed in the
;; "Government website directories" section of swh-sopc-data-sources and
;; writes, per the spec's :channel, either
;; countries/<c>/sources/registry/roots.csv (authoritative central
;; scoping: domains enter data/public-sector-domains-central.csv
;; directly, like the CISA/Lannuaire registries) or
;; countries/<c>/sources/directory/orgs.csv (mixed levels or types:
;; hosts feed candidates.csv with a strong score bonus and the
;; 'directory' source tag; curation decides). Unlike detect-forges and
;; detect-universities this script SERVES the repository's core goal --
;; it lives apart only to keep pipeline.clj lean while the spec table
;; grows; pipeline.clj consumes its outputs.
;;
;; Usage: bb scripts/detect-from-directories.clj harvest [C…]
;;
;; Environment variables: none (specs are self-contained).

(ns detect-from-directories
  (:require [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Helpers (small copies of pipeline.clj's -- the scripts stay independent)
;; ---------------------------------------------------------------------------

(def ua "world-gov-domain-names/0.1 (https://github.com/bzg)")

(defn err [& xs] (binding [*out* *err*] (println (apply str xs))))

(defn single-line [s] (-> (str s) (str/replace #"\s+" " ") str/trim))

(defn truncate [s n]
  (if (> (count s) n) (str (subs s 0 (- n 3)) "...") s))

(defn write-csv-file [path header rows]
  (when-let [parent (fs/parent path)]
    (fs/create-dirs parent))
  (with-open [w (io/writer (str path))]
    (csv/write-csv w (cons header rows))))

(defn country-src [country-dir source & [file]]
  (str "countries/" country-dir "/sources/" source (when file (str "/" file))))

(defn extract-host [url]
  (when (and url (not (str/blank? url)))
    (-> url
        str/lower-case
        (str/replace #"^https?://" "")
        (str/replace #"^www\." "")
        (str/replace #"/.*$" "")
        (str/replace #":.*$" ""))))

(defn valid-hostname? [h]
  (boolean
    (and h (re-matches #"[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+" h))))

(def http-client
  (http/client (assoc http/default-client-opts
                      :follow-redirects :never
                      :connect-timeout 15000)))

(defn http-get
  "GET returning the body string on HTTP 200, nil otherwise (no retry on
  deterministic 4xx)."
  ([url] (http-get url {}))
  ([url {:keys [timeout retries accept] :or {timeout 30 retries 3 accept "*/*"}}]
   (loop [attempt 1]
     (let [resp (try (http/get url
                               {:client http-client
                                :headers {"User-Agent" ua "Accept" accept}
                                :throw false
                                :timeout (* timeout 1000)})
                     (catch Exception _ nil))
           status (:status resp)]
       (cond
         (and resp (= 200 status) (not (str/blank? (:body resp))))
         (:body resp)

         (and status (<= 400 status 499) (not= 429 status))
         nil

         (< attempt retries)
         (do (Thread/sleep (* attempt 3000))
             (recur (inc attempt)))

         :else nil)))))

;; Declarative per-country specs for the machine-readable government
;; directories listed in swh-sopc-data-sources ("Government website
;; directories"). One generic fetcher per format family; each country is
;; a data entry, not code. :channel picks where the hosts land:
;;   :registry   -- the directory's central-government scoping is
;;                  authoritative (organisation-form filter, federal-only
;;                  export): sources/registry/roots.csv, entering the
;;                  central file directly like the CISA/Lannuaire
;;                  registries. :host-filter guards against off-TLD
;;                  entries (a stray sites.google.com must never become
;;                  a confirmed root).
;;   :candidates -- the directory mixes levels or types without a
;;                  reliable marker: sources/directory/orgs.csv, feeding
;;                  candidates.csv with a strong score bonus; curation
;;                  decides.
(def directory-specs
  {"DEU_germany"
   ;; Behördenwegweiser: federal bodies only, but no type column and a
   ;; sprinkling of federally-anchored foundations and associations ->
   ;; candidates, not registry.
   {:channel     :candidates
    :format      :csv
    :url         (str "https://www.service.bund.de/SharedDocs/CSV/"
                      "Anschriftenverzeichnis-CSV.csv?__blob=publicationFile")
    :encoding    "ISO-8859-1"
    :separator   \;
    :name-col    "Organisation"
    :website-col "Internetadresse"
    :source      "service.bund.de"}

   "NOR_norway"
   ;; Enhetsregisteret: institutional sector code 6100 = central
   ;; government (statsforvaltningen) -- ministries, directorates,
   ;; courts, police districts. Authoritative central scoping ->
   ;; registry. (organisasjonsform STAT alone only carries the ~18
   ;; top-level organs.)
   {:channel       :registry
    :format        :json-pages
    :url           (str "https://data.brreg.no/enhetsregisteret/api/enheter"
                        "?institusjonellSektorkode=6100&size=500&page=")
    :items-path    [:_embedded :enheter]
    :name-field    :navn
    :website-field :hjemmeside
    :host-filter   #"\.no$"
    ;; academia is out of scope (see detect-universities.clj); the \b
    ;; keeps university hospitals (UNIVERSITETSSYKEHUS) in -- Norwegian
    ;; hospitals are central-state bodies.
    ;; (?iu): plain (?i) does not case-fold Ø/ø
    :exclude-name  #"(?iu)universitetet|universitetssenter|universitet\b|høgskole|høyskole|fagskole|allaskuvla"
    :source        "data.brreg.no"}

   "ITA_italy"
   ;; IndicePA: 23k+ entities of every level; the category code isolates
   ;; the central ones -- C1 ministries, C2 constitutional organs, C5
   ;; independent authorities, C8 national research bodies (CNR, ISTAT,
   ;; ISS, ASI), C10 fiscal agencies, C11 interior-ministry departments,
   ;; C16 social-security institutes (~130 entities). The categories mix
   ;; a few parapublic bodies -> candidates, curation decides.
   {:channel     :candidates
    :format      :csv
    :url         (str "https://indicepa.gov.it/ipa-dati/datastore/dump/"
                      "d09adf99-dc10-4349-8c53-27b1e5aa97b6?format=csv")
    :name-col    "Denominazione_ente"
    :website-col "Sito_istituzionale"
    :col-filters [["Codice_Categoria" #"^(?:C1|C2|C5|C8|C10|C11|C16)$"]]
    :source      "indicepa.gov.it"}

   "POL_poland"
   ;; Katalog Podmiotów Publicznych: 87k entities of every level (mostly
   ;; schools and municipal bodies), reduced to the ACTIVE central types
   ;; -- ministries, central institutions, top courts, ministry units.
   ;; Deconcentrated field services (district inspectorates, local
   ;; prosecutors...) stay out: their HQs are in 'instytucje centralne'.
   ;; Snapshot resource URL: update it when dane.gov.pl publishes a new
   ;; export (dataset 3520). The file is UTF-8 with a few stray invalid
   ;; bytes -> replaced, cosmetic only.
   {:channel     :candidates
    :format      :csv
    :url         "https://api.dane.gov.pl/media/resources/20260529/export_gov.csv"
    :separator   \;
    :name-col    "Nazwa podmiotu"
    :website-col "Strona www"
    :col-filters [["Status" #"^ACTIVE$"]
                  ["Typ podmiotu"
                   #"^(?:ministerstwa|instytucje centralne|sądy administracyjne|sądy apelacyjne|jednostki organizacyjne (?:Ministerstwa|Kancelarii))"]]
    :source      "dane.gov.pl (dataset 3520)"}

   "CAN_canada"
   ;; Inventory of Federal Organizations and Interests: federal by
   ;; construction, but includes Crown corporations and shared-governance
   ;; interests without a clean central marker -> candidates.
   {:channel     :candidates
    :format      :csv
    :url         (str "https://open.canada.ca/data/dataset/"
                      "a35cf382-690c-4221-a971-cf0fd189a46f/resource/"
                      "7c131a87-7784-4208-8e5c-043451240d95/download/"
                      "ifoi_roif_en.csv")
    :name-col    "legal_title"
    :website-col "website"
    :source      "open.canada.ca"}

   "GBR_united_kingdom"
   ;; gov.uk organisations register: the domains under gov.uk are
   ;; already covered by the CDDO registry; the value is the ~300
   ;; 'exempt' arm's-length bodies running their own website off
   ;; gov.uk (ukri.org, aria.org.uk, acas.org.uk...). Their nature
   ;; varies (NDPBs, levy boards, NI bodies) -> candidates.
   {:channel     :candidates
    :format      :govuk
    :list-url    "https://www.gov.uk/api/organisations?page="
    :content-url "https://www.gov.uk/api/content/government/organisations/"
    :source      "gov.uk/api/organisations"}

   ;; No spec for ESP_spain (investigated 2026-08-18): DIR3 is an
   ;; e-invoicing unit-code registry without website fields, and both
   ;; administracionelectronica.gob.es and its file downloads sit behind
   ;; a WAF that rejects automated requests ("Request Rejected", 244-byte
   ;; bodies behind HTTP 200). The need is low anyway: the gob.es root
   ;; covers the ministries, and the wikidata+linkgraph channels already
   ;; surface the bare-.es agencies (csic.es, aemet.es, boe.es at score
   ;; 10). The PAG directory (administracion.gob.es) is HTML-only.

   "NLD_netherlands"
   ;; Register van Overheidsorganisaties: full XML dump with clean
   ;; organisation types. The central types below (~325 bodies) leave
   ;; out Gemeente/Provincie/Waterschap and the cooperation vehicles;
   ;; ZBO's and advisory colleges vary in nature -> candidates.
   {:channel     :candidates
    :format      :xml
    :url         "https://organisaties.overheid.nl/archive/exportOO.xml"
    :org-tag     "organisatie"
    :name-tag    "naam"
    :type-tag    "type"
    :url-tag     "url"
    :type-filter #"^(?:Ministerie|Agentschap|Inspectie|Zelfstandig bestuursorgaan|Adviescollege|Rechtspraak|Hoog College van Staat)$"
    :source      "organisaties.overheid.nl"}})

(def directory-http-client
  ;; unlike the no-redirect default client, directory exports often sit
  ;; behind a redirect to a storage host (open.canada.ca)
  (http/client (assoc http/default-client-opts
                      :follow-redirects :normal
                      :connect-timeout 15000)))

(defn- directory-curl-bytes
  "Download url with curl and return its bytes, nil on failure. The
  system CA store (unlike the JVM truststore) carries the national CA
  chains several government hosts sit behind."
  [url]
  (let [tmp (fs/create-temp-file)]
    (try
      (let [{:keys [exit]}
            (try (proc/sh "curl" "-sfL" "--max-time" "300" "-A" ua
                          "-o" (str tmp) url)
                 (catch Exception _ {:exit 1}))]
        (when (zero? (or exit 1))
          (fs/read-all-bytes tmp)))
      (finally (fs/delete-if-exists tmp)))))

(defn- directory-fetch-csv
  "[[name website] ...] from a CSV directory export. Fetched as bytes:
  these exports are often latin-1 (or UTF-8 with stray invalid bytes,
  which String replaces rather than rejects) and the default decoding
  would garble the organisation names. :col-filters, a vector of
  [column-name regex] pairs, keeps only the rows matching every filter
  (how IndicePA's 23k mixed-level entities reduce to the central
  categories, or Poland's catalog to ACTIVE central types).
  Downloaded with curl: several government hosts chain through national
  CAs the JVM truststore does not carry (dane.gov.pl)."
  [{:keys [url encoding separator name-col website-col col-filters]}]
  (let [body (directory-curl-bytes url)]
    (when body
      (let [rows (csv/read-csv (String. ^bytes body (or encoding "UTF-8"))
                               :separator (or separator \,))
            ;; a UTF-8 BOM would glue itself to the first header cell
            header (update (vec (first rows)) 0 #(str/replace % "﻿" ""))
            idx  (zipmap header (range))
            name-i (get idx name-col)
            web-i  (get idx website-col)
            filters (for [[col re] col-filters
                          :let [i (get idx col)]
                          :when i]
                      [i re])]
        (when (and name-i web-i)
          (for [r (rest rows)
                :let [web (str/trim (or (nth r web-i nil) ""))]
                :when (and (seq web)
                           (every? (fn [[i re]]
                                     (re-find re (str/trim (or (nth r i nil) ""))))
                                   filters))]
            [(str/trim (or (nth r name-i nil) "")) web]))))))

(defn- directory-fetch-json-pages
  "[[name website] ...] from a paginated JSON API: URL ends in 'page=',
  pages are fetched until one comes back empty."
  [{:keys [url items-path name-field website-field]}]
  (loop [page 0 acc []]
    (let [body (http-get (str url page) {:accept "application/json"})
          items (when body
                  (get-in (json/parse-string body true) items-path))]
      (if (empty? items)
        (seq acc)
        (recur (inc page)
               (into acc (for [it items
                               :let [web (str (get it website-field))]
                               :when (seq web)]
                           [(str (get it name-field)) web])))))))

(defn- xml-texts
  "All text contents of the descendants of el whose unqualified tag name
  is tag-name."
  [el tag-name]
  (for [node (tree-seq :content :content el)
        :when (and (map? node) (= tag-name (name (:tag node))))
        s (:content node)
        :when (string? s)]
    s))

(defn- directory-fetch-xml
  "[[name website] ...] from an XML directory dump: elements whose
  unqualified tag is :org-tag, keeping those with at least one :type-tag
  text matching :type-filter; the first :url-tag text is the website.
  Tag names are matched without their namespace (the Dutch ROO export
  qualifies everything). Downloaded with curl to a temp file: these
  dumps are tens of MB and some hosts chain through national CAs the
  JVM truststore does not carry (organisaties.overheid.nl)."
  [{:keys [url org-tag name-tag type-tag url-tag type-filter]}]
  (let [tmp (fs/create-temp-file)]
    (try
      (let [{:keys [exit]}
            (try (proc/sh "curl" "-sfL" "--max-time" "300" "-A" ua
                          "-o" (str tmp) url)
                 (catch Exception _ {:exit 1}))]
        (when (zero? (or exit 1))
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
               [(str org-name) web])))))
      (finally (fs/delete-if-exists tmp)))))

(defn- directory-fetch-govuk
  "[[name website] ...] for the gov.uk organisations register. Two-stage
  and gov.uk-specific: the paginated listing carries every organisation
  but only gov.uk paths as web_url; the organisations whose govuk_status
  is 'exempt' (arm's-length bodies running their own website) expose
  that external URL in the per-organisation Content API, one call each.
  Live/joining organisations sit under www.gov.uk and are already
  covered by the CDDO registry; closed ones are skipped."
  [{:keys [list-url content-url]}]
  (let [exempt
        (loop [page 1 acc []]
          (let [body (http-get (str list-url page) {:accept "application/json"})
                d (when body (json/parse-string body true))
                results (:results d)]
            (if (empty? results)
              acc
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
    (err (str "  " (count exempt) " exempt organisations; fetching their"
              " external URLs (one call each)"))
    (doall
     (for [[title slug] exempt
           :let [body (http-get (str content-url slug)
                                {:accept "application/json"})
                 url (when body
                       (get-in (json/parse-string body true)
                               [:details :organisation_govuk_status :url]))
                 _ (Thread/sleep 200)]
           :when (seq (str url))]
       [title url]))))

(defn- directory-hosts
  "{host {:n mentions :names #{...}}} from the spec's [name website] rows;
  hosts filtered by :host-filter, organisations dropped by :exclude-name
  (both optional)."
  [{:keys [host-filter exclude-name]} rows]
  (reduce (fn [m [org-name web]]
            (let [h (extract-host web)]
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

(defn cmd-directory
  "Harvest the official government directories of directory-specs (all of
  them, or the given country_dirs) and write, per the spec's :channel,
  either sources/registry/roots.csv (authoritative central scoping) or
  sources/directory/orgs.csv (candidates channel, curation decides)."
  [args]
  (doseq [[country {:keys [channel format source] :as spec}]
          (sort-by key directory-specs)
          :when (or (empty? args) (some #{country} args))]
    (let [rows (case format
                 :csv        (directory-fetch-csv spec)
                 :json-pages (directory-fetch-json-pages spec)
                 :xml        (directory-fetch-xml spec)
                 :govuk      (directory-fetch-govuk spec))]
      (if (nil? rows)
        (err "ERR: could not fetch the " country " directory (" source ")")
        (let [hosts (directory-hosts spec rows)]
          (case channel
            :registry
            (let [out (country-src country "registry" "roots.csv")]
              (write-csv-file out ["domain" "organization" "source"]
                              (for [[h {:keys [names]}] (sort-by key hosts)]
                                [h (str/join " | " names) source]))
              (println (str country ": " (count hosts) " domains -> " out
                            " (" (count rows) " orgs listed)")))
            :candidates
            (let [out (country-src country "directory" "orgs.csv")]
              (write-csv-file out ["hostname" "mentions" "evidence"]
                              (for [[h {:keys [n names]}] (sort-by key hosts)]
                                [h (str n)
                                 (truncate (str/join " | " names) 150)]))
              (println (str country ": " (count hosts) " hosts -> " out
                            " (" (count rows) " orgs listed)")))))))))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(defn usage []
  (println "Usage: bb scripts/detect-from-directories.clj harvest [C…]")
  (println)
  (println (str "Countries with a spec: "
                (str/join " " (sort (keys directory-specs))))))

(let [args *command-line-args*]
  (cond
    (or (empty? args) (#{"-h" "--help" "help"} (first args)))
    (do (usage) (when (empty? args) (System/exit 1)))

    (= "harvest" (first args))
    (cmd-directory (vec (rest args)))

    :else
    (do (err "ERR: unknown sub-command '" (first args) "'")
        (usage)
        (System/exit 1))))

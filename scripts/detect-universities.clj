#!/usr/bin/env bb
;; detect-universities -- strictly public universities (Babashka).
;;
;; Harvests the Wikidata classes "public university" (Q875538), "public
;; research university" (Q62078547) and "national university" (Q1145118)
;; into data/public-universities.csv. This is a PARKED dataset (decision
;; of 2026-08-13, extraction of 2026-08-17): universities are out of the
;; repository's scope -- the per-country regex covers central government,
;; national research bodies and the first tier below -- but the strictly
;; public subtrees are kept here for a later re-exploration. Nothing in
;; pipeline.clj reads this file.
;;
;; Usage: bb scripts/detect-universities.clj <command> [args…]
;;   fetch [C…]         SPARQL harvest (all countries, or the given
;;                      country_dirs) -> data/public-universities.csv
;;   migrate            one-off: pull the level=university rows out of
;;                      countries/*/sources/wikidata/central_admin.csv
;;                      into data/public-universities.csv

(ns detect-universities
  (:require [babashka.http-client :as http]
            [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Helpers (small copies of pipeline.clj's -- the scripts stay independent)
;; ---------------------------------------------------------------------------

(def ua "world-gov-domain-names/0.1 (https://github.com/bzg)")

(defn err [& xs] (binding [*out* *err*] (println (apply str xs))))

(defn read-csv-raw [path]
  (when (fs/exists? path)
    (with-open [r (io/reader (str path))]
      (doall (csv/read-csv r)))))

(defn write-csv-file [path header rows]
  (when-let [parent (fs/parent path)]
    (fs/create-dirs parent))
  (with-open [w (io/writer (str path))]
    (csv/write-csv w (cons header rows))))

(defn extract-host [url]
  (when (and url (not (str/blank? url)))
    (-> url
        str/lower-case
        (str/replace #"^https?://" "")
        (str/replace #"^www\." "")
        (str/replace #"/.*$" "")
        (str/replace #":.*$" ""))))

;; ---------------------------------------------------------------------------
;; Output file
;; ---------------------------------------------------------------------------

(def universities-file "data/public-universities.csv")
(def universities-header ["hostname" "country" "label" "website"])

(defn- read-universities
  "{[hostname country] [hostname country label website]} from the file."
  []
  (into {}
        (for [[hostname country :as row]
              (rest (or (read-csv-raw universities-file) []))]
          [[hostname country] (vec (take 4 (concat row (repeat ""))))])))

(defn- write-universities! [by-key label]
  (write-csv-file universities-file universities-header
                  (sort-by (juxt second first) (vals by-key)))
  (println (str "Wrote " universities-file " (" (count by-key)
                " universities" label ")")))

;; ---------------------------------------------------------------------------
;; migrate -- one-off extraction from the wikidata source files
;; ---------------------------------------------------------------------------

(defn cmd-migrate
  "Move every level=university row of
  countries/*/sources/wikidata/central_admin.csv (written before the
  2026-08-17 extraction) into data/public-universities.csv, removing them
  from the source files so pipeline.clj never sees them again."
  [_]
  (let [existing (read-universities)
        moved (atom [])
        touched (atom 0)]
    (doseq [path (fs/glob "countries" "*/sources/wikidata/central_admin.csv")
            :let [path (str path)
                  country (second (re-find #"countries/([^/]+)/" path))
                  [header & rows] (read-csv-raw path)]
            :when header]
      (let [{unis true kept false}
            (group-by #(= "university" (str/trim (or (nth % 4 nil) ""))) rows)]
        (when (seq unis)
          (swap! touched inc)
          (doseq [[_type label website hostname _level] unis
                  :when (not (str/blank? hostname))]
            (swap! moved conj [hostname country (or label "") (or website "")]))
          (write-csv-file path header (or kept [])))))
    (let [merged (reduce (fn [m [hostname country :as row]]
                           (cond-> m
                             (not (contains? m [hostname country]))
                             (assoc [hostname country] row)))
                         existing @moved)]
      (write-universities! merged
                           (str "; " (count @moved) " rows migrated out of "
                                @touched " wikidata source files")))))

;; ---------------------------------------------------------------------------
;; fetch -- SPARQL refresh
;; ---------------------------------------------------------------------------

(def wikidata-endpoint "https://query.wikidata.org/sparql")

(def public-university-classes
  ["Q875538"    ; public university
   "Q62078547"  ; public research university
   "Q1145118"]) ; national university

(def excluded-classes
  ;; residual private and religious typings under the public subtrees
  ["Q902104"    ; private university
   "Q557206"    ; Catholic university
   "Q2120466"   ; pontifical university
   "Q14911880"  ; seminary
   "Q1322589"]) ; Roman College

(defn- university-query [class-qid country-qid]
  (str "SELECT DISTINCT ?org ?orgLabel ?website WHERE {\n"
       "  ?org wdt:P31/wdt:P279* wd:" class-qid " ;\n"
       "       wdt:P17 wd:" country-qid " ;\n"
       "       wdt:P856 ?website .\n"
       "  FILTER NOT EXISTS { ?org wdt:P576 ?d }\n"
       (apply str
              (for [q excluded-classes]
                (str "  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:" q " }\n")))
       "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }\n"
       "}"))

(defn- run-query [q]
  (loop [attempt 1]
    (let [resp (try (http/get wikidata-endpoint
                              {:headers {"User-Agent" ua
                                         "Accept" "application/sparql-results+json"}
                               :query-params {"query" q}
                               :throw false
                               :timeout 120000})
                    (catch Exception _ nil))]
      (if (and resp (= 200 (:status resp)) (not (str/blank? (:body resp))))
        (-> (json/parse-string (:body resp) true) :results :bindings)
        (if (< attempt 3)
          (do (Thread/sleep (* attempt 5000))
              (recur (inc attempt)))
          nil)))))

(defn cmd-fetch
  "Fetch the strictly public universities of every country (or of the
  given country_dirs) and merge them into data/public-universities.csv.
  Country QIDs come from data/country_qid.csv (bb pipeline build-qid)."
  [args]
  (let [pairs (or (seq (for [row (rest (or (read-csv-raw "data/country_qid.csv") []))
                             :let [[country _iso3 qid] row]
                             :when (or (empty? args) (some #{country} args))]
                         [qid country]))
                  (do (err "ERR: data/country_qid.csv missing or no matching country. "
                           "Run 'bb pipeline build-qid' first")
                      nil))]
    (when pairs
      (let [merged
            (reduce
             (fn [m [qid country]]
               (let [rows
                     (apply concat
                            (for [cls public-university-classes
                                  :let [bindings (run-query
                                                  (university-query cls qid))
                                        _ (Thread/sleep 1000)]]
                              (if (nil? bindings)
                                (do (err "FAIL " country " " cls) [])
                                (for [b bindings
                                      :let [website (get-in b [:website :value])
                                            host (extract-host website)]
                                      :when host]
                                  [host country
                                   (get-in b [:orgLabel :value] "")
                                   website]))))]
                 (err (str "=== " country " : " (count rows) " rows"))
                 ;; fresh rows first: a refetched label wins over a stale one
                 (reduce (fn [m [hostname country :as row]]
                           (assoc m [hostname country] row))
                         m rows)))
             (read-universities)
             pairs)]
        (write-universities! merged "")))))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(def commands
  {"fetch"   cmd-fetch
   "migrate" cmd-migrate})

(defn usage []
  (println "Usage: bb scripts/detect-universities.clj <command> [args…]")
  (println)
  (println "Commands:")
  (println "  fetch [C…] | migrate"))

(let [args *command-line-args*]
  (if (empty? args)
    (do (usage) (System/exit 1))
    (let [[cmd & rest-args] args]
      (if (#{"-h" "--help" "help"} cmd)
        (usage)
        (if-let [f (get commands cmd)]
          (f (vec rest-args))
          (do (err "ERR: unknown sub-command '" cmd "'")
              (usage)
              (System/exit 1)))))))

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

(ns detect-universities
  (:require [common :refer :all]
            [enrich :as enrich]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Output file
;; ---------------------------------------------------------------------------

(def universities-file "data/public-universities.csv")
(def universities-header ["hostname" "country" "label" "website"])

(defn- read-universities
  "Read data/public-universities.csv into {[hostname country] [hostname
  country label website]}."
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
;; fetch -- SPARQL refresh
;; ---------------------------------------------------------------------------

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

(defn- run-query
  "Return the bindings of a SPARQL query, nil when the endpoint did not
  answer or sent no JSON."
  [q]
  (try (some-> (enrich/wikidata-run-query q) (json/parse-string true) :results :bindings)
       (catch Exception _ nil)))

(defn cmd-fetch
  "Fetch the strictly public universities of every country (or of the
  given country_dirs) from Wikidata and merge them into
  data/public-universities.csv. Return 1 when data/country_qid.csv (bb
  pipeline build-qid) is missing or matches no country."
  [args]
  (let [pairs (or (seq (for [row (rest (or (read-csv-raw "data/country_qid.csv") []))
                             :let [[country _iso3 qid] row]
                             :when (or (empty? args) (some #{country} args))]
                         [qid country]))
                  (do (err "ERR: data/country_qid.csv missing or no matching country. "
                           "Run 'bb pipeline build-qid' first")
                      nil))]
    (if-not pairs
      1
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
        (write-universities! merged "")
        0))))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(def commands
  {"fetch" cmd-fetch})

(defn usage []
  (println "Usage: bb scripts/detect-universities.clj <command> [args…]")
  (println)
  (println "Commands:")
  (println "  fetch [C…]"))

;; Run only as a script, not when loaded from another namespace.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

(ns registries
  "Official registries feeding sources/<registry>/registered.csv: esr
  (French public higher education) and govuk (UK sub-central bodies).
  The directories (CISA, lannuaire…) are harvested by
  detect-from-directories.clj. pipeline.clj dispatches."
  (:require [common :refer :all]
            [enrich :as enrich]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as str]))

;; ===========================================================================
;;  Registries -- ESR (FR)
;; ===========================================================================

(def esr-url
  (str "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/"
       "datasets/fr-esr-principaux-etablissements-enseignement-superieur/exports/json"))

(defn esr-domain
  "The registrable domain of an ESR institution's website, nil when it
  names none: off .fr only an apex site counts (unc.nc; a deeper host
  may sit on a shared provider), under .fr a deeper host names its
  parent (engees.unistra.fr -> unistra.fr), gouv.fr aside (curated
  already). Pure."
  [url]
  (when-let [host (extract-host url)]
    (when-let [domain (parent-domain host)]
      (when (and (valid-hostname? domain)
                 (not (host-covered? domain #{"gouv.fr"}))
                 (or (str/ends-with? domain ".fr") (= domain host)))
        domain))))

(defn cmd-esr [_]
  ;; Fetch the higher-education ministry's catalogue of the main
  ;; institutions (universities, grandes écoles, grands établissements)
  ;; into sources/esr/registered.csv: the distinct domains of the public
  ;; ones, level central -- they are all state establishments, the level
  ;; academia gives France's universities. The private ones are left out.
  (let [where "secteur_d_etablissement=\"public\""
        body  (http-get esr-url {:timeout 90
                                 :query-params {"select" "url" "where" where}})]
    (if (str/blank? body)
      (do (err "ERR: ESR fetch failed") 1)
      (let [domains (->> (json/parse-string body true)
                         (keep #(esr-domain (:url %)))
                         distinct
                         sort)
            out (registered-file "FRA_france" "esr")]
        (write-registered! "FRA_france" "esr" (for [d domains] [d "central"]))
        (println (str "Wrote " out " (" (count domains) " public higher-education domains)"))
        0))))

;; ===========================================================================
;;  Utilitaire -- govuk (UK sub-central exclusions for the gov.uk suffix)
;; ===========================================================================

(def govuk-domains-url
  ;; Official CDDO list of every registered .gov.uk domain (one column).
  ;; Snapshot asset: update the URL when a newer list is published on
  ;; https://www.gov.uk/government/publications/list-of-gov-uk-domain-names
  (str "https://assets.publishing.service.gov.uk/media/"
       "69cbd582024cdf09254f3f7d/"
       "List_of_.gov.uk_domain_names_as_of_31_March_2026_1.csv"))

(def govuk-local-label-patterns
  "Label patterns of sub-central gov.uk bodies: councils of all tiers and
  their -pc/-tc/… suffixes, -pcc commissioners, -ca combined authorities,
  -ni devolved departments, fire services (anchored: fire.gov.uk is a
  Home Office campaign) and national parks."
  [#"parish"
   #"village"
   #"(town|community|county|city|district|borough)-?council"
   #"councils"
   #"-council$"
   #"-(pc|tc|cc|bc|dc|mbc|lbc|rbc|cbc|udc|rdc|gpc|jpc|pcc|pfcc|wcc|ecc|aptc|ca|cca|ni|vjb)$"
   #"[a-z0-9](pc|tc|cc|bc|dc)$"
   #"shire$"
   #"district$"
   #"(alc|lca|ifca)$"
   #"idbs?$|drainageboards?$"
   #"waste$"
   #"foster"
   #"porthealth|crematorium"
   #".+-?fire(-?service|-?control)?$"
   #"fire-?and-?rescue"
   #"^firescotland$"
   #"nationalpark|national-park|-npa$"])

(def govuk-central-allowlist
  "Central-government labels the patterns above would wrongly exclude
  (acronym bodies ending in pc/tc/cc/bc: RPC, DECC, HMGCC, JNCC, IPCC…)."
  #{"rpc" "apc" "otc" "decc" "hmgcc" "jncc" "ipcc" "aebc"})

(def govuk-extra-local-labels
  "Sub-central labels neither the patterns nor Wikidata catch (nidirect:
  the Northern Ireland citizen portal)."
  #{"nidirect"})

(def govuk-national-gss
  ;; Country/UK-level GSS code prefixes: a body anchored to one of these is
  ;; national (Natural England, Charity Commission, NIO…), not sub-central.
  #"^(E92|S92|W92|N92|K0)")

(defn- govuk-label-of-website
  "Return the registrable gov.uk label of a website URL, whatever the host
  depth (https://beta.xcouncil.gov.uk/ -> xcouncil); nil when not a
  gov.uk site."
  [web]
  (some-> (re-find
           #"^https?://(?:[a-z0-9-]+\.)*([a-z0-9-]+)\.gov\.uk(?:[/:?#]|$)"
           (str/lower-case (str web)))
          second))

(defn- govuk-wikidata-locals
  "Return the set of gov.uk labels of the Wikidata entities anchored below
  country level in the UK statistical geography (GSS code P836 on the
  entity or on its P1001 jurisdiction); govuk-national-gss codes are
  dropped."
  []
  (let [queries
        ["SELECT ?gss ?web WHERE { ?item wdt:P836 ?gss ; wdt:P856 ?web . }"
         (str "SELECT ?gss ?web WHERE {\n"
              "  ?item wdt:P1001 ?area ; wdt:P856 ?web .\n"
              "  ?area wdt:P836 ?gss . }")]]
    (->> queries
         (mapcat (fn [q]
                   ;; A missing (network failure) or truncated (invalid
                   ;; JSON) WDQS answer must not pass for an empty result:
                   ;; up to 3 attempts, then fail loudly rather than
                   ;; silently under-excluding.
                   (loop [attempt 1]
                     (let [body (enrich/wikidata-run-query q)
                           _ (Thread/sleep 1000)
                           parsed
                           (if (str/blank? body)
                             (do (err "  [govuk] no WDQS response"
                                      " (attempt " attempt ")")
                                 ::fail)
                             (try (-> (json/parse-string body true)
                                      :results :bindings)
                                  (catch Exception e
                                    (err "  [govuk] truncated WDQS response"
                                         " (attempt " attempt "): "
                                         (.getMessage e))
                                    ::fail)))]
                       (cond (not= parsed ::fail) parsed
                             (< attempt 3) (recur (inc attempt))
                             :else (throw (ex-info (str "WDQS query failed "
                                                        "3 times, aborting")
                                                   {})))))))
         (keep (fn [b]
                 (let [label (govuk-label-of-website (get-in b [:web :value] ""))
                       gss   (get-in b [:gss :value] "")]
                   (when (and label (not (re-find govuk-national-gss gss)))
                     label))))
         set)))

(defn cmd-govuk [_]
  ;; Build the GBR sub-central list: every gov.uk label belonging to a
  ;; sub-central body (councils of all tiers, combined authorities, fire
  ;; services, national parks, NI devolved departments…), so the policy
  ;; table keeps only UK central government under the gov.uk suffix.
  ;; Universe: the official CDDO list of registered gov.uk domains,
  ;; classified by Wikidata GSS anchoring plus naming conventions. Local
  ;; bodies enter sources/govuk/registered.csv as level local, devolved
  ;; ones as central-1.
  (let [body (http-get govuk-domains-url {:timeout 90})]
    (if (str/blank? body)
      (do (err "ERR: gov.uk domain list fetch failed (" govuk-domains-url ")") 1)
      (let [universe (->> (csv/read-csv (java.io.StringReader. body))
                          rest
                          (keep #(some->> (first %) str/trim str/lower-case
                                          (re-matches #"([a-z0-9-]+)\.gov\.uk")
                                          second))
                          set)
            wd-hits (set (filter universe (govuk-wikidata-locals)))
            pattern-hit? (fn [l] (and (not (govuk-central-allowlist l))
                                      (boolean (some #(re-find % l)
                                                     govuk-local-label-patterns))))
            sub-central (-> wd-hits
                            (into (filter pattern-hit? universe))
                            (into (filter universe govuk-extra-local-labels)))
            central-1? (fn [l] (or (str/ends-with? l "-ni")
                                   (contains? #{"nidirect" "firescotland"} l)))
            {devolved true local false} (group-by (comp boolean central-1?) (sort sub-central))
            out (registered-file "GBR_united_kingdom" "govuk")]
        (write-registered! "GBR_united_kingdom" "govuk"
                           (concat (for [l local] [(str l ".gov.uk") "local"])
                                   (for [l devolved] [(str l ".gov.uk") "central-1"])))
        (println (str "Wrote " out " (" (count local) " local labels out"
                      " of " (count universe) " registered gov.uk domains; "
                      (count wd-hits) " matched via Wikidata GSS; "
                      (count devolved) " central-1 rows)"))
        0))))

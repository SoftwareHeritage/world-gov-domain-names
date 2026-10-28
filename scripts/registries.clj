(ns registries
  "The gov.uk registry, feeding sources/govuk/registered.csv: the CDDO
  list of gov.uk domains, sub-central bodies told apart. The other
  registries (CISA, lannuaire, esr…) are harvested by
  detect-from-directories.clj. pipeline.clj dispatches."
  (:require [common :refer :all]
            [enrich :as enrich]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as str]))

;; ===========================================================================
;;  Utilitaire -- govuk (UK sub-central exclusions for the gov.uk suffix)
;; ===========================================================================

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
  ;; ones as central-1. The URL of the CDDO list, a dated snapshot, is
  ;; the 'data:' of the id cddo in sources_domains.yaml.
  (let [url  (sopc-data-url "cddo")
        body (when url (http-get url {:timeout 90}))]
    (if (str/blank? body)
      (do (when url (err "ERR: gov.uk domain list fetch failed (" url ")")) 1)
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

(ns registries
  "Official lists that feed the confirmed domains through
  write-registered! (sources/<registry>/registered.csv): the US
  federal .gov registry (CISA), France's national administration
  directory (lannuaire), the UK sub-central bodies (govuk, from the CDDO
  list and Wikidata), plus two helpers on the master data: the Wikidata
  QID mapping (build-qid) and the UN membership check (validate-un).
  Pure definitions: pipeline.clj dispatches."
  (:require [common :refer :all]
            [enrich :as enrich]
            [babashka.http-client :as http]
            [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as str]))

;; ===========================================================================
;;  Registries -- CISA (US) and lannuaire (FR)
;; ===========================================================================

(def cisa-federal-url
  "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv")

(defn cmd-cisa [_]
  ;; Fetch CISA's authoritative federal .gov registry into
  ;; sources/cisa/registered.csv (every domain, level central). Every entry
  ;; is a verified US federal executive/legislative/judicial domain, so
  ;; this is the clean central-gov source for the US -- preferred over a
  ;; bare 'gov' suffix, which false-matches 'government.com', 'govtech.io'…
  (let [body (http-get cisa-federal-url {:timeout 60})]
    (if (str/blank? body)
      (do (err "ERR: CISA fetch failed (" cisa-federal-url ")") 1)
      (let [domains (->> (rest (csv/read-csv (java.io.StringReader. body)))
                         (keep #(some-> (first %) str/trim str/lower-case))
                         (filter valid-hostname?)
                         distinct
                         sort)
            out (registered-file "USA_united_states" "cisa")]
        (write-registered! "USA_united_states" "cisa" (for [d domains] [d "central"]))
        (println (str "Wrote " out " (" (count domains) " federal .gov domains from CISA)"))
        0))))

(def lannuaire-url
  (str "https://api-lannuaire.service-public.fr/api/explore/v2.1/catalog/"
       "datasets/api-lannuaire-administration/exports/json"))

(defn cmd-lannuaire [_]
  ;; Fetch France's official national administration directory into
  ;; sources/lannuaire/registered.csv: distinct .fr registrable domains of the central
  ;; administrations (ministries + central services). The .fr filter drops
  ;; the international-org cross-references (imf.org, wmo.int, ...) that
  ;; pollute the listed websites.
  (let [where "type_organisme=\"Administration centrale (ou Ministère)\""
        body (http-get lannuaire-url
                       {:timeout 90
                        :query-params {"select" "site_internet" "where" where}})]
    (if (str/blank? body)
      (do (err "ERR: lannuaire fetch failed") 1)
      (let [domains (->> (json/parse-string body true)
                         (mapcat (fn [r]
                                   (when-let [si (:site_internet r)]
                                     (map :valeur (try (json/parse-string si true)
                                                       (catch Exception _ nil))))))
                         (keep extract-host)
                         (keep parent-domain)
                         (filter #(str/ends-with? % ".fr"))
                         (filter valid-hostname?)
                         distinct
                         sort)
            out (registered-file "FRA_france" "lannuaire")]
        (write-registered! "FRA_france" "lannuaire" (for [d domains] [d "central"]))
        (println (str "Wrote " out " (" (count domains) " .fr central-admin domains)"))
        0))))

;; ===========================================================================
;;  UN membership validation (UN Digital Library)
;; ===========================================================================

(def un-members-record-url
  "Record page of the UN member states dataset published by the Dag
  Hammarskjöld Library. Links to a dated CSV snapshot whose name changes at
  each refresh."
  "https://digitallibrary.un.org/record/4082085")

(def redirect-http-client
  "The UN Digital Library serves record files behind a 302, which the
  default no-redirect client refuses to follow."
  (http/client (assoc http/default-client-opts
                      :follow-redirects :normal
                      :connect-timeout 15000)))

(defn un-members-csv-url
  "Full URL of the most recent member_states_auths_*.csv snapshot linked
  from the record page, or nil when unreachable."
  []
  (when-let [body (http-get un-members-record-url)]
    (some->> (re-seq #"files/(member_states_auths_[0-9-]+\.csv)" body)
             (map second)
             sort
             last
             (str un-members-record-url "/files/"))))

(defn date-count
  "Number of dates in a comma-separated list ('1945-10-24, 1971-09-02' -> 2)."
  [s]
  (count (remove str/blank? (str/split (str s) #","))))

(defn un-member-iso3s
  "Parse the UN library CSV into the set of ISO3 codes of current members.
  The file is a name authority where Start/End date hold comma-separated
  usage episodes: a name is in current use when it has more start dates
  than end dates (a plain empty-End-date test misses countries that went
  through renames, e.g. Egypt or Cambodia)."
  [body]
  (let [rows (csv/read-csv body)
        headers (first rows)]
    (into #{}
          (for [row (rest rows)
                :let [m (zipmap headers row)
                      iso (str/trim (or (get m "ISO Code") ""))]
                :when (and (not (str/blank? iso))
                           (> (date-count (get m "Start date"))
                              (date-count (get m "End date"))))]
            iso))))

(defn cmd-validate-un
  "Check the un_status column of data/world-governments.csv against the
  official UN member list. Returns 1 -- the dispatcher exits with it -- on
  a fetch failure, any mismatch or a blank un_status (blank would silently
  default to member everywhere else in the pipeline)."
  [_]
  (println "Fetching official UN member list from digitallibrary.un.org…")
  (let [url (un-members-csv-url)
        body (when url (http-get url {:client redirect-http-client}))]
    (if (str/blank? body)
      (do (err "ERR: could not fetch the UN member states CSV")
          1)
      (let [official (un-member-iso3s body)
            local (for [row (or (read-csv-file "data/world-governments.csv") [])]
                    {:name (get row "Country")
                     :iso (str/trim (or (get row "iso3") ""))
                     :status (str/trim (or (get row "un_status") ""))})
            local-iso3s (set (map :iso local))
            problems
            (concat
             (for [{:keys [name status]} local
                   :when (str/blank? status)]
               (str name ": blank un_status (set member/observer/non_un explicitly)"))
             (for [{:keys [name iso status]} local
                   :when (and (= status "member") (not (official iso)))]
               (str name ": marked member but absent from the official UN list"))
             (for [{:keys [name iso status]} local
                   :when (and (#{"observer" "non_un"} status) (official iso))]
               (str name ": marked " status " but the UN lists it as a member"))
             (for [iso (sort official)
                   :when (not (local-iso3s iso))]
               (str iso ": UN member missing from data/world-governments.csv")))]
        (println (str "  official members: " (count official)
                      " ; rows in world-governments.csv: " (count local)))
        (if (seq problems)
          (do (doseq [p problems] (println (str "  MISMATCH " p)))
              (err "ERR: " (count problems) " mismatch(es)")
              1)
          (println "  un_status is consistent with the official UN list."))))))

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
  "Naming conventions of sub-central bodies registered directly under
  gov.uk: parish/town/community and principal councils, the -pc/-tc/…
  council suffixes, police-and-crime commissioners (-pcc), combined
  authorities (-ca), Northern Ireland devolved departments (-ni), fire
  services and national parks. fire.gov.uk and firekills.gov.uk are Home
  Office campaigns, hence the anchored fire patterns."
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
  "Central-government labels the patterns above would wrongly exclude:
  acronym bodies ending in pc/tc/cc/bc (Regulatory Policy Committee, DECC,
  HMG Communications Centre, Joint Nature Conservation Committee, IPCC,
  Animal Procedures Committee, Agriculture and Environment Biotechnology
  Commission, ...)."
  #{"rpc" "apc" "otc" "decc" "hmgcc" "jncc" "ipcc" "aebc"})

(def govuk-extra-local-labels
  "Sub-central labels neither the patterns nor the Wikidata queries catch:
  nidirect (NI Direct, the Northern Ireland citizen portal)."
  #{"nidirect"})

(def govuk-national-gss
  ;; Country/UK-level GSS code prefixes: a body anchored to one of these is
  ;; national (Natural England, Charity Commission, NIO…), not sub-central.
  #"^(E92|S92|W92|N92|K0)")

(defn- govuk-label-of-website
  "The registrable gov.uk label of a website URL, whatever the host depth:
  https://beta.xcouncil.gov.uk/ -> xcouncil. Nil when not a gov.uk site."
  [web]
  (some-> (re-find
           #"^https?://(?:[a-z0-9-]+\.)*([a-z0-9-]+)\.gov\.uk(?:[/:?#]|$)"
           (str/lower-case (str web)))
          second))

(defn- govuk-wikidata-locals
  "#{gov.uk-label} of the Wikidata entities anchored below country level
  in the UK statistical geography: the entity itself carries a GSS code
  (P836 -- council areas) or its P1001 jurisdiction does (council
  organisations). Country/UK-level GSS codes are filtered out (see
  govuk-national-gss)."
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

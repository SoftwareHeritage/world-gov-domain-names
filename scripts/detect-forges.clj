#!/usr/bin/env bb
;; detect-forges -- government forges unknown to Software Heritage, and
;; leads for government source-code catalogs (Babashka).
;;
;; Feeds the "Government source-code catalogs" section of
;; swh-sopc-data-sources and the SWH archival-coverage checks. This is a
;; SIDE tool: it shares the harvested data of this repository but serves
;; the Software Heritage catalog goal, not the domain-regex goal --
;; hence its extraction out of pipeline.clj (2026-08-17).
;;
;; Usage: bb scripts/detect-forges.clj <command> [args…]
;;   scan [recheck] [github-orgs] [COUNTRY…|TARGET…]
;;                      forges unknown to SWH -> data/forge-unknown-swh.csv
;;                      (recheck or targets: no discovery, the forges of
;;                      the file and of known-forges.csv only)
;;   catalogs [COUNTRY…]
;;                      hosts that look like a source-code catalog
;;                      -> data/catalog-candidates.csv
;;   github-orgs        GitHub governments.yml -> data/github-gov-orgs.csv
;;
;; Environment variables:
;;   SWH_TOKEN          authenticated SWH API requests (higher rate limit)
;;   PARALLEL           # concurrent requests (default 64 over the
;;                      harvested hosts, 8 otherwise)
;;   DOH_URL            DNS-over-HTTPS resolver handed to curl (default
;;                      https://1.1.1.1/dns-query; empty: system resolver)

(ns detect-forges
  (:require [common :refer :all]
            [pipeline :as pipeline]
            [babashka.http-client :as http]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; SWH archive coverage
;; ---------------------------------------------------------------------------

(def swh-search-endpoint "https://archive.softwareheritage.org/api/1/origin/search/")

(defn- swh-get
  "GET the SWH origin-search API for pattern (at most limit results) and
  return the response map, nil on a network error. SWH_TOKEN authenticates
  the request."
  [pattern limit]
  (let [token (System/getenv "SWH_TOKEN")]
    (try (http/get (str swh-search-endpoint pattern "/")
                   {:client http-client
                    :headers (cond-> {"User-Agent" ua
                                      "Accept" "application/json"}
                               token (assoc "Authorization"
                                            (str "Bearer " token)))
                    :query-params {"limit" (str limit)}
                    :throw false
                    :timeout 30000})
         (catch Exception _ nil))))

(defn- rate-limit-pause!
  "Sleep until the API's x-ratelimit-reset epoch (60s when absent, at most
  an hour), announcing why on stderr."
  [resp why]
  (let [reset (some-> (get-in resp [:headers "x-ratelimit-reset"]) parse-long)
        now   (quot (System/currentTimeMillis) 1000)
        wait  (if reset (min 3600 (max 1 (- reset now))) 60)]
    (err (str "  (" why ", pausing " wait "s)"))
    (Thread/sleep (* 1000 wait))))

(defn origin-matches?
  "True when origin url lives on target: same host or a subdomain, and
  under /<org>/ for a github.com/<org> target."
  [target url]
  (let [[thost tpath] (str/split (str/lower-case target) #"/" 2)
        [_ host path] (re-find #"^[a-z+]+://([^/]+)(.*)" (str/lower-case (str url)))]
    (and host
         (or (= host thost) (str/ends-with? host (str "." thost)))
         (or (nil? tpath) (str/starts-with? path (str "/" tpath "/"))))))

(def swh-search-limit 100)

(defn swh-origins-count
  "Return the number of origins the SWH archive knows on target (0 =
  unknown), capped at swh-search-limit; nil when the API could not be
  answered. Waits the rate-limit window out on a 429."
  [target]
  (loop [attempt 1]
    (let [resp      (swh-get target swh-search-limit)
          status    (:status resp)
          remaining (some-> (get-in resp [:headers "x-ratelimit-remaining"])
                            parse-long)]
      (cond
        (= 200 status)
        (let [n (try (->> (json/parse-string (:body resp))
                          (map #(get % "url"))
                          (filter #(origin-matches? target %))
                          count)
                     (catch Exception _ nil))]
          (when (and remaining (<= remaining 1))
            (rate-limit-pause! resp "rate-limit window exhausted"))
          n)

        (and (= 429 status) (< attempt 6))
        (do (rate-limit-pause! resp "HTTP 429")
            (recur (inc attempt)))

        (and (nil? resp) (< attempt 3))
        (do (Thread/sleep 3000)
            (recur (inc attempt)))

        :else nil))))

(defn swh-auth-check!
  "Validate SWH_TOKEN with one authenticated request when it is set.
  Return true when the sweep may proceed (with or without token), false
  when the API rejects the token."
  []
  (if-not (System/getenv "SWH_TOKEN")
    true
    (let [resp (swh-get "github.com/torvalds/linux" 1)]
      (cond
        (= 200 (:status resp))
        (do (err (str "  SWH_TOKEN accepted (rate limit: "
                      (get-in resp [:headers "x-ratelimit-remaining"] "?")
                      " of "
                      (get-in resp [:headers "x-ratelimit-limit"] "?")
                      " requests left in this window)"))
            true)

        (contains? #{401 403} (:status resp))
        (do (err "ERR: SWH_TOKEN rejected by the SWH API (expired or revoked?).")
            (err "     Generate a new one at https://archive.softwareheritage.org/oidc/profile/#tokens")
            false)

        :else
        (do (err "WARN: could not validate SWH_TOKEN (network error); proceeding anyway")
            true)))))

;; ---------------------------------------------------------------------------
;; Fingerprinting -- which forge software a target runs
;; ---------------------------------------------------------------------------
;; One request for the homepage, then a dozen well-known endpoints on
;; the few hosts that stay ambiguous.

;; DNS-over-HTTPS resolver handed to curl. The system resolver drops
;; most lookups at 64 concurrent fingerprints (curl exit 6 on live
;; hosts); DOH_URL= (empty) falls back on it, to use with a low PARALLEL.
(def doh-url (or (System/getenv "DOH_URL") "https://1.1.1.1/dns-query"))

;; [forge-type regex], matched in order against the response headers of
;; every hop followed by the final body; the first match wins. They run
;; over every government site: a page that merely mentions "Redmine" or
;; links to Bitbucket must not match, hence headers, cookies and
;; generator tags rather than words.
(def forge-signatures
  [["heptapod"     #"(?i)<title>[^<]*Heptapod|content=\"Heptapod\""]
   ["gitlab"       #"(?im)^x-gitlab-meta:"]
   ["gitlab"       #"(?im)^set-cookie:\s*_gitlab_session="]
   ["gitlab"       #"(?i)content=\"GitLab\"|<title>[^<]*· GitLab</title>|<title>GitLab is not responding"]
   ["forgejo"      #"(?i)content=\"Forgejo|Powered by Forgejo"]
   ["gitea"        #"(?i)content=\"Gitea|Powered by Gitea"]
   ["gitea"        #"(?im)^set-cookie:\s*i_like_gitea="]
   ["gogs"         #"(?im)^set-cookie:\s*i_like_gogs="]
   ["gogs"         #"(?i)content=\"Gogs"]
   ["github-enterprise" #"(?im)^x-github-enterprise-version:"]
   ["bitbucket"    #"(?im)^set-cookie:\s*BITBUCKETSESSIONID="]
   ["bitbucket"    #"(?i)<meta name=\"application-name\" content=\"Bitbucket\""]
   ["azure-devops" #"(?im)^x-tfs-processid:|^x-vss-e2eid:"]
   ["gerrit"       #"(?i)<title>Gerrit Code Review|<gr-app"]
   ["gitiles"      #"(?i)googlesource\.com/gitiles"]
   ["phabricator"  #"(?im)^set-cookie:\s*phsid="]
   ["phabricator"  #"(?i)phabricator-standard-page"]
   ["tuleap"       #"(?im)^set-cookie:\s*(?:__Host-)?TULEAP_"]
   ["fusionforge"  #"(?i)content=\"FusionForge|Powered By FusionForge"]
   ["redmine"      #"(?im)^set-cookie:\s*_redmine_session="]
   ["redmine"      #"(?i)<meta name=\"description\" content=\"Redmine\""]
   ["trac"         #"(?im)^set-cookie:\s*trac_(?:form_token|session)="]
   ["trac"         #"(?i)Powered by <a[^>]*><strong>Trac"]
   ["rhodecode"    #"(?i)RhodeCode (?:Enterprise|Community)|<title>[^<]*RhodeCode"]
   ["kallithea"    #"(?i)kallithea-scm\.org"]
   ["gitbucket"    #"(?i)gitbucket\.(?:js|css)|<title>[^<]*GitBucket"]
   ["gitblit"      #"(?i)gitblit\.(?:css|png)|<title>[^<]*Gitblit"]
   ["onedev"       #"(?i)<title>[^<]*\bOneDev\b"]
   ["scm-manager"  #"(?i)<title>SCM-Manager"]
   ["sourcehut"    #"(?i)>\s*sourcehut\s*</a>"]
   ["allura"       #"(?i)Apache Allura"]
   ["pagure"       #"(?i)<title>[^<]* - Pagure|/static/pagure[-.]"]
   ["bonobo"       #"(?i)Bonobo Git Server"]
   ["savane"       #"(?i)Powered by Savane"]
   ["cgit"         #"(?i)<meta name='generator' content='cgit|id='cgit'"]
   ["gitweb"       #"(?i)<meta name=\"generator\" content=\"gitweb|<!-- git web interface"]
   ["hgweb"        #"(?i)Mercurial repositories index|static/hg(?:logo|icon)\.png"]
   ["fossil"       #"(?i)<meta name=\"generator\" content=\"Fossil|by\s+Fossil\s+(?:version\s+)?\d+\.\d+"]
   ["viewvc"       #"(?i)<meta name=\"generator\" content=\"ViewVC"]
   ["websvn"       #"(?i)Powered by <a[^>]*>WebSVN"]
   ["subversion"   #"(?i)Powered by <a href=\"https?://subversion\.(?:apache|tigris)\.org/?\">(?:Apache )?Subversion"]])

;; A homepage that redirects to an organization on a public forge.
(def public-forge-pattern
  #"(?i)^https://(?:www\.)?(github\.com|gitlab\.com|bitbucket\.org|codeberg\.org|sourceforge\.net)/(?!login|oauth|users/|account)[\w.-]+")

;; [path forge-type regex] asked, without following redirects, on the
;; hosts the homepage left ambiguous: API endpoints that answer even
;; when the web interface demands a login, and the usual sub-paths of a
;; forge that does not sit at the root (no forge-type: forge-signatures
;; decide).
(def forge-endpoints
  [["/api/v4/version" "gitlab"
    #"(?im)^x-gitlab-meta:|\"message\":\"401 Unauthorized\"|\"version\":\"[^\"]+\",\"revision\""]
   ["/api/v1/version" "forgejo" #"\"version\":\"[^\"]*(?:forgejo|\+gitea)"]
   ["/api/v1/version" "gitea"   #"\{\"version\":\"\d+\.\d+[^\"]*\"\}\s*$|/api/swagger\""]
   ["/api/v3/meta" "github-enterprise"
    #"(?im)^x-github-enterprise-version:|\"installed_version\"|enterprise-server@"]
   ["/rest/api/1.0/application-properties" "bitbucket" #"\"displayName\":\"Bitbucket\""]
   ["/config/server/version" "gerrit" #"(?m)^\)\]\}'\s*\"\d"]
   ["/api/version" "tuleap" #"\"flavor_name\""]
   ["/_apis/connectionData" "azure-devops"
    #"(?im)^x-tfs-processid:|^x-vss-e2eid:|\"locationServiceData\""]
   ["/gitlab/users/sign_in"] ["/git/"] ["/cgit/"] ["/gitweb/"] ["/svn/"] ["/hg/"]])

;; Hostname labels hinting at a forge (any label, digits allowed:
;; scm2.dev.example.gov).
(def forge-host-hint
  #"(?i)(?:^|[.-])(?:git\w*|gogs|g?forges?|scm|svn|hg|cvs|vcs|codes?|src|sources?|repos?|repository|gerrit|stash|bitbucket|tfs|devops|redmine|trac|phabricator|tuleap|developers?|oss|open-?source)\d*[.-]")

;; 200-responses that do not expose the forge itself.
(def page-note-markers
  [["blocked by Incapsula WAF"  #"_Incapsula_Resource"]
   ["blocked by Cloudflare"     #"(?i)Attention Required! \| Cloudflare|cf-chl-"]
   ["reverse-proxy default page, forge not exposed" #"(?i)Nginx Proxy Manager"]])

(def curl-exit-notes
  {6  "DNS does not resolve"
   7  "connection refused"
   28 "timeout"
   35 "TLS handshake failed"
   52 "empty reply"
   56 "connection reset"})

(defn- first-matching
  "First label of the [label regex] pairs whose regex matches s, else nil."
  [pairs s]
  (some (fn [[label re]] (when (re-find re s) label)) pairs))

(defn- forge-note
  "Return a short diagnostic for a fetched homepage: curl error, WAF or default page,
  or a redirect that left the target's host. Empty string otherwise."
  [target final-url body exit]
  (or (when-not (zero? exit)
        (get curl-exit-notes exit (str "curl exit " exit)))
      (first-matching page-note-markers body)
      (let [[_ host] (re-find #"^https?://([^/:]+)" (str/lower-case (str final-url)))]
        (when (and host (not= host (str/lower-case (first (str/split target #"/")))))
          ;; drop the query string: SSO redirects carry volatile state/nonce
          ;; parameters that would churn the CSV on every run
          (str "redirects to " (str/replace final-url #"\?.*" ""))))
      ""))

(defn- curl-pages!
  "GET urls with one curl process -- certificates not verified, the
  connection reused across the urls of a host, redirects followed with
  :follow? -- and return one {:status :exit :url :text} per url: the
  final HTTP code ('000' when no response came back), curl's exit code,
  the final URL and the response headers of every hop followed by the
  body, capped."
  [urls & {:keys [follow?]}]
  (let [args (concat ["curl" "-ksi" "--compressed" "-A" ua
                      "--connect-timeout" "8" "--max-time" (if follow? "20" "8")
                      "--max-filesize" "5000000"
                      "-w" "\n@@@%{http_code} %{exitcode} %{url_effective}@@@\n"]
                     (when follow? ["-L" "--max-redirs" "5"])
                     (when-not (str/blank? doh-url) ["--doh-url" doh-url])
                     urls)
        out  (try (:out (apply proc/sh args))
                  (catch Exception _ ""))]
    (for [[_ text status exit url]
          (re-seq #"(?s)(.*?)\n@@@(\d{3}) (\d+) (\S*)@@@\n" (str out))]
      {:status status :exit (parse-long exit) :url url
       :text (subs text 0 (min (count text) 300000))})))

(defn- homepage!
  "The curl-pages! page of the homepage of target, a host or a
  host/path, redirects followed; status '000' when no response came
  back."
  [target]
  (or (first (curl-pages! [(str "https://" target (when-not (str/includes? target "/") "/"))]
                          :follow? true))
      {:status "000" :exit 1 :url "" :text ""}))

(defn- endpoint-hit
  "[forge-type url] of the first forge-endpoints entry that answers as
  a forge under base, else nil."
  [base]
  (let [paths (distinct (map first forge-endpoints))
        pages (zipmap paths (curl-pages! (map #(str base %) paths)))]
    (some (fn [[path type re]]
            (when-let [{:keys [url text]} (pages path)]
              (when-let [type (if re
                                (when (re-find re text) type)
                                (first-matching forge-signatures text))]
                [type url])))
          forge-endpoints)))

(defn fingerprint!
  "Fingerprint target, a host or a host/path: fetch its homepage, then
  ask forge-endpoints when the homepage shows no forge and the host is
  ambiguous -- its name hints at a forge, its root is closed (401, 403)
  or empty (404) -- or deep? is set; never past a WAF page. Return
  {:type :status :note}: the forge software (the host of a public forge
  the homepage redirects to), nil when none shows; the HTTP code of the
  homepage ('000' when no response came back); a short diagnostic, the
  forge-note and where the forge shows when off the homepage."
  [target & {:keys [deep?]}]
  (let [host? (not (str/includes? target "/"))
        {:keys [status exit url text]} (homepage! target)
        type  (if-let [[_ forge] (re-find public-forge-pattern (str url))]
                (str/lower-case forge)
                (first-matching forge-signatures text))
        [etype eurl]
        (when (and (nil? type) host? (not= status "000")
                   (not (first-matching page-note-markers text))
                   (or deep?
                       (re-find forge-host-hint target)
                       (contains? #{"401" "403" "404"} status)))
          (endpoint-hit (str "https://" target)))]
    {:type   (or type etype)
     :status status
     :note   (->> [(forge-note target url text exit)
                        ;; no query string, as in forge-note
                        (when eurl (str "forge at " (str/replace eurl #"\?.*" "")))]
                       (remove str/blank?)
                       (str/join "; ")
                       single-line)}))

;; ---------------------------------------------------------------------------
;; Harvested hosts -- what scan and catalogs fetch
;; ---------------------------------------------------------------------------

;; Harvest statuses not worth a request: the host resolved but port 443
;; did not open. A host that connected and was slow to answer is
;; fetched; DNS failures too -- one in ten is an artefact of a resolver
;; overloaded by the harvest probe -- and TLS errors: self-signed and
;; expired certificates are common on self-hosted forges, and curl does
;; not verify them.
(def scan-closed-pattern
  #"(?i)connect(?:ion)? timed out|could not connect|^ConnectException$")

(defn- scan-targets
  "{host country} of the hosts of countries in the central+ scope: the
  longest confirmed or excluded domain a host sits under decides, and
  must be central or central-1. The hosts the harvest found closed
  (scan-closed-pattern) are left out, unless their name hints at a
  forge: the harvest probe gives up after a few seconds. A host
  harvested for several countries goes to the first one."
  [countries]
  (let [confirmed (group-by first (confirmed-rows))]
    (into {}
          (for [c (reverse countries)
                :let [level-of (into {} (for [[_ d level] (confirmed c)] [d level]))
                      excluded (get @pipeline/excluded-domains c #{})]
                [host _parent status] (pipeline/country-hosts c)
                :let [level (some #(if (excluded %) "excluded" (level-of %))
                                  (host-suffixes host))]
                :when (and (contains? harvest-levels level)
                           (or (re-find forge-host-hint host)
                               (not (re-find scan-closed-pattern (str status)))))]
            [host c]))))

(defn- scan-hosts!
  "{host (f host)} of hosts, 64 at a time (PARALLEL) and in random order
  so that the load spreads across servers."
  [f hosts]
  (let [hosts (shuffle (vec hosts))
        done  (atom 0)]
    (err "Fetching " (count hosts) " hosts")
    (zipmap hosts
            (bounded-pmap (parallel 64)
                          (fn [host]
                            (let [result (f host)
                                  n      (swap! done inc)]
                              (when (zero? (mod n 5000))
                                (err "  ... " n "/" (count hosts)))
                              result))
                          hosts))))

(defn- none-answered?
  "True when a scan-hosts! map has results and all have status '000'."
  [scanned]
  (and (seq scanned) (every? #(= "000" (:status %)) (vals scanned))))

;; ---------------------------------------------------------------------------
;; Scan -- the forges unknown to the SWH archive
;; ---------------------------------------------------------------------------
;; Candidates are the hosts of the central+ scope a fingerprint finds a
;; forge on, whatever their name, plus the curated forges of
;; data/known-forges.csv, which the discovery cannot reach: most sit on
;; domains outside the harvest (opencode.de, code.europa.eu…).

(def known-forges-file "data/known-forges.csv")
(def unknown-file "data/forge-unknown-swh.csv")
(def unknown-header
  ["target" "country" "kind" "source" "forge_type" "http_status" "note"])

(defn- fingerprint-or-down!
  "fingerprint!, an exception reported and read as no response."
  [target & opts]
  (try (apply fingerprint! target opts)
       (catch Exception e
         (err "  " target ": " (single-line (ex-message e)))
         {:status "000" :note ""})))

(def api-error-note "SWH API error, archival status unknown")

(defn- unknown-row
  "The unknown-header row of a candidate SWH does not know, given its
  fingerprint and its previous row in the file, if any. When the host
  does not answer, or answers with a server error, the forge seen on
  it before stands. A curated forge or a GitHub organization always
  has a row, a discovered host only while a forge shows: otherwise
  nil."
  [[target country kind source] {:keys [type status note]} previous]
  (let [seen (nth previous 4 nil)
        type (or type
                 (when (and (re-matches #"000|5\d\d" status)
                            (not (contains? #{nil "" "unknown"} seen)))
                   seen))]
    (when (or type (not= source "scan"))
      [target country kind source (or type "unknown") status note])))

(defn cmd-scan
  "Write the forges the SWH archive does not know to
  data/forge-unknown-swh.csv, with their forge_type/http_status/note.
  Candidates: the hosts of the central+ scope a fingerprint finds a
  forge on (scan-targets, about an hour for every country), the curated
  forges of data/known-forges.csv, the rows already in the file and,
  with the 'github-orgs' argument, data/github-gov-orgs.csv. Each is
  checked against the SWH origin-search API; one whose lookup failed
  is kept with the note 'SWH API error'. Arguments narrow the run, the
  rows left out being kept as they are: country dirs scan these
  countries only; 'recheck' skips the discovery; names with a dot are
  targets to recheck alone. Return 1, the file untouched, on unknown
  country dirs, on countries mixed with targets, when a target is no
  candidate or when no host answered the discovery."
  [args]
  (let [flags     (set (filter #{"recheck" "github-orgs"} args))
        names     (remove flags args)
        only      (set (filter #(str/includes? % ".") names))
        countries (set (remove only names))
        scan?     (and (not (flags "recheck")) (empty? only))
        ;; GitHub organizations carry the group of governments.yml, no
        ;; country dir: a country never leaves them out
        in-scope? (fn [[target country _kind source]]
                    (and (or (empty? only) (contains? only target))
                         (or (empty? countries) (contains? countries country)
                             (= source "governments.yml"))))
        previous  (rest (read-csv-raw unknown-file))
        curated   (for [[target country kind _label] (rest (read-csv-raw known-forges-file))
                        :when (contains? #{"forge" "github-org"} kind)]
                    [target country kind "curated"])
        gh-orgs   (when (flags "github-orgs")
                    (for [[org group] (rest (read-csv-raw "data/github-gov-orgs.csv"))]
                      [(str "github.com/" org) group "github-org" "governments.yml"]))]
    (cond
      (nil? (scoped-countries (seq countries)))
      1

      (and (seq only) (seq countries))
      (do (err "ERR: give country dirs or targets, not both") 1)

      (and (flags "github-orgs") (empty? gh-orgs))
      (do (err "ERR: data/github-gov-orgs.csv missing. Run 'bb forges github-orgs' first")
          1)

      (not (swh-auth-check!))
      (do (err "ERR: aborting (unset SWH_TOKEN to run anonymously, slower)") 1)

      :else
      (let [hosts      (when scan? (scan-targets (or (seq (sort countries)) (country-dirs))))
            scanned    (if scan?
                         (scan-hosts! (fn [host]
                                        (let [{:keys [type] :as fp} (fingerprint-or-down! host)]
                                          (when type (err "  " host " -> " type))
                                          fp))
                                      (keys hosts))
                         {})
            found      (for [[host {:keys [type]}] scanned :when type]
                         [host (hosts host) "forge" "scan"])
            ;; a row whose forge left known-forges.csv stands as a
            ;; discovered one: it stays while a forge shows
            earlier    (for [[target country kind source] previous]
                         [target country kind (if (= source "curated") "scan" source)])
            ;; curated first: on a duplicate target its kind and country win
            candidates (->> (concat curated found earlier gh-orgs)
                            dedup-by-first
                            (filter in-scope?))
            missing    (remove (set (map first candidates)) only)]
        (cond
          (seq missing)
          (do (err "ERR: neither in " unknown-file " nor in " known-forges-file ": "
                   (str/join ", " missing))
              1)

          (and scan? (none-answered? scanned))
          (do (err "ERR: no host answered (network or DOH_URL down?), "
                   unknown-file " left untouched")
              1)

          :else
          (let [row-of      (into {} (map (juxt first identity) previous))
                ;; the scan asks the endpoints of ambiguous hosts only:
                ;; a candidate it saw no forge on is asked them all
                fingerprint (fn [target]
                              (let [fp (scanned target)]
                                (if (:type fp)
                                  fp
                                  (fingerprint-or-down! target :deep? true))))
                row         (fn [[target :as candidate] fp]
                              (unknown-row candidate fp (row-of target)))
                discovered? #(= "scan" (nth % 3))
                ;; a discovered host is fingerprinted before the SWH
                ;; lookup, and spared one when it leaves no row
                early       (->> (filter discovered? candidates)
                                 (bounded-pmap (parallel 8)
                                               (fn [[target]] [target (fingerprint target)]))
                                 (into {}))
                to-check    (filter (fn [[target :as candidate]]
                                      (or (not (discovered? candidate))
                                          (row candidate (early target))))
                                    candidates)
                checked     (doall
                              (for [[target :as candidate] to-check]
                                (let [n (swh-origins-count target)]
                                  (err (str "  " target " -> "
                                            (cond (nil? n) "API error"
                                                  (zero? n) "UNKNOWN to SWH"
                                                  ;; the count is capped by the
                                                  ;; query limit, not exact
                                                  (>= n swh-search-limit)
                                                  (str swh-search-limit "+ origin(s)")
                                                  :else (str n " origin(s)"))))
                                  (Thread/sleep 500)
                                  [candidate n])))
                unknown     (remove (fn [[_ n]] (and n (pos? n))) checked)
                errors      (count (filter (comp nil? second) unknown))
                rows        (bounded-pmap
                              (parallel 8)
                              (fn [[[target :as candidate] n]]
                                (cond-> (row candidate (or (early target) (fingerprint target)))
                                  (nil? n) (update 6 #(str/join "; " (remove str/blank? [api-error-note %])))))
                              unknown)
                kept        (remove (comp (set (map first candidates)) first) previous)]
            (write-csv-file unknown-file unknown-header
                            (sort-by first (concat kept rows)))
            (println (str "Wrote " unknown-file " (" (count rows) " forges unknown to SWH of "
                          (count to-check) " checked"
                          (when (pos? errors)
                            (str ", " errors " of them on an API error"))
                          (when (seq kept) (str "; " (count kept) " rows kept"))
                          ")"))))))))

;; ---------------------------------------------------------------------------
;; Catalogs -- leads for government source-code catalogs
;; ---------------------------------------------------------------------------
;; A catalog is a portal pointing at code hosted elsewhere (code.gouv.fr,
;; developers.italia.it…): no software to fingerprint, only weaker
;; signals -- its name, links to public forges, open-source vocabulary.
;; Hence leads to review by hand, the confirmed ones going to
;; data/known-catalogs.csv.

(def known-catalogs-file "data/known-catalogs.csv")
(def catalog-file "data/catalog-candidates.csv")
(def catalog-header ["hostname" "country" "title" "forge_links" "keywords"])

;; First hostname labels a catalog or a developer portal goes by. Not
;; any label: every subdomain of developer.example.gov would be a lead.
(def catalog-host-hint
  #"(?i)^(?:www\.)?(?:codes?|opencode|open-?source|oss|foss|floss|developers?|softwarepublico|softwarelibre|logiciels?-?libres?)\d*[.-]")

;; A link to an organization or a repository on a public forge.
(def forge-link-pattern
  #"(?i)https?://(?:www\.)?(?:github\.com|gitlab\.com|bitbucket\.org|codeberg\.org)/[\w.-]+(?:/[\w.-]+)?")

;; [keyword regex]: the vocabulary of a catalog, in a few languages.
(def catalog-keywords
  [["open-source"     #"(?i)open[ -]?source|c[oó]digo (?:abierto|aberto)|logiciels? libres?|software (?:libre|livre)|quelloffen|öppen källkod|avoin (?:lähde)?koodi"]
   ["public-software" #"(?i)software p[uú]blico|public code|publiccode|code\.json"]
   ["source-code"     #"(?i)source code|code source|c[oó]digo[- ]fonte|c[oó]digo fuente|quellcode|codice sorgente"]
   ["repositories"    #"(?i)\brepositories\b|dépôts de code|repositorios de c[oó]digo"]])

(defn- catalog-row
  "The catalog-header row of host given its homepage, nil when it is no
  lead: the homepage must answer 2xx and neither show a forge nor
  redirect to a public one (bb forges scan lists these), and either
  the name hints at a catalog, or the page links to five public-forge
  organizations or repositories, or it uses two of the
  catalog-keywords."
  [host country {:keys [status url text]}]
  (let [links    (count (distinct (map str/lower-case (re-seq forge-link-pattern text))))
        keywords (keep (fn [[k re]] (when (re-find re text) k)) catalog-keywords)
        title    (some-> (re-find #"(?is)<title[^>]*>(.*?)</title>" text)
                         second single-line (truncate 80))]
    (when (and (re-matches #"2\d\d" status)
               (not (first-matching forge-signatures text))
               (not (re-find public-forge-pattern (str url)))
               (or (re-find catalog-host-hint host)
                   (>= links 5)
                   (>= (count keywords) 2)))
      [host country (or title "") links (str/join "; " keywords)])))

(defn- distinct-pages
  "Drop the catalog rows repeating the page of another host of their
  country -- same title, links and keywords: www twins, mirrors, tile
  servers -- and keep the shortest hostname. Untitled rows all stay."
  [rows]
  (->> rows
       (group-by (fn [[host & page]] (if (str/blank? (second page)) host page)))
       vals
       (map #(first (sort-by (juxt (comp count first) first) %)))))

(defn cmd-catalogs
  "Fetch the homepage of every host of the central+ scope (scan-targets,
  about an hour for every country) and write the ones that look like a
  source-code catalog (catalog-row) to data/catalog-candidates.csv
  (hostname,country,title,forge_links,keywords), one row per page
  (distinct-pages), the hosts of data/known-catalogs.csv and
  data/known-forges.csv left out. With
  country dirs as arguments, fetch these countries only: the rows of
  the others are kept. Return 1, the file untouched, when no host
  answered."
  [args]
  (if-let [countries (scoped-countries args)]
    (let [known   (set (for [file [known-catalogs-file known-forges-file]
                             [target] (rest (read-csv-raw file))]
                         (first (str/split target #"/"))))
          hosts   (scan-targets countries)
          scanned (scan-hosts! (fn [host]
                                 (let [page (try (homepage! host)
                                                 (catch Exception _ {:status "000" :text ""}))
                                       row  (catalog-row host (hosts host) page)]
                                   (when row (err "  " host " -> " (nth row 2)))
                                   {:status (:status page) :row row}))
                               (remove known (keys hosts)))
          rows    (distinct-pages (keep :row (vals scanned)))
          in-scan (set countries)
          kept    (remove #(contains? in-scan (second %))
                          (rest (read-csv-raw catalog-file)))]
      (if (none-answered? scanned)
        (do (err "ERR: no host answered (network or DOH_URL down?), "
                 catalog-file " left untouched")
            1)
        (do (write-csv-file catalog-file catalog-header
                            (sort-by (juxt second first) (concat kept rows)))
            (println (str "Wrote " catalog-file " (" (count rows) " leads on "
                          (count scanned) " hosts"
                          (when (seq kept) (str "; " (count kept) " rows kept"))
                          ")")))))
    1))

;; ---------------------------------------------------------------------------
;; GitHub governments.yml
;; ---------------------------------------------------------------------------

(def governments-yml-url
  "https://raw.githubusercontent.com/github/government.github.com/gh-pages/_data/governments.yml")

(def github-gov-orgs-extra-file "data/github-gov-orgs-extra.csv")

(defn cmd-github-orgs
  "Fetch GitHub's governments.yml (government organizations grouped by the
  file's own headings) and write data/github-gov-orgs.csv (org,group),
  appending the local additions of data/github-gov-orgs-extra.csv. Return
  1 on a fetch failure."
  [_]
  (if-let [body (http-get governments-yml-url {:accept "text/plain"})]
    (let [data     (yaml/parse-string body :keywords false)
          upstream (for [[group orgs] data, org orgs] [(str org) (str group)])
          known    (set (map (comp str/lower-case first) upstream))
          extra    (for [[org group] (rest (read-csv-raw github-gov-orgs-extra-file))
                         :when (not (contains? known (str/lower-case org)))]
                     [org group])
          rows     (concat upstream extra)]
      (write-csv-file "data/github-gov-orgs.csv" ["org" "group"] rows)
      (println (str "Wrote data/github-gov-orgs.csv (" (count upstream)
                    " orgs in " (count data) " groups"
                    (when (seq extra) (str " + " (count extra) " local extras"))
                    ")")))
    (do (err "ERR: could not fetch " governments-yml-url) 1)))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(def commands
  {"scan"        cmd-scan
   "catalogs"    cmd-catalogs
   "github-orgs" cmd-github-orgs})

(defn usage []
  (println "Usage: bb scripts/detect-forges.clj <command> [args…]")
  (println)
  (println "Commands:")
  (println "  scan [recheck] [github-orgs] [COUNTRY…|TARGET…] |")
  (println "  catalogs [COUNTRY…] | github-orgs")
  (println)
  (println "Environment variables: SWH_TOKEN, PARALLEL=N, DOH_URL"))

;; Run only as a script, not when loaded from another namespace.
(when (= *file* (System/getProperty "babashka.file"))
  (dispatch commands usage *command-line-args*))

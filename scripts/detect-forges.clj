#!/usr/bin/env bb
;; detect-forges -- government forges and source-code catalogs (Babashka).
;;
;; Feeds the "Government source-code catalogs" section of
;; swh-sopc-data-sources and the SWH archival-coverage checks. This is a
;; SIDE tool: it shares the harvested data of this repository but serves
;; the Software Heritage catalog goal, not the domain-regex goal --
;; hence its extraction out of pipeline.clj (2026-08-17).
;;
;; Usage: bb scripts/detect-forges.clj <command> [args…]
;;   forges             forge-looking hosts -> data/forge-candidates.csv
;;   forges-swh [github-orgs]
;;                      forge targets unknown to SWH -> data/forge-unknown-swh.csv
;;   forges-probe       re-probe forge-unknown-swh.csv (type + accessibility)
;;   github-orgs        GitHub governments.yml -> data/github-gov-orgs.csv
;;
;; Environment variables:
;;   SWH_TOKEN          authenticated SWH API requests (higher rate limit)
;;   PARALLEL           # concurrent probes (default 8)

(ns detect-forges
  (:require [babashka.http-client :as http]
            [babashka.fs :as fs]
            [babashka.process :as proc]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Helpers (small copies of pipeline.clj's -- the two scripts stay
;; independent so that neither can break the other)
;; ---------------------------------------------------------------------------

(def ua "world-gov-domain-names/0.1 (https://github.com/bzg)")

(defn err [& xs] (binding [*out* *err*] (println (apply str xs))))

(defn single-line [s] (-> (str s) (str/replace #"\s+" " ") str/trim))

(defn read-csv-raw [path]
  (when (fs/exists? path)
    (with-open [r (io/reader (str path))]
      (doall (csv/read-csv r)))))

(defn write-csv-file [path header rows]
  (when-let [parent (fs/parent path)]
    (fs/create-dirs parent))
  (with-open [w (io/writer (str path))]
    (csv/write-csv w (cons header rows))))

(defn country-dirs []
  (->> (fs/list-dir "countries")
       (filter fs/directory?)
       (map (comp str fs/file-name))
       sort
       vec))

(defn parallel [default-n]
  (let [v (System/getenv "PARALLEL")]
    (if (and v (re-matches #"\d+" v)) (max 1 (Integer/parseInt v)) default-n)))

(defn bounded-pmap
  "pmap over coll with at most n threads, preserving order. An exception in
  f propagates to the caller (and cancels the pending tasks): wrap f when
  one failing item must not abort the whole batch."
  [n f coll]
  (let [pool (java.util.concurrent.Executors/newFixedThreadPool (int (max 1 n)))
        g (bound-fn* f)]
    (try
      (->> coll
           (mapv #(.submit pool ^Callable (fn [] (g %))))
           (mapv #(.get ^java.util.concurrent.Future %)))
      (finally
        (.shutdownNow pool)))))

(def http-client
  (http/client (assoc http/default-client-opts
                      :follow-redirects :never
                      :connect-timeout 15000)))

(defn http-get
  "GET returning the body string on HTTP 200, nil otherwise (no retry on
  deterministic 3xx/4xx: the client never follows redirects)."
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

         (and status (<= 300 status 499) (not= 429 status))
         nil

         (< attempt retries)
         (do (Thread/sleep (* attempt 3000))
             (recur (inc attempt)))

         :else nil)))))

;; ---------------------------------------------------------------------------
;; Harvest scan -- forge-looking hostnames
;; ---------------------------------------------------------------------------

(def forge-host-pattern
  #"(?i)^(?:git|gitlab|gitea|forgejo|forges?|codes?|source|open-?source|oss|developers?)\.")

(defn cmd-forges
  "Scan every countries/<c>/subdomains.csv for hostnames that look like a
  software forge or source-code catalog (git.*, gitlab.*, forge.*, code.*…)
  and write data/forge-candidates.csv. These are leads for the
  'Government source-code catalogs' section of swh-sopc-data-sources."
  [_]
  (let [rows (->> (for [c (country-dirs)
                        :let [path (str "countries/" c "/subdomains.csv")]
                        :when (fs/exists? path)
                        [host _parent status] (rest (read-csv-raw path))
                        :when (and host (re-find forge-host-pattern host))]
                    [host c (or status "")])
                  distinct
                  (sort-by (juxt second first)))
        reachable (count (filter #(re-matches #"[23]\d\d" (nth % 2)) rows))]
    (write-csv-file "data/forge-candidates.csv"
                    ["hostname" "country" "http_status"] rows)
    (println (str "Wrote data/forge-candidates.csv (" (count rows)
                  " hosts, " reachable " reachable)"))))

;; ---------------------------------------------------------------------------
;; SWH archive coverage
;; ---------------------------------------------------------------------------

(def swh-search-endpoint "https://archive.softwareheritage.org/api/1/origin/search/")

(defn- swh-get
  "GET the SWH origin-search API for pattern (at most limit results).
  Returns the response map (never throws), nil on a network error. Honors
  SWH_TOKEN for authenticated (higher rate limit) requests."
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

(defn swh-origins-count
  "Number of origins the SWH archive knows under host (0 = unknown), or nil
  when the API could not be answered. A 429 waits the rate-limit window
  out and retries; an exhausted quota triggers a pre-emptive pause."
  [host]
  (loop [attempt 1]
    (let [resp      (swh-get host 10)
          status    (:status resp)
          remaining (some-> (get-in resp [:headers "x-ratelimit-remaining"])
                            parse-long)]
      (cond
        (= 200 status)
        (let [n (try (count (json/parse-string (:body resp)))
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

(def known-forges-file "data/known-forges.csv")

(defn swh-auth-check!
  "When SWH_TOKEN is set, make one authenticated request and fail fast when
  the API rejects the token (an expired or revoked offline token gives HTTP
  403). Returns true when the sweep may proceed (with or without token)."
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
;; Forge probing (type + accessibility)
;; ---------------------------------------------------------------------------

;; Homepage markers checked in order; the first match wins. Forgejo before
;; Gitea (a Forgejo page mentions both), Gitea/Gogs before GitLab (their
;; pages never embed GitLab assets), cgit before gitweb.
(def forge-type-markers
  [["forgejo"     #"(?i)forgejo"]
   ["gitea"       #"(?i)content=\"Gitea|powered by gitea|href=\"https://gitea\.io"]
   ["gogs"        #"(?i)content=\"Gogs|powered by gogs"]
   ["gitlab"      #"(?i)content=\"GitLab\"|GitLab (?:Community|Enterprise) Edition|users/sign_in|users/auth/|gitlab-\w|/assets/webpack/"]
   ["cgit"        #"(?i)id='cgit'|class='cgit|cgit v\d"]
   ["gitweb"      #"(?i)gitweb"]
   ["gerrit"      #"(?i)Gerrit Code Review"]
   ["bonobo"      #"(?i)Bonobo Git Server|href=\"/Home/LogOn\""]
   ["phabricator" #"(?i)phabricator|phorge"]
   ["tuleap"      #"(?i)tuleap"]
   ["fusionforge" #"(?i)fusionforge"]
   ["redmine"     #"(?i)redmine"]
   ["rhodecode"   #"(?i)rhodecode"]
   ["kallithea"   #"(?i)kallithea"]
   ["gitbucket"   #"(?i)gitbucket"]
   ["bitbucket"   #"(?i)bitbucket"]
   ["allura"      #"(?i)Apache Allura"]
   ["pagure"      #"(?i)pagure"]
   ["trac"        #"(?i)powered by trac"]])

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

(defn- forge-type
  "'github' when the target or the final URL lives on github.com, else the
  first forge-type-markers match on the homepage, else 'unknown'."
  [target final-url body]
  (cond
    (or (str/starts-with? target "github.com/")
        (str/starts-with? (str final-url) "https://github.com/")) "github"
    (str/blank? body)                                             "unknown"
    :else (or (first-matching forge-type-markers body)            "unknown")))

(defn- forge-note
  "Short diagnostic for a probe: curl error, WAF or default page, or a
  redirect that left the target's host. Empty string otherwise."
  [target final-url body exit]
  (or (when-not (zero? exit)
        (get curl-exit-notes exit (str "curl exit " exit)))
      (first-matching page-note-markers body)
      (let [[_ host] (re-find #"^https?://([^/]+)" (str final-url))]
        (when (and host (not= host (first (str/split target #"/"))))
          ;; drop the query string: SSO redirects carry volatile state/nonce
          ;; parameters that would churn the CSV on every run
          (str "redirects to " (str/replace final-url #"\?.*" ""))))
      ""))

(defn probe-forge!
  "GET https://target/ with curl (-k: government forges often sit behind
  self-signed certificates; -L: the landing page usually redirects) and
  sniff the forge software from the final page. Returns {:type :status
  :note}: :type from forge-type, :status the final HTTP code as a string
  ('000' when no response came back) and :note from forge-note."
  [target]
  (let [url (str "https://" target (when-not (str/includes? target "/") "/"))
        tmp (fs/create-temp-file)]
    (try
      (let [{:keys [exit out]}
            (try (proc/sh {:continue true}
                          "curl" "-ksL" "--max-time" "25" "--connect-timeout" "10"
                          "-A" ua "-o" (str tmp)
                          "-w" "%{http_code}\t%{url_effective}" url)
                 (catch Exception _ {:exit 1 :out ""}))
            [code final-url] (str/split (str/trim (str out)) #"\t" 2)
            body (let [s (try (slurp (fs/file tmp)) (catch Exception _ ""))]
                   ;; cap what the marker regexes have to scan
                   (subs s 0 (min (count s) 300000)))]
        {:type   (forge-type target final-url body)
         :status (if (str/blank? code) "000" code)
         :note   (single-line (forge-note target final-url body exit))})
      (finally (fs/delete-if-exists tmp)))))

(def forge-unknown-header
  ["target" "country" "kind" "source" "forge_type" "http_status" "note"])

(defn probe-forge-rows
  "Probe each [target country kind source …] row and return
  [target country kind source forge_type http_status note] rows, in order."
  [rows]
  (bounded-pmap (parallel 8)
                (fn [[target country kind source]]
                  (let [{:keys [type status note]}
                        (try (probe-forge! target)
                             (catch Exception e
                               {:type "unknown" :status "000"
                                :note (str "probe error: " (single-line (ex-message e)))}))]
                    (err (str "  " target " -> " type " (HTTP " status
                              (when-not (str/blank? note) (str ", " note)) ")"))
                    [target country kind source type status note]))
                rows))

(defn cmd-forges-probe
  "Re-probe the targets of data/forge-unknown-swh.csv and refresh the
  forge_type/http_status/note columns in place, without touching the SWH
  API. Useful to re-check forge accessibility between two forges-swh runs."
  [_args]
  (let [rows (rest (read-csv-raw "data/forge-unknown-swh.csv"))]
    (if (empty? rows)
      (err "ERR: data/forge-unknown-swh.csv missing or empty. "
           "Run 'bb forges forges-swh' first")
      (let [probed (probe-forge-rows rows)
            n200   (->> probed (filter #(= "200" (nth % 5))) count)]
        (write-csv-file "data/forge-unknown-swh.csv" forge-unknown-header probed)
        (println (str "Wrote data/forge-unknown-swh.csv (" (count probed)
                      " targets probed, " n200 " answering 200)"))))))

(defn cmd-forges-swh
  "Check forge targets against the Software Heritage archive (origin search
  API) and write the ones SWH does not know yet to
  data/forge-unknown-swh.csv, along with the forge software (sniffed from
  the homepage) and its HTTP accessibility (probe-forge!).
  Targets are the harvested forge-looking hosts
  (data/forge-candidates.csv) plus the curated entries of
  data/known-forges.csv with kind 'forge' or 'github-org' (a 'catalog' only
  points at code hosted elsewhere, so there is nothing to search for).
  With the 'github-orgs' argument, every organization of
  data/github-gov-orgs.csv is checked too -- slow anonymously, set
  SWH_TOKEN. Anonymous API rate limits apply in all cases."
  [args]
  (let [harvest (when (fs/exists? "data/forge-candidates.csv")
                  (for [[host country _status]
                        (rest (read-csv-raw "data/forge-candidates.csv"))]
                    [host country "forge" "harvest"]))
        curated (when (fs/exists? known-forges-file)
                  (for [[target country kind _label]
                        (rest (read-csv-raw known-forges-file))
                        :when (contains? #{"forge" "github-org"} kind)]
                    [target country kind "curated"]))
        gh-orgs (when (some #{"github-orgs"} args)
                  (if (fs/exists? "data/github-gov-orgs.csv")
                    (for [[org group] (rest (read-csv-raw "data/github-gov-orgs.csv"))]
                      [(str "github.com/" org) group "github-org" "governments.yml"])
                    (do (err "ERR: data/github-gov-orgs.csv missing. "
                             "Run 'bb forges github-orgs' first")
                        nil)))
        targets (->> (concat harvest curated gh-orgs)
                     (reduce (fn [m [target :as row]]
                               (if (contains? m target) m (assoc m target row)))
                             {})
                     vals
                     (sort-by first))]
    (cond
      (empty? targets)
      (err "ERR: no targets. Run 'bb forges forges' first")

      (not (swh-auth-check!))
      (err "ERR: aborting (unset SWH_TOKEN to run anonymously, slower)")

      :else
      (let [checked (doall
                      (for [[target country kind source] targets]
                        (let [n (swh-origins-count target)]
                          (err (str "  " target " -> "
                                    (cond (nil? n) "API error"
                                          (zero? n) "UNKNOWN to SWH"
                                          ;; the query asks limit=10: the
                                          ;; count is capped, not exact
                                          (>= n 10) "10+ origin(s)"
                                          :else (str n " origin(s)"))))
                          (Thread/sleep 500)
                          [target country kind source n])))
            unknown (filter #(and (some? (nth % 4)) (zero? (nth % 4))) checked)
            errors  (filter #(nil? (nth % 4)) checked)]
        (err (str "  probing " (count unknown)
                  " unknown targets (forge type + accessibility)"))
        (write-csv-file "data/forge-unknown-swh.csv" forge-unknown-header
                        (probe-forge-rows unknown))
        (println (str "Wrote data/forge-unknown-swh.csv (" (count unknown)
                      " of " (count targets) " targets unknown to SWH"
                      (when (seq errors)
                        (str "; " (count errors) " API errors, not listed"))
                      ")"))))))

;; ---------------------------------------------------------------------------
;; GitHub governments.yml
;; ---------------------------------------------------------------------------

(def governments-yml-url
  "https://raw.githubusercontent.com/github/government.github.com/gh-pages/_data/governments.yml")

(defn cmd-github-orgs
  "Fetch GitHub's community-maintained list of government organizations
  (governments.yml, behind government.github.com/community) and write it as
  data/github-gov-orgs.csv (org,group). Groups are the file's own headings:
  mostly countries, sometimes regions or programs. Feed the result to
  'forges-swh github-orgs' to spot orgs the SWH archive does not know."
  [_]
  (if-let [body (http-get governments-yml-url {:accept "text/plain"})]
    (let [data (yaml/parse-string body :keywords false)
          rows (for [[group orgs] data, org orgs] [(str org) (str group)])]
      (write-csv-file "data/github-gov-orgs.csv" ["org" "group"] rows)
      (println (str "Wrote data/github-gov-orgs.csv (" (count rows)
                    " orgs in " (count data) " groups)")))
    (err "ERR: could not fetch " governments-yml-url)))

;; ---------------------------------------------------------------------------
;; Dispatcher
;; ---------------------------------------------------------------------------

(def commands
  {"forges"       cmd-forges
   "forges-swh"   cmd-forges-swh
   "forges-probe" cmd-forges-probe
   "github-orgs"  cmd-github-orgs})

(defn usage []
  (println "Usage: bb scripts/detect-forges.clj <command> [args…]")
  (println)
  (println "Commands:")
  (println "  forges | forges-swh [github-orgs] | forges-probe | github-orgs")
  (println)
  (println "Environment variables: SWH_TOKEN, PARALLEL=N"))

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

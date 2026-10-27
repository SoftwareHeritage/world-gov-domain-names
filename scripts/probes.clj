(require '[babashka.pods :as pods])
;; Loaded here rather than declared in bb.edn, which would download it
;; for every command: only the commands that probe or fetch need it.
(pods/load-pod 'huahaiy/datalevin "1.2.8")

(ns probes
  "The volatile state of the pipeline, kept in the local Datalevin
  database data/.db (git-ignored) rather than in the CSV files, one
  entity per :host/name:
  - :host/http-status (the HTTP code or a one-line error) and :host/mx
    (\"prio host; …\", \"none\" or \"dig error\"), each with its date
    (:host/http-status-at, :host/mx-at), absent until probed;
  - :fetch/result (\"ok\" or \"fail\") and :fetch/at, the last crt.sh
    fetch of a harvest root.
  MAX_AGE=<days> makes a probe or a fetch older than that, or undated,
  due again. Query it from a
  bb REPL with
  (d/q '[:find ?n :where [?e :host/name ?n] [?e :host/http-status \"200\"]]
       (d/db (d/get-conn \"data/.db\")))."
  (:require [pod.huahaiy.datalevin :as d]
            [common :refer [env-int]]
            [clojure.string :as str]))

(def db-dir "data/.db")

(def schema
  {:host/name           {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :host/http-status    {:db/valueType :db.type/string}
   :host/http-status-at {:db/valueType :db.type/instant}
   :host/mx             {:db/valueType :db.type/string}
   :host/mx-at          {:db/valueType :db.type/instant}
   :fetch/result        {:db/valueType :db.type/string}
   :fetch/at            {:db/valueType :db.type/instant}})

(def ^:private probe-attrs [:host/http-status :host/mx])
(def ^:private probe-at-attrs [:host/http-status-at :host/mx-at])

(defn- with-conn [f]
  (let [conn (d/get-conn db-dir schema)]
    (try (f conn) (finally (d/close conn)))))

(defn- transact! [tx]
  (when (seq tx)
    (with-conn (fn [conn] (doseq [batch (partition-all 10000 tx)] (d/transact! conn (vec batch)))))))

(defn- attr-map
  "{name value} of the entities having attr."
  [conn attr]
  (into {} (d/q '[:find ?n ?v :in $ ?a :where [?e :host/name ?n] [?e ?a ?v]]
                (d/db conn) attr)))

(defn- due?
  "True when a value dated at is older than MAX_AGE days, or undated;
  never without MAX_AGE."
  [at]
  (boolean
    (when-let [days (env-int "MAX_AGE" nil)]
      (or (nil? at)
          (< (.getTime ^java.util.Date at)
             (- (System/currentTimeMillis) (* days 86400000)))))))

;; ---------------------------------------------------------------------------
;; HTTPS and MX probes
;; ---------------------------------------------------------------------------

;; {:values {host [http_status mx]} :at {host [http-at mx-at]}}, read
;; once per run, kept in step by save!.
(def ^:private cache (atom nil))

(defn- probes-state []
  (or @cache
      (reset! cache
              (with-conn
                (fn [conn]
                  (let [[st mx]   (map #(attr-map conn %) probe-attrs)
                        [sta mxa] (map #(attr-map conn %) probe-at-attrs)
                        hosts     (into (set (keys st)) (keys mx))]
                    {:values (into {} (for [h hosts] [h [(st h "") (mx h "")]]))
                     :at     (into {} (for [h hosts] [h [(sta h) (mxa h)]]))}))))))

(defn all-probes
  "{host [http_status mx]} of every probed host."
  []
  (:values (probes-state)))

(defn probes-of
  "{host [http_status mx]} of hosts, [\"\" \"\"] for a host not probed yet."
  [hosts]
  (let [all (all-probes)]
    (into {} (for [h hosts] [h (get all h ["" ""])]))))

(defn stale?
  "True when probe idx (0 = HTTPS status, 1 = MX) of host is older than
  MAX_AGE days, or undated."
  [host idx]
  (due? (get-in (probes-state) [:at host idx])))

(defn save!
  "Store probe idx (0 = HTTPS status, 1 = MX) of a {host value} map,
  dated now, blank values left out."
  [idx values]
  (let [now    (java.util.Date.)
        values (remove (comp str/blank? val) values)]
    (transact! (for [[h v] values]
                 {:host/name h (probe-attrs idx) v (probe-at-attrs idx) now}))
    (when @cache
      (swap! cache (fn [state]
                     (reduce (fn [s [h v]]
                               (-> s
                                   (update-in [:values h] #(assoc (or % ["" ""]) idx v))
                                   (update-in [:at h] #(assoc (or % [nil nil]) idx now))))
                             state values))))
    nil))

;; ---------------------------------------------------------------------------
;; crt.sh fetches
;; ---------------------------------------------------------------------------

(defn save-fetches!
  "Store the outcome (:ok or :fail) of a {root outcome} map, dated now."
  [outcomes]
  (let [now (java.util.Date.)]
    (transact! (for [[root outcome] outcomes]
                 {:host/name root :fetch/result (name outcome) :fetch/at now})))
  nil)

(defn failed-fetches
  "Roots whose last fetch failed, sorted."
  []
  (sort (for [[root result] (with-conn #(attr-map % :fetch/result))
              :when (= result "fail")]
          root)))

(defn due-fetches
  "The roots whose last fetch is older than MAX_AGE days, or missing;
  every root without MAX_AGE."
  [roots]
  (if (env-int "MAX_AGE" nil)
    (let [at (with-conn #(attr-map % :fetch/at))]
      (filter #(due? (at %)) roots))
    roots))

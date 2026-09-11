(ns indegree
  "Link-graph in-degree from the eu-plus-government-scans crawl: for each
  host, the number of distinct public-sector domains of the same country
  linking to it, written to countries/<c>/sources/linkgraph/indegree.csv.
  pipeline.clj dispatches."
  (:require [common :refer :all]
            [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def linkgraph-min-indegree
  "Minimum number of distinct linking domains for a host to enter
  proposed.csv."
  3)

(def linkgraph-base-url
  ;; Data published by https://github.com/mgifford/eu-plus-government-scans:
  ;; a crawl of government sites (EU + neighbours) recording outbound links
  ;; and asset dependencies. gov-domains.json maps hostname -> country;
  ;; relationships.jsonl holds one edge per line.
  "https://raw.githubusercontent.com/mgifford/eu-plus-government-scans/main/docs/data/")

(def linkgraph-cache "/tmp/linkgraph")

(defn- linkgraph-fetch!
  "Download one crawl data file into the /tmp cache and return its path,
  nil on failure. FORCE=1 re-downloads."
  [file]
  (let [path (str linkgraph-cache "/" file)
        tmp  (str path ".tmp")]
    (if (and (fs/exists? path) (not force?))
      path
      (do (ensure-dir linkgraph-cache)
          (println (str "Downloading " file "..."))
          ;; download to a .tmp then rename: an interrupted download must
          ;; not leave a truncated file that the cache would then serve
          (if (curl-download! (str linkgraph-base-url file) tmp {:timeout 600})
            (do (fs/move tmp path {:replace-existing true})
                path)
            (do (err "ERR: could not fetch " linkgraph-base-url file)
                (fs/delete-if-exists tmp)
                nil))))))

(def linkgraph-country-aliases
  "Crawler country names whose normalize-name does not equal our slug."
  {"unitedkingdomuk"  "unitedkingdom"
   "republicofcyprus" "cyprus"})

(defn- linkgraph-suffix->dir
  "Build {domain-suffix country_dir} from the crawler's inventory, one
  entry per dotted suffix (2+ labels) of each hostname; drop suffixes
  claimed by two countries. Nil when the inventory is unavailable."
  []
  (when-let [path (linkgraph-fetch! "gov-domains.json")]
    (let [by-slug   @slug->country-dir
          name->dir (fn [n] (let [n (normalize-name n)]
                              (by-slug (get linkgraph-country-aliases n n))))
          domains   (get (json/parse-string (slurp path)) "domains")
          mapping   (into {} (keep (fn [n] (when-let [d (name->dir n)] [n d])))
                          (distinct (vals domains)))]
      (doseq [n (remove mapping (distinct (vals domains)))]
        (err "WARN: unmapped country in link graph: " n))
      (->> domains
           (reduce (fn [m [host cname]]
                     (if-let [dir (mapping cname)]
                       (let [parts (str/split (str/lower-case host) #"\.")]
                         (reduce (fn [m i]
                                   (update m (str/join "." (subvec parts i))
                                           (fnil conj #{}) dir))
                                 m (range (dec (count parts)))))
                       m))
                   {})
           (keep (fn [[suf dirs]] (when (= 1 (count dirs)) [suf (first dirs)])))
           (into {})))))

(defn- linkgraph-lookup
  "Return the country_dir of the longest suffix of domain in suffix->dir."
  [suffix->dir domain]
  (let [parts (str/split domain #"\.")]
    (some (fn [i] (suffix->dir (str/join "." (subvec parts i))))
          (range (dec (count parts))))))

(defn cmd-indegree
  "Compute each government domain's in-degree per crawled country:
  editorial links and form destinations feed indegree, other dependencies
  (scripts, stylesheets, media) feed indegree_tech; the crawler's own
  target_category is ignored. Write
  countries/<c>/sources/linkgraph/indegree.csv
  (hostname,indegree,indegree_tech). Optional args restrict to the given
  country_dirs; return 1 when the crawl data is unavailable."
  [args]
  (let [suffix->dir (linkgraph-suffix->dir)
        rel-path    (and suffix->dir (linkgraph-fetch! "relationships.jsonl"))]
    (if-not rel-path
      (do (err "ERR: link-graph data unavailable") 1)
      (let [only       (when (seq args) (set args))
            editorial? #{"editorial_link" "form_destination"}
            acc        ;; {country_dir {target {:ed #{src...} :tech #{src...}}}}
            (with-open [r (io/reader rel-path)]
              (reduce
               (fn [acc line]
                 (let [row (json/parse-string line)
                       src (get row "source_domain")
                       tgt (get row "target_domain")
                       dir (and (seq src) (seq tgt) (not= src tgt)
                                (linkgraph-lookup suffix->dir src))]
                   (if (and dir
                            (or (nil? only) (contains? only dir))
                            (= dir (linkgraph-lookup suffix->dir tgt)))
                     (let [k (if (editorial? (get row "relationship_type"))
                               :ed :tech)]
                       (update-in acc [dir tgt k] (fnil conj #{}) src))
                     acc)))
               {} (line-seq r)))]
        (doseq [[dir targets] (sort-by key acc)]
          (let [rows   (->> targets
                            (map (fn [[tgt {:keys [ed tech]}]]
                                   [tgt (count ed) (count tech)]))
                            (sort-by (juxt #(- (nth % 1)) first)))
                strong (count (filter #(>= (nth % 1) linkgraph-min-indegree)
                                      rows))]
            (write-csv-file (country-src dir "linkgraph" "indegree.csv")
                            ["hostname" "indegree" "indegree_tech"]
                            (map (fn [[t e c]] [t (str e) (str c)]) rows))
            (println (str dir ": " (count rows) " linked domains ("
                          strong " with indegree >= "
                          linkgraph-min-indegree ")"))))
        (println "Run 'bb pipeline propose' to fold them into proposed.csv.")
        0))))

#!/usr/bin/env bash
# ============================================================================
#  world-gov-domain-names -- point d'entrée unique du pipeline
# ============================================================================
#
# Sous-commandes principales :
#   collect        crt.sh harvest + normalize + probe + collect_200
#   enrich         wikidata (durci) + iana/cia_factbook/un_desa/oecd (parallèle)
#   report         cross_check (score + rapport par pays)
#   all            collect + enrich + report
#
# Sous-commandes ciblées (granularité fine) :
#   fetch [DOM…]            crt.sh fetch (1 ou plusieurs domaines)
#   retry [DOM…]            relance des FAIL
#   normalize               normalise tous les subdomains.csv
#   probe [DOM…]            sonde HTTPS HEAD les lignes au statut vide
#   collect-200             agrège all_200_domains.csv
#   un-status [PAYS…]       régénère countries/<c>/un_status
#   wikidata [QID:PAYS…]    fetch + diff Wikidata (admin centrale)
#   iana [PAYS…]            registre ccTLD IANA
#   cia [PAYS…]             section Government depuis factbook.json
#   un-desa [PAYS…]         portail national + EGDI UN/DESA
#   oecd [PAYS…]            flag d'appartenance OECD
#   cross-check [PAYS…]     rapport markdown + candidates.csv scoré
#   build-qid               (re)génère data/country_qid.csv depuis Wikidata
#                           (à lancer si on ajoute un pays au CSV maître)
#
# Variables d'environnement :
#   FORCE=1         force la réécriture des sorties existantes
#   PARALLEL        nombre de requêtes simultanées (selon la phase)
#   TIMEOUT         timeout curl en secondes (probe)
#
# Principe d'isolation : les phases d'enrichissement n'écrivent QUE dans
# countries/<c>/<source>/. La promotion d'un candidat vers
# countries/<c>/<domain>/ reste manuelle.
# ============================================================================

set -u
cd "$(dirname "$0")/.."

UA='world-gov-domain-names/0.1 (https://github.com/bzg)'
FORCE="${FORCE:-0}"

# ============================================================================
#  Helpers communs (ex-_lib_fetch.sh)
# ============================================================================

# Fusionne un CSV existant avec une liste fraîche de sous-domaines.
# Préserve les statuts non vides en cas de doublon. Trie ASCII (LC_ALL=C).
#   $1 = CSV existant ; $2 = fichier de noms bruts.
merge_crt_csv() {
  local existing="$1"
  local fresh="$2"
  echo "subdomain,http_status"
  {
    [ -f "$existing" ] && tail -n +2 "$existing"
    sed 's/$/,/' "$fresh"
  } | awk -F, '
      {
        dom = $1
        status = ""
        if (NF > 1) {
          status = $2
          for (i = 3; i <= NF; i++) status = status FS $i
        }
        if (!(dom in s) || (s[dom] == "" && status != "")) s[dom] = status
      }
      END { for (k in s) print k "," s[k] }
    ' | LC_ALL=C sort
}

csv_data_lines() {
  local f="$1" n
  [ -f "$f" ] || { echo 0; return; }
  n=$(wc -l < "$f")
  [ "$n" -le 1 ] && { echo 0; return; }
  echo $((n - 1))
}

# Résout une liste de noms de domaines en chemins countries/<c>/<d>/.
resolve_dirs() {
  if [ $# -eq 0 ]; then
    find countries -mindepth 2 -maxdepth 2 -type d \
      -not -name wikidata -not -name iana -not -name un_desa \
      -not -name cia_factbook -not -name oecd
    return
  fi
  local d m
  local -a matches
  for d in "$@"; do
    matches=(countries/*/"$d")
    if [ ! -d "${matches[0]}" ]; then
      echo "ERR: aucun pays ne contient '$d' sous countries/*/." >&2
      continue
    fi
    for m in "${matches[@]}"; do printf '%s\n' "$m"; done
  done
}

# Itère sur les country_dir : ceux passés en argument, ou tous countries/*.
iter_country_dirs() {
  local fn="$1"; shift
  if [ $# -gt 0 ]; then
    local c; for c in "$@"; do "$fn" "$c"; done
  else
    find countries -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
      | LC_ALL=C sort \
      | while read -r c; do "$fn" "$c"; done
  fi
}

country_slug() {
  printf '%s' "$1" | awk -F_ '{
    s=""; for (i=2; i<=NF; i++) s = s $i
    print tolower(s)
  }'
}

country_slug_table() {
  find countries -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
    | awk -F_ '{
        s=""; for (i=2; i<=NF; i++) s = s $i
        print tolower(s) "\t" $0
      }'
}

extract_host() {
  printf '%s' "$1" \
    | sed -E 's|^https?://||; s|^www\.||; s|/.*$||; s|:.*$||' \
    | tr '[:upper:]' '[:lower:]'
}

un_status_for() {
  local f="countries/$1/un_status"
  [ -f "$f" ] && head -1 "$f" | tr -d '\r\n '
}

csv_quote() { printf '"%s"' "${1//\"/\"\"}"; }

# ============================================================================
#  Phase 0 -- set_un_status
# ============================================================================

cmd_un_status() {
  declare -A STATUS_BY_SLUG
  while IFS=, read -r country _ _ _ _ status; do
    [ "$country" = "Country" ] && continue
    status=${status//$'\r'/}
    local norm
    norm=$(echo "$country" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
    STATUS_BY_SLUG["$norm"]="$status"
  done < <(tr -d '\r' < data/world-governments.csv)

  _set_one_status() {
    local country_dir="$1" slug status
    slug=$(country_slug "$country_dir")
    status="${STATUS_BY_SLUG[$slug]:-}"
    if [ -z "$status" ]; then
      echo "  [$country_dir] aucun mapping (slug '$slug' absent de world-governments.csv)" >&2
      return 1
    fi
    printf '%s\n' "$status" > "countries/$country_dir/un_status"
    echo "=== $country_dir : $status"
  }

  iter_country_dirs _set_one_status "$@"
}

# ============================================================================
#  Phase 1 -- fetch / retry (crt.sh)
# ============================================================================

cmd_fetch() {
  local parallel="${PARALLEL:-4}"
  export -f _fetch_one merge_crt_csv csv_data_lines

  _fetch_one() {
    local dir="$1" domain out tmp http_code names
    domain=$(basename "$dir")
    out="$dir/subdomains.csv"
    tmp=$(mktemp)
    http_code=$(curl -s -o "$tmp" -w '%{http_code}' --max-time 120 \
      "https://crt.sh/?q=%25.${domain}&output=json")
    if [ "$http_code" = "200" ] && [ -s "$tmp" ]; then
      names="$tmp.names"
      jq -r '.[].name_value' < "$tmp" 2>/dev/null | sed 's/^\*\.//' > "$names"
      merge_crt_csv "$out" "$names" > "$tmp.csv" && mv "$tmp.csv" "$out"
      rm -f "$names"
      echo "OK   $domain ($(csv_data_lines "$out") lignes)"
    else
      echo "FAIL $domain (http=$http_code)"
      [ -f "$out" ] || echo "subdomain,http_status" > "$out"
    fi
    rm -f "$tmp"
  }
  export -f _fetch_one

  resolve_dirs "$@" | xargs -I{} -P "$parallel" bash -c '_fetch_one "$@"' _ {}
}

cmd_retry() {
  local parallel="${PARALLEL:-2}"
  export -f merge_crt_csv csv_data_lines

  _retry_one() {
    local dir="$1" domain out tmp http_code attempt names
    domain=$(basename "$dir")
    out="$dir/subdomains.csv"
    http_code=""
    for attempt in 1 2 3; do
      tmp=$(mktemp)
      http_code=$(curl -s -o "$tmp" -w '%{http_code}' --max-time 180 \
        "https://crt.sh/?q=%25.${domain}&output=json")
      if [ "$http_code" = "200" ] && [ -s "$tmp" ]; then
        names="$tmp.names"
        jq -r '.[].name_value' < "$tmp" 2>/dev/null | sed 's/^\*\.//' > "$names"
        merge_crt_csv "$out" "$names" > "$tmp.csv" && mv "$tmp.csv" "$out"
        rm -f "$tmp" "$names"
        echo "OK   $domain ($(csv_data_lines "$out") lignes) [try=$attempt]"
        return 0
      fi
      rm -f "$tmp"
      sleep $((attempt * 5))
    done
    echo "FAIL $domain (http=$http_code)"
    return 1
  }
  export -f _retry_one

  local -a domains
  if [ $# -gt 0 ]; then
    domains=("$@")
  else
    mapfile -t domains < <(grep '^FAIL' /tmp/fetch_subdomains.log 2>/dev/null | awk '{print $2}')
    if [ ${#domains[@]} -eq 0 ]; then
      echo "Aucun FAIL dans /tmp/fetch_subdomains.log (et pas d'argument fourni)." >&2
      return 1
    fi
  fi
  resolve_dirs "${domains[@]}" | xargs -I{} -P "$parallel" bash -c '_retry_one "$@"' _ {}
}

# ============================================================================
#  Phase 2 -- normalize
# ============================================================================

cmd_normalize() {
  for csv in countries/*/*/subdomains.csv; do
    [ -f "$csv" ] || continue
    local name=$(basename "$(dirname "$csv")")
    case "$name" in wikidata|iana|un_desa|cia_factbook|oecd) continue;; esac
    local before after tmp
    before=$(csv_data_lines "$csv")
    tmp=$(mktemp)
    {
      echo "subdomain,http_status"
      tail -n +2 "$csv" | awk -F, '
        {
          dom = $1; status = ""
          if (NF > 1) {
            status = $2
            for (i = 3; i <= NF; i++) status = status FS $i
          }
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", dom)
          if (dom == "") next
          dom = tolower(dom)
          sub(/^\*\./, "", dom)
          if (dom ~ /\*/) next
          if (dom ~ /@/) next
          if (!(dom in s) || (s[dom] == "" && status != "")) s[dom] = status
        }
        END { for (k in s) print k "," s[k] }
      ' | LC_ALL=C sort
    } > "$tmp" && mv "$tmp" "$csv"
    after=$(csv_data_lines "$csv")
    echo "[$name] $before -> $after"
  done
}

# ============================================================================
#  Phase 3 -- probe
# ============================================================================

cmd_probe() {
  local parallel="${PARALLEL:-50}"
  local timeout="${TIMEOUT:-5}"
  export TIMEOUT="$timeout"

  _probe_one() {
    local sub="$1" result code err status status_csv
    result=$(curl -s -o /dev/null -I \
      -w '%{http_code}|%{errormsg}' \
      --max-time "$TIMEOUT" --connect-timeout "$TIMEOUT" \
      "https://${sub}/" 2>/dev/null)
    code="${result%%|*}"; err="${result#*|}"
    [ "$code" = "000" ] && status="${err:-unknown error}" || status="$code"
    status_csv="\"${status//\"/\"\"}\""
    printf '%s,%s\n' "$sub" "$status_csv"
  }
  export -f _probe_one

  _probe_domain() {
    local dir="$1" csv name to_probe count tmp
    csv="$dir/subdomains.csv"
    name=$(basename "$dir")
    case "$name" in wikidata|iana|un_desa|cia_factbook|oecd) return 0;; esac
    [ -f "$csv" ] || return 0

    to_probe=$(tail -n +2 "$csv" | awk -F, '$2 == "" && $1 != "" { print $1 }')
    if [ -z "$to_probe" ]; then
      echo "[$name] aucun sous-domaine vide à sonder"
      return 0
    fi
    count=$(printf '%s\n' "$to_probe" | wc -l)
    echo "[$name] $count sous-domaines à sonder"

    tmp=$(mktemp)
    {
      echo "subdomain,http_status"
      {
        tail -n +2 "$csv" | awk -F, '$2 != ""'
        printf '%s\n' "$to_probe" | xargs -I{} -P "$parallel" \
          bash -c '_probe_one "$@"' _ {}
      } | LC_ALL=C sort
    } > "$tmp" && mv "$tmp" "$csv"
  }

  if [ $# -gt 0 ]; then
    local d; for d in "$@"; do
      local -a matches=(countries/*/"$d")
      if [ ! -d "${matches[0]}" ]; then
        echo "ERR: aucun pays ne contient le domaine '$d'" >&2
        continue
      fi
      local m; for m in "${matches[@]}"; do _probe_domain "$m"; done
    done
  else
    local dir; for dir in countries/*/*/; do _probe_domain "${dir%/}"; done
  fi
}

# ============================================================================
#  Phase 4 -- collect_200 (avec auto-trigger un-status)
# ============================================================================

cmd_collect_200() {
  local out="all_200_domains.csv"

  local need_un_status=0 dir
  for dir in countries/*/; do
    [ -f "${dir}un_status" ] || { need_un_status=1; break; }
  done
  if [ "$need_un_status" = 1 ]; then
    echo "Markers un_status manquants → régénération" >&2
    cmd_un_status
  fi

  declare -A UN_STATUS
  for dir in countries/*/; do
    local c=$(basename "$dir")
    [ -f "$dir/un_status" ] && UN_STATUS["$c"]=$(head -1 "$dir/un_status" | tr -d '\r\n ')
  done

  {
    echo "subdomain,parent_domain,country,un_status"
    local csv; for csv in countries/*/*/subdomains.csv; do
      [ -f "$csv" ] || continue
      local parent country status
      parent=$(basename "$(dirname "$csv")")
      country=$(basename "$(dirname "$(dirname "$csv")")")
      status="${UN_STATUS[$country]:-member}"
      awk -F',' -v parent="$parent" -v country="$country" -v status="$status" '
        NR > 1 && /,"200"$/ { print $1 "," parent "," country "," status }
      ' "$csv"
    done | LC_ALL=C sort -u
  } > "$out"

  local total member observer non_un
  total=$(( $(wc -l < "$out") - 1 ))
  member=$(awk -F, 'NR>1 && $4=="member"' "$out" | wc -l)
  observer=$(awk -F, 'NR>1 && $4=="observer"' "$out" | wc -l)
  non_un=$(awk -F, 'NR>1 && $4=="non_un"' "$out" | wc -l)
  echo "Écrit $out ($total sous-domaines HTTP 200)"
  echo "  membres UN : $member ; observateurs : $observer ; non-UN : $non_un"
}

# ============================================================================
#  Phase 5 -- Wikidata (fetch admin centrale + diff)
# ============================================================================

cmd_wikidata() {
  local ENDPOINT='https://query.wikidata.org/sparql'
  # qid:type:strictness (strict = filtres lourds, light = filtres légers)
  local -a CLASSES=(
    "Q192350:ministry:strict"
    "Q11204:parliament:strict"
    "Q193445:central_bank:light"
    "Q35798:constitutional_court:light"
    "Q35749:supreme_court:light"
  )

  _wd_build_query() {
    local class_qid="$1" country_qid="$2" strictness="$3" strict_filters=""
    if [ "$strictness" = "strict" ]; then
      strict_filters=$(cat <<FILTERS
  FILTER NOT EXISTS { ?org wdt:P1001 ?j . FILTER(?j != wd:$country_qid) }
  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q56061 }
  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q10864048 }
  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q13220204 }
  FILTER NOT EXISTS { ?org wdt:P31/wdt:P279* wd:Q1799794 }
FILTERS
)
    fi
    cat <<SPARQL
SELECT DISTINCT ?org ?orgLabel ?website WHERE {
  ?org wdt:P31/wdt:P279* wd:$class_qid ;
       wdt:P17 wd:$country_qid ;
       wdt:P856 ?website .
  FILTER NOT EXISTS { ?org wdt:P576 ?d }
$strict_filters
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
SPARQL
  }

  _wd_run_query() {
    local query="$1" attempt resp http
    for attempt in 1 2 3; do
      resp=$(curl -s -G --max-time 180 -w '\n%{http_code}' \
        --data-urlencode "query=$query" \
        -H "Accept: application/sparql-results+json" \
        -H "User-Agent: $UA" "$ENDPOINT")
      http="${resp##*$'\n'}"; resp="${resp%$'\n'*}"
      if [ "$http" = "200" ] && [ -n "$resp" ]; then
        printf '%s' "$resp"; return 0
      fi
      sleep $((attempt * 5))
    done
    return 1
  }

  _wd_write_missing() {
    local in="$1" out="$2" k host covered line
    local source_dirs_re='^(wikidata|iana|un_desa|cia_factbook|oecd)$'
    mapfile -t KNOWN < <(find countries -mindepth 2 -maxdepth 2 -type d -printf '%f\n' \
      | awk -v re="$source_dirs_re" '$0 !~ re' | LC_ALL=C sort -u)
    {
      echo "type,label,website,hostname"
      tail -n +2 "$in" | while IFS= read -r line; do
        host="${line##*,}"
        [ -z "$host" ] && continue
        covered=0
        for k in "${KNOWN[@]}"; do
          if [ "$host" = "$k" ] || [[ "$host" == *".${k}" ]]; then
            covered=1; break
          fi
        done
        [ "$covered" = 0 ] && printf '%s\n' "$line"
      done
    } > "$out"
  }

  _wd_extract_host() {
    printf '%s' "$1" | awk -F/ '{print tolower($3)}' | sed 's/^www\.//'
  }

  _wd_process() {
    local country_qid="$1" country_dir="$2"
    mkdir -p "countries/$country_dir/wikidata"
    local out="countries/$country_dir/wikidata/central_admin.csv"
    local missing_out="countries/$country_dir/wikidata/missing_domains.csv"

    if [ -f "$out" ] && [ "$FORCE" != "1" ]; then
      echo "=== $country_dir ($country_qid) : SKIP (FORCE=1 pour refaire)"
      _wd_write_missing "$out" "$missing_out"
      return 0
    fi

    echo "=== $country_dir ($country_qid) ==="
    local total=0 tmp entry qid type strictness resp count query
    tmp=$(mktemp)
    {
      echo "type,label,website,hostname"
      for entry in "${CLASSES[@]}"; do
        qid="${entry%%:*}"
        type="${entry#*:}"; type="${type%%:*}"
        strictness="${entry##*:}"
        query=$(_wd_build_query "$qid" "$country_qid" "$strictness")
        if ! resp=$(_wd_run_query "$query"); then
          echo "  [$type] échec après 3 essais" >&2; continue
        fi
        count=$(echo "$resp" | jq -r '.results.bindings | length' 2>/dev/null || echo 0)
        echo "  [$type] $count résultats" >&2
        total=$((total + count))
        echo "$resp" | jq -r --arg t "$type" '
          .results.bindings[] | [$t, .orgLabel.value, .website.value] | @tsv
        ' 2>/dev/null | while IFS=$'\t' read -r t lbl url; do
          local host lbl_csv url_csv
          host=$(_wd_extract_host "$url")
          lbl_csv="\"${lbl//\"/\"\"}\""
          url_csv="\"${url//\"/\"\"}\""
          printf '%s,%s,%s,%s\n' "$t" "$lbl_csv" "$url_csv" "$host"
        done
        sleep 1
      done
    } > "$tmp" && mv "$tmp" "$out"
    echo "  → $out ($total entrées)"

    _wd_write_missing "$out" "$missing_out"
    local missing
    missing=$(($(wc -l < "$missing_out") - 1))
    echo "  → $missing_out ($missing candidats hors couverture)"
  }

  if [ $# -gt 0 ]; then
    local c; for c in "$@"; do _wd_process "${c%%:*}" "${c##*:}"; done
  else
    [ -f data/country_qid.csv ] || {
      echo "ERR: data/country_qid.csv manquant. Lancez 'pipeline.sh build-qid' d'abord" >&2
      return 1
    }
    tail -n +2 data/country_qid.csv | while IFS=, read -r country_dir iso3 qid; do
      _wd_process "$qid" "$country_dir"
    done
  fi
}

# ============================================================================
#  Phase 6 -- IANA
# ============================================================================

cmd_iana() {
  _iana_portal_for() {
    local country_dir="$1" slug
    slug=$(country_slug "$country_dir")
    awk -F, -v target="$slug" '
      NR==1 { next }
      {
        gsub(/^[ \t]+|[ \t]+$/, "", $1)
        s = tolower($1); gsub(/[^a-z0-9]/, "", s)
        if (s == target) { print $4; exit }
      }
    ' data/world-governments.csv
  }

  _iana_fetch_html() {
    local cctld="$1" resp http attempt
    for attempt in 1 2 3; do
      resp=$(curl -s --max-time 30 -w '\n%{http_code}' -H "User-Agent: $UA" \
        "https://www.iana.org/domains/root/db/${cctld}.html")
      http="${resp##*$'\n'}"; resp="${resp%$'\n'*}"
      if [ "$http" = "200" ] && [ -n "$resp" ]; then printf '%s' "$resp"; return 0; fi
      sleep $((attempt * 3))
    done
    return 1
  }

  _iana_extract_field() {
    local html="$1" h2="$2"
    awk -v sec="$h2" '
      $0 ~ "<h2>"sec"</h2>" { found=1; next }
      found && /<b>/ {
        gsub(/^[ \t]*<b>/, ""); sub(/<\/b>.*$/, ""); gsub(/<[^>]+>/, "")
        print; exit
      }
    ' <<< "$html"
  }

  _iana_extract_url() {
    local html="$1" label="$2"
    grep -oE "<b>${label}:</b> <a href=\"[^\"]+\"" <<< "$html" \
      | head -1 | sed -E 's/.*href="([^"]+)".*/\1/'
  }

  _iana_extract_whois() {
    grep -oE 'WHOIS Server:</b>[^<]+' <<< "$1" | head -1 \
      | sed -E 's|.*WHOIS Server:</b>[[:space:]]*||; s|[[:space:]]*<.*||; s|[[:space:]]*$||'
  }

  _iana_process() {
    local country_dir="$1" portal cctld out html manager registry whois
    portal=$(_iana_portal_for "$country_dir")
    [ -z "$portal" ] && { echo "  [$country_dir] aucun portail" >&2; return 1; }
    cctld=$(printf '%s' "$portal" | awk -F. '{print tolower($NF)}')
    [ -z "$cctld" ] || [ "${#cctld}" -lt 2 ] && { echo "  [$country_dir] cctld invalide" >&2; return 1; }

    mkdir -p "countries/$country_dir/iana"
    out="countries/$country_dir/iana/cctld.csv"
    [ -f "$out" ] && [ "$FORCE" != "1" ] && { echo "=== $country_dir (.$cctld) : SKIP"; return 0; }

    echo "=== $country_dir (.$cctld) ==="
    html=$(_iana_fetch_html "$cctld") || { echo "  échec après 3 essais" >&2; return 1; }

    manager=$(_iana_extract_field "$html" "ccTLD Manager")
    [ -z "$manager" ] && manager=$(_iana_extract_field "$html" "Sponsoring Organisation")
    registry=$(_iana_extract_url "$html" "URL for registration services")
    whois=$(_iana_extract_whois "$html")

    {
      echo "cctld,manager,registry_url,whois_server"
      printf '%s,%s,%s,%s\n' ".$cctld" \
        "$(csv_quote "$manager")" "$(csv_quote "$registry")" "$(csv_quote "$whois")"
    } > "$out"
    echo "  → $out (manager: ${manager:-?})"
    sleep 1
  }

  iter_country_dirs _iana_process "$@"
}

# ============================================================================
#  Phase 6 -- CIA Factbook
# ============================================================================

cmd_cia() {
  local MAP_FILE="data/factbook_gec.csv"
  local TREE_CACHE="/tmp/world-gov-factbook-tree.json"
  local TMPDIR_RUN
  TMPDIR_RUN=$(mktemp -d)

  _cia_build_map() {
    if [ -f "$MAP_FILE" ] && [ "$FORCE" != "1" ]; then
      local n; n=$(($(wc -l < "$MAP_FILE") - 1))
      if [ "$n" -lt 150 ]; then
        echo "WARN: $MAP_FILE n'a que $n entrées, reconstruction forcée." >&2
      else
        return 0
      fi
    fi
    echo "Construction du mapping country_dir ↔ GEC factbook…" >&2

    if [ ! -s "$TREE_CACHE" ] || [ "$FORCE" = "1" ]; then
      curl -s --max-time 30 -H "User-Agent: $UA" \
        "https://api.github.com/repos/factbook/factbook.json/git/trees/master?recursive=1" \
        > "$TREE_CACHE"
    fi
    jq -r '.tree[] | select(.path | endswith(".json")) | .path' "$TREE_CACHE" \
      > "$TMPDIR_RUN/tree.txt" 2>/dev/null
    [ -s "$TMPDIR_RUN/tree.txt" ] || { echo "ERR: tree GitHub indisponible" >&2; return 1; }

    curl -s --max-time 30 -H "User-Agent: $UA" \
      "https://raw.githubusercontent.com/factbook/factbook.json/master/SUMMARY.md" \
      | grep -oE '`[a-z]+` [^`]+' \
      | sed -E 's/`([a-z]+)` (.*)$/\1\t\2/' > "$TMPDIR_RUN/pairs.txt"
    [ -s "$TMPDIR_RUN/pairs.txt" ] || { echo "ERR: SUMMARY.md indisponible" >&2; return 1; }

    country_slug_table > "$TMPDIR_RUN/dirs.txt"

    awk -F'\t' '
      NR==FNR { dir[$1]=$2; next }
      {
        gec=$1; name=$2
        gsub(/[[:space:]]+$/, "", name)
        norm=tolower(name); gsub(/[^a-z0-9]/, "", norm)
        if (norm in dir) print dir[norm] "," gec
      }
    ' "$TMPDIR_RUN/dirs.txt" "$TMPDIR_RUN/pairs.txt" > "$TMPDIR_RUN/matched.txt"

    awk -F, '
      NR==FNR { sub(/\.json$/, "", $0); split($0, a, "/"); reg[a[2]]=a[1]; next }
      { print $0 "," reg[$2] }
    ' "$TMPDIR_RUN/tree.txt" "$TMPDIR_RUN/matched.txt" > "$TMPDIR_RUN/with_region.txt"

    {
      echo "country_dir,gec,region"
      cat "$TMPDIR_RUN/with_region.txt"
      [ -f data/factbook_aliases.csv ] && tail -n +2 data/factbook_aliases.csv
    } | awk -F, '!seen[$1]++' > "$MAP_FILE.tmp"
    mv "$MAP_FILE.tmp" "$MAP_FILE"
    local n; n=$(($(wc -l < "$MAP_FILE") - 1))
    echo "  → $MAP_FILE ($n pays mappés)" >&2
  }

  _cia_decode_html() {
    python3 -c 'import html,sys; sys.stdout.write(html.unescape(sys.stdin.read()))' \
      | sed 's|<[^>]\+>||g'
  }

  _cia_extract_field() {
    jq -r "$2 // empty" <<< "$1" 2>/dev/null \
      | _cia_decode_html \
      | tr '\n' ' ' \
      | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
  }

  _cia_process() {
    local country_dir="$1" entry gec region raw out full gov url
    entry=$(awk -F, -v c="$country_dir" 'NR>1 && $1==c {print $2","$3; exit}' "$MAP_FILE")
    [ -z "$entry" ] && { echo "  [$country_dir] aucun GEC factbook" >&2; return 1; }
    gec="${entry%%,*}"; region="${entry##*,}"

    mkdir -p "countries/$country_dir/cia_factbook"
    raw="countries/$country_dir/cia_factbook/government.json"
    out="countries/$country_dir/cia_factbook/summary.csv"
    [ -f "$raw" ] && [ "$FORCE" != "1" ] && { echo "=== $country_dir ($gec) : SKIP"; return 0; }

    echo "=== $country_dir ($gec, $region) ==="
    url="https://raw.githubusercontent.com/factbook/factbook.json/master/$region/$gec.json"
    full=$(curl -s --max-time 30 -H "User-Agent: $UA" "$url")
    if [ -z "$full" ] || ! jq -e .Government >/dev/null 2>&1 <<< "$full"; then
      echo "  fetch échoué ou pas de section Government" >&2; return 1
    fi
    gov=$(jq '.Government' <<< "$full")
    echo "$gov" > "$raw"

    {
      echo "field,text"
      local pair key path val
      for pair in \
        "country_name:.[\"Country name\"][\"conventional long form\"].text" \
        "government_type:.[\"Government type\"].text" \
        "capital:.Capital.name.text" \
        "chief_of_state:.[\"Executive branch\"][\"chief of state\"].text" \
        "head_of_government:.[\"Executive branch\"][\"head of government\"].text" \
        "legislature:.[\"Legislative branch\"].description.text" \
        "judicial_highest_courts:.[\"Judicial branch\"][\"highest court(s)\"].text" \
        "constitution_history:.Constitution.history.text"
      do
        key="${pair%%:*}"; path="${pair#*:}"
        val=$(_cia_extract_field "$gov" "$path")
        [ -z "$val" ] && continue
        printf '%s,"%s"\n' "$key" "${val//\"/\"\"}"
      done
    } > "$out"
    echo "  → $raw + $out"
    sleep 1
  }

  trap 'rm -rf "$TMPDIR_RUN"' RETURN
  _cia_build_map || return 1
  iter_country_dirs _cia_process "$@"
}

# ============================================================================
#  Phase 6 -- UN/DESA
# ============================================================================

cmd_un_desa() {
  local MAP_FILE="data/un_desa_ids.csv"
  local TMPDIR_RUN
  TMPDIR_RUN=$(mktemp -d)

  _un_build_map() {
    if [ -f "$MAP_FILE" ] && [ "$FORCE" != "1" ]; then
      local n; n=$(($(wc -l < "$MAP_FILE") - 1))
      if [ "$n" -lt 150 ]; then
        echo "WARN: $MAP_FILE n'a que $n entrées, reconstruction forcée." >&2
      else
        return 0
      fi
    fi
    echo "Construction du mapping country_dir ↔ ID UN/DESA…" >&2
    curl -s --max-time 30 -H "User-Agent: $UA" \
      "https://publicadministration.un.org/egovkb/en-us/Data-Center" \
      | grep -oE '/Data/Country-Information/id/[0-9]+-[A-Za-z-]+' | sort -u \
      | sed -E 's|^/Data/Country-Information/id/([0-9]+)-(.*)$|\1\t\2|' \
      > "$TMPDIR_RUN/pairs.txt"
    [ -s "$TMPDIR_RUN/pairs.txt" ] || { echo "ERR: index UN/DESA indisponible" >&2; return 1; }

    country_slug_table > "$TMPDIR_RUN/dirs.txt"

    awk -F'\t' '
      NR==FNR { dir[$1]=$2; next }
      {
        id=$1; name=$2
        norm=tolower(name); gsub(/-/, "", norm)
        if (norm in dir) print dir[norm] "," id "," name
      }
    ' "$TMPDIR_RUN/dirs.txt" "$TMPDIR_RUN/pairs.txt" > "$TMPDIR_RUN/matched.txt"

    {
      echo "country_dir,un_id,un_name"
      cat "$TMPDIR_RUN/matched.txt"
      [ -f data/un_desa_aliases.csv ] && tail -n +2 data/un_desa_aliases.csv
    } | awk -F, '!seen[$1]++' > "$MAP_FILE.tmp"
    mv "$MAP_FILE.tmp" "$MAP_FILE"
    local n; n=$(($(wc -l < "$MAP_FILE") - 1))
    echo "  → $MAP_FILE ($n pays mappés)" >&2
  }

  _un_fetch_html() {
    local id="$1" slug="$2" attempt resp http
    for attempt in 1 2 3; do
      resp=$(curl -s --max-time 30 -w '\n%{http_code}' -H "User-Agent: $UA" \
        "https://publicadministration.un.org/egovkb/en-us/Data/Country-Information/id/${id}-${slug}")
      http="${resp##*$'\n'}"; resp="${resp%$'\n'*}"
      [ "$http" = "200" ] && [ -n "$resp" ] && { printf '%s' "$resp"; return 0; }
      sleep $((attempt * 3))
    done
    return 1
  }

  _un_process() {
    local country_dir="$1" entry un_id un_name out html portal rank url
    entry=$(awk -F, -v c="$country_dir" 'NR>1 && $1==c {print $2","$3; exit}' "$MAP_FILE")
    [ -z "$entry" ] && { echo "  [$country_dir] aucun ID UN/DESA" >&2; return 1; }
    un_id="${entry%%,*}"; un_name="${entry##*,}"

    mkdir -p "countries/$country_dir/un_desa"
    out="countries/$country_dir/un_desa/summary.csv"
    [ -f "$out" ] && [ "$FORCE" != "1" ] && { echo "=== $country_dir (UN id=$un_id) : SKIP"; return 0; }

    echo "=== $country_dir (UN id=$un_id $un_name) ==="
    html=$(_un_fetch_html "$un_id" "$un_name") || { echo "  fetch échoué" >&2; return 1; }

    portal=$(grep -oE '<a href="[^"]+">National Portal</a>' <<< "$html" \
      | head -1 | sed -E 's|.*href="([^"]+)".*|\1|')
    rank=$(grep -oE 'Rank [0-9]+ of [0-9]+' <<< "$html" | head -1)
    url="https://publicadministration.un.org/egovkb/en-us/Data/Country-Information/id/${un_id}-${un_name}"

    {
      echo "field,text"
      [ -n "$portal" ] && printf 'national_portal,"%s"\n' "$portal"
      [ -n "$rank" ]   && printf 'egdi_rank,"%s"\n' "$rank"
      printf 'source_url,"%s"\n' "$url"
    } > "$out"
    echo "  → $out (portal: ${portal:-?}, $rank)"
    sleep 1
  }

  trap 'rm -rf "$TMPDIR_RUN"' RETURN
  _un_build_map || return 1
  iter_country_dirs _un_process "$@"
}

# ============================================================================
#  Phase 6 -- OECD
# ============================================================================

cmd_oecd() {
  local GAG_URL='https://www.oecd.org/en/topics/government-at-a-glance.html'
  local SDMX_GOV='https://sdmx.oecd.org/public/rest/dataflow/OECD.GOV.GIP/DSD_GOV@DF_GOV_2025'

  declare -A OECD_MEMBERS=(
    [AUS]=1971 [AUT]=1961 [BEL]=1961 [CAN]=1961 [CHL]=2010 [COL]=2020
    [CRI]=2021 [CZE]=1995 [DNK]=1961 [EST]=2010 [FIN]=1969 [FRA]=1961
    [DEU]=1961 [GRC]=1961 [HRV]=2025 [HUN]=1996 [ISL]=1961 [IRL]=1961
    [ISR]=2010 [ITA]=1962 [JPN]=1964 [KOR]=1996 [LVA]=2016 [LTU]=2018
    [LUX]=1961 [MEX]=1994 [NLD]=1961 [NZL]=1973 [NOR]=1961 [POL]=1996
    [PRT]=1961 [SVK]=2000 [SVN]=2010 [ESP]=1961 [SWE]=1961 [CHE]=1961
    [TUR]=1961 [GBR]=1961 [USA]=1961
  )

  _oecd_process() {
    local country_dir="$1" iso3="${country_dir%%_*}" out since
    mkdir -p "countries/$country_dir/oecd"
    out="countries/$country_dir/oecd/membership.csv"
    [ -f "$out" ] && [ "$FORCE" != "1" ] && { echo "=== $country_dir : SKIP"; return 0; }
    since="${OECD_MEMBERS[$iso3]:-}"
    if [ -n "$since" ]; then
      {
        echo "field,text"
        echo "oecd_member,yes"
        echo "member_since,$since"
        printf 'gov_at_a_glance,"%s"\n' "$GAG_URL"
        printf 'sdmx_dataflow,"%s"\n' "$SDMX_GOV"
      } > "$out"
      echo "=== $country_dir : OECD member (depuis $since)"
    else
      printf 'field,text\noecd_member,no\n' > "$out"
      echo "=== $country_dir : non-membre"
    fi
  }

  iter_country_dirs _oecd_process "$@"
}

# ============================================================================
#  enrich = wikidata + (iana + cia + un_desa + oecd en parallèle)
# ============================================================================

cmd_enrich() {
  echo "→ wikidata (séquentiel)…" >&2
  cmd_wikidata "$@"

  echo "→ iana + cia + un_desa + oecd (parallèle)…" >&2
  local LOGDIR; LOGDIR=$(mktemp -d)
  local -a pids=()

  ( "$0" iana "$@" > "$LOGDIR/iana.log" 2>&1 ) & pids+=($!:iana)
  ( "$0" cia  "$@" > "$LOGDIR/cia.log"  2>&1 ) & pids+=($!:cia)
  ( "$0" un-desa "$@" > "$LOGDIR/un_desa.log" 2>&1 ) & pids+=($!:un_desa)
  ( "$0" oecd "$@" > "$LOGDIR/oecd.log" 2>&1 ) & pids+=($!:oecd)

  local fail=0
  for entry in "${pids[@]}"; do
    local pid="${entry%%:*}" name="${entry#*:}"
    if wait "$pid"; then echo "  ✓ $name OK" >&2
    else echo "  ✗ $name a échoué (voir $LOGDIR/$name.log)" >&2; fail=1; fi
  done

  echo
  echo "=== résumé enrich ==="
  local name; for name in iana cia un_desa oecd; do
    echo "--- $name ---"; tail -10 "$LOGDIR/$name.log"
  done
  rm -rf "$LOGDIR"
  return "$fail"
}

# ============================================================================
#  Phase 7 -- cross_check (= score + rapport)
# ============================================================================

cmd_cross_check() {
  local TMPDIR_RUN; TMPDIR_RUN=$(mktemp -d)
  trap 'rm -rf "$TMPDIR_RUN"' RETURN

  local SOURCE_DIRS_RE='^(wikidata|iana|un_desa|cia_factbook|oecd)$'
  mapfile -t KNOWN_ROOTS < <(
    find countries -mindepth 2 -maxdepth 2 -type d -printf '%f\n' \
      | awk -v re="$SOURCE_DIRS_RE" '$0 !~ re' \
      | LC_ALL=C sort -u
  )

  _xc_csv_field() {
    [ -f "$1" ] || return 1
    awk -F, -v k="$2" 'NR>1 && $1==k {
      out=$2; for (i=3; i<=NF; i++) out = out FS $i
      sub(/^"/, "", out); sub(/"$/, "", out)
      print out; exit
    }' "$1"
  }

  _xc_is_already_covered() {
    local host="$1" k
    for k in "${KNOWN_ROOTS[@]}"; do
      [ "$host" = "$k" ] || [[ "$host" == *".${k}" ]] && return 0
    done
    return 1
  }

  _xc_collected_for() {
    local f="$TMPDIR_RUN/collected/$1"
    [ -f "$f" ] && cat "$f"
  }

  _xc_is_collected() {
    local host="$1" c="$2" line
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      [ "$line" = "$host" ] || [[ "$line" == *".${host}" ]] && return 0
    done < <(_xc_collected_for "$c")
    return 1
  }

  local MULTI_TLDS='co.uk gov.uk ac.uk org.uk com.au gov.au org.au co.nz gov.nz com.br gov.br co.za gov.za'
  _xc_parent_domain() {
    local host="$1" t
    for t in $MULTI_TLDS; do
      [[ "$host" == *".${t}" ]] && { printf '%s' "$t"; return; }
    done
    printf '%s' "$host" | awk -F. '{n=NF; print $(n-1)"."$n}'
  }

  declare -A SECONDARY_TLDS=(
    [GBR_united_kingdom]="scot wales im je gg gi io"
    [DNK_denmark]="fo gl"
    [NLD_netherlands]="aw cw sx"
  )

  local GOV_PATTERN='(^|\.)(gov|bund|govt|gouv|governo|gobierno|kormany|hallinto|riksdag|presidencia|presidence|parlement|parlamento|parliament|admin)(\.|$)'

  local SUBDIV_PATTERN='(departmental|départemental|departementale|départementale|regional|régional|state ministry|prefecture|préfecture|conseil général|general council|county council|provincial|county of|municipal|metropolitan|community of|communauté|comunidad autónoma|comunità|länder|bundesland|senate department|staatskanzlei|landtag|free state of|land of |bavaria|bavarian|bayer(ian|n)|saxony|saxon|sächs|hessian|hesse|hessisch|niedersä|niedersaechs|lower saxony|nordrhein|north rhine|westfalen|westphalia|baden-würt|baden-wuert|saarland|saarl|brandenburg|bremen ministry|free hanseatic|schleswig-holstein|mecklenburg|vorpommern|thüring|thuering|thuringia|rheinland-pfalz|rhineland-palatinate|hamburg ministry|hamburg(ische|er) (ministerium|behörde)|berlin senate|berlin(er) senat)'

  _xc_extract_factbook_phrases() {
    printf '%s' "$1" \
      | sed -E 's/\([^)]*\)//g' \
      | tr ';' '\n' \
      | sed -E 's/ or /\n/g; s/, /\n/g' \
      | awk '{
          gsub(/^[[:space:]]+|[[:space:]]+$/, "")
          if (length($0) >= 12) print tolower($0)
        }'
  }

  # Pré-éclate all_200_domains.csv par pays
  if [ -f all_200_domains.csv ]; then
    mkdir -p "$TMPDIR_RUN/collected"
    awk -F, 'NR>1 { print $1 > ("'"$TMPDIR_RUN"'/collected/" $3) }' all_200_domains.csv
  fi

  _xc_score() {
    local c="$1" cctld_primary="" un_portal_host="" fb_courts="" fb_phrases=""
    [ -f "countries/$c/iana/cctld.csv" ] && \
      cctld_primary=$(awk -F, 'NR==2 {print $1}' "countries/$c/iana/cctld.csv" | sed 's/^\.//')
    if [ -f "countries/$c/un_desa/summary.csv" ]; then
      local up; up=$(_xc_csv_field "countries/$c/un_desa/summary.csv" "national_portal")
      [ -n "$up" ] && un_portal_host=$(extract_host "$up")
    fi
    [ -f "countries/$c/cia_factbook/summary.csv" ] && \
      fb_courts=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "judicial_highest_courts")
    fb_phrases=$(_xc_extract_factbook_phrases "$fb_courts")

    declare -A WD_COUNT WD_LABEL
    if [ -f "countries/$c/wikidata/central_admin.csv" ]; then
      while IFS= read -r line; do
        [ -z "$line" ] && continue
        local host label
        host="${line##*,}"
        [ -z "$host" ] && continue
        label=$(printf '%s' "$line" | awk -F'","' '{print $1}' | sed -E 's/^[^,]*,"//')
        WD_COUNT[$host]=$(( ${WD_COUNT[$host]:-0} + 1 ))
        if [ -z "${WD_LABEL[$host]:-}" ]; then
          WD_LABEL[$host]="$label"
        elif [[ "${WD_LABEL[$host]}" != *"$label"* ]]; then
          WD_LABEL[$host]="${WD_LABEL[$host]} | $label"
        fi
      done < <(tail -n +2 "countries/$c/wikidata/central_admin.csv")
    fi

    declare -A ALL
    local h
    for h in "${!WD_COUNT[@]}"; do ALL[$h]=1; done
    [ -n "$un_portal_host" ] && ALL[$un_portal_host]=1

    local out="countries/$c/candidates.csv"
    {
      echo "hostname,score,sources,label"
      for h in "${!ALL[@]}"; do
        _xc_is_already_covered "$h" && continue
        local score=0 sources="" label="${WD_LABEL[$h]:-}" wd_count=${WD_COUNT[$h]:-0}

        if [ "$h" = "$un_portal_host" ]; then
          score=$((score + 5))
          sources="un_desa"
          label="${label:+$label | }UN/DESA national portal"
        fi
        if [ "$wd_count" -gt 0 ]; then
          local effective=$wd_count
          [ "$effective" -gt 2 ] && effective=2
          score=$((score + 3 * effective))
          local i; for ((i=0; i<wd_count; i++)); do sources="${sources:+$sources;}wikidata"; done
        fi
        [ -n "$cctld_primary" ] && {
          [ "$h" = "$cctld_primary" ] || [[ "$h" == *".${cctld_primary}" ]] && score=$((score + 1))
        }
        echo "$h" | grep -qiE "$GOV_PATTERN" && score=$((score + 1))
        if [ -n "$label" ] && [ -n "$fb_phrases" ]; then
          local label_lc; label_lc=$(printf '%s' "$label" | tr '[:upper:]' '[:lower:]')
          local phrase matched=0
          while IFS= read -r phrase; do
            [ -z "$phrase" ] && continue
            [[ "$label_lc" == *"$phrase"* ]] && { matched=1; break; }
          done <<< "$fb_phrases"
          [ "$matched" = 1 ] && score=$((score + 2))
        fi
        [ -n "$label" ] && echo "$label" | grep -qiE "$SUBDIV_PATTERN" && score=$((score - 5))
        if [ "$wd_count" -ge 3 ] && [ -n "$label" ]; then
          if ! echo "$label" | grep -qiE '(federal|national|sovereign|state of [a-z]+ federation)'; then
            score=$((score - 5))
          fi
        fi
        [ "$score" -gt 10 ] && score=10
        [ "$score" -lt 0 ] && score=0

        local lbl_csv="\"${label//\"/\"\"}\""
        printf '%s,%d,%s,%s\n' "$h" "$score" "$sources" "$lbl_csv"
      done | LC_ALL=C sort -t, -k2,2nr -k1,1
    } > "$out"
    unset WD_COUNT WD_LABEL ALL
  }

  _xc_report() {
    local c="$1" out="countries/$c/cross_check.md"
    _xc_score "$c"

    local cctld="" manager=""
    local iana="countries/$c/iana/cctld.csv"
    if [ -f "$iana" ]; then
      cctld=$(awk -F, 'NR==2 {print $1}' "$iana")
      manager=$(awk -F, 'NR==2 {print $2}' "$iana" | sed 's/^"//; s/"$//')
    fi
    local oecd_status="" oecd_since=""
    [ -f "countries/$c/oecd/membership.csv" ] && {
      oecd_status=$(_xc_csv_field "countries/$c/oecd/membership.csv" "oecd_member")
      oecd_since=$(_xc_csv_field "countries/$c/oecd/membership.csv" "member_since")
    }
    local un_portal="" un_rank=""
    [ -f "countries/$c/un_desa/summary.csv" ] && {
      un_portal=$(_xc_csv_field "countries/$c/un_desa/summary.csv" "national_portal")
      un_rank=$(_xc_csv_field "countries/$c/un_desa/summary.csv" "egdi_rank")
    }
    local fb_govtype="" fb_capital="" fb_courts="" fb_chief="" fb_head=""
    if [ -f "countries/$c/cia_factbook/summary.csv" ]; then
      fb_govtype=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "government_type")
      fb_capital=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "capital")
      fb_chief=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "chief_of_state")
      fb_head=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "head_of_government")
      fb_courts=$(_xc_csv_field "countries/$c/cia_factbook/summary.csv" "judicial_highest_courts")
    fi
    local n_collected un_st
    n_collected=$(_xc_collected_for "$c" | wc -l)
    un_st=$(un_status_for "$c")

    {
      echo "# Cross-check: $c"
      echo
      echo "## Overview"
      echo
      case "$un_st" in
        member)   echo "- UN status: **Member State**" ;;
        observer) echo "- UN status: **Observer State** (include as observer, not as member)" ;;
        non_un)   echo "- UN status: **Not recognised by the UN** (exclude from UN-facing report)" ;;
      esac
      [ -n "$cctld" ] && echo "- ccTLD: \`$cctld\` (manager: $manager)"
      [ "$oecd_status" = "yes" ] && echo "- OECD: member since $oecd_since"
      [ "$oecd_status" = "no" ]  && echo "- OECD: non-member"
      [ -n "$un_rank" ] && echo "- UN/DESA EGDI: $un_rank"
      [ -n "$fb_govtype" ] && echo "- Government type: $fb_govtype"
      [ -n "$fb_capital" ] && echo "- Capital: $fb_capital"
      echo "- Domains collected (HTTP 200): $n_collected"
      echo

      if [ -n "$un_portal" ]; then
        echo "## UN/DESA national portal"; echo
        local host; host=$(extract_host "$un_portal")
        echo "- Declared: [$un_portal]($un_portal) (host \`$host\`)"
        if _xc_is_collected "$host" "$c"; then
          echo "- ✅ Covered by collected domains"
        else
          local parent; parent=$(_xc_parent_domain "$host")
          if [ -d "countries/$c/$parent" ]; then
            echo "- ⚠️ Exact hostname not in the 200s, but a \`$parent\` root directory exists (to be probed)"
          else
            echo "- ⚠️ ABSENT -- neither \`$host\` covered nor \`countries/$c/$parent/\` directory present"
          fi
        fi
        echo
      fi

      if [ -n "$fb_courts" ] || [ -n "$fb_chief" ]; then
        echo "## Institutions named by CIA Factbook"; echo
        [ -n "$fb_chief" ] && echo "- Chief of state: $fb_chief"
        [ -n "$fb_head" ]  && echo "- Head of government: $fb_head"
        [ -n "$fb_courts" ] && echo "- Highest courts: $fb_courts"
        echo
        echo "(institution names usable as seeds for further research)"
        echo
      fi

      local cand="countries/$c/candidates.csv"
      if [ -f "$cand" ]; then
        local n_cand; n_cand=$(($(wc -l < "$cand") - 1))
        echo "## Candidate domains ranked by score"; echo
        if [ "$n_cand" -gt 0 ]; then
          echo "$n_cand candidate(s). Full list in [\`candidates.csv\`](candidates.csv)."
          echo "Top 20 by score (0-10) -- higher = stronger cross-source evidence:"; echo
          echo '| score | hostname | sources | label |'
          echo '|------:|----------|---------|-------|'
          tail -n +2 "$cand" | head -20 | awk -F, '{
            host=$1; score=$2; src=$3
            lab=$4; for (i=5; i<=NF; i++) lab = lab FS $i
            gsub(/^"|"$/, "", lab)
            if (length(lab) > 80) lab = substr(lab, 1, 77) "..."
            printf "| %s | `%s` | %s | %s |\n", score, host, src, lab
          }'
        else
          echo "No remaining candidates (every flagged institution is covered)."
        fi
        echo
      fi

      if [ -n "$cctld" ]; then
        local primary="${cctld#.}" extra="${SECONDARY_TLDS[$c]:-}" accept_re alt anomalies
        accept_re="^(${primary}|eu|com|net|org|int)$"
        if [ -n "$extra" ]; then
          alt=$(echo "$extra" | tr ' ' '|')
          accept_re="^(${primary}|eu|com|net|org|int|${alt})$"
        fi
        anomalies=$(_xc_collected_for "$c" \
          | awk -v re="$accept_re" -F. '$NF !~ re {print "."$NF" "$0}' | sort -u)
        if [ -n "$anomalies" ]; then
          echo "## ccTLD anomalies"; echo
          echo "Domains outside \`$cctld\` (allowed: common gTLDs + \`$extra\`):"; echo
          echo '```'; echo "$anomalies" | head -20; echo '```'; echo
        fi
      fi
    } > "$out"
    echo "=== $c → $out"
  }

  iter_country_dirs _xc_report "$@"
}

# ============================================================================
#  Utilitaire -- build-qid (one-shot, rejoue si on ajoute un pays)
# ============================================================================

cmd_build_qid() {
  local ENDPOINT='https://query.wikidata.org/sparql'
  local OUT='data/country_qid.csv'
  local query='SELECT DISTINCT ?country ?iso3 WHERE {
    ?country wdt:P31 wd:Q6256 ;
             wdt:P298 ?iso3 .
  }'
  local tmp; tmp=$(mktemp)

  echo "Interrogation Wikidata…"
  curl -s -G --max-time 120 \
    --data-urlencode "query=$query" \
    -H "Accept: application/sparql-results+json" \
    -H "User-Agent: $UA" "$ENDPOINT" \
    | jq -r '.results.bindings[] | [.iso3.value, (.country.value | sub("^.*/"; ""))] | @tsv' \
    | sort -u > "$tmp"

  if [ ! -s "$tmp" ]; then
    echo "ERR: réponse Wikidata vide ou invalide" >&2
    rm -f "$tmp"; return 1
  fi

  {
    echo "country_dir,iso3,wikidata_qid"
    local missing=0 dir dir_name iso3 qid
    for dir in countries/*/; do
      dir_name=$(basename "$dir")
      iso3="${dir_name%%_*}"
      qid=$(awk -F'\t' -v iso="$iso3" '$1 == iso { print $2 }' "$tmp")
      if [ -z "$qid" ]; then
        echo "WARN: pas de Q-ID pour $dir_name (iso3=$iso3)" >&2
        missing=$((missing+1)); continue
      fi
      printf '%s,%s,%s\n' "$dir_name" "$iso3" "$qid"
    done
  } > "$OUT"

  rm -f "$tmp"
  local n; n=$(($(wc -l < "$OUT") - 1))
  echo "Écrit $OUT ($n pays mappés)"
}

# ============================================================================
#  Dispatcher
# ============================================================================

usage() {
  sed -n '2,33p' "$0" | sed 's/^# \?//'
  exit "${1:-1}"
}

cmd="${1:-}"
[ -z "$cmd" ] && usage 1
shift || true

case "$cmd" in
  collect)
    cmd_fetch "$@" 2>&1 | tee /tmp/fetch_subdomains.log
    cmd_retry 2>&1 || true
    cmd_normalize
    cmd_probe "$@"
    cmd_collect_200
    ;;
  enrich)        cmd_enrich "$@" ;;
  report)        cmd_cross_check "$@" ;;
  all)
    "$0" collect "$@"
    "$0" enrich  "$@"
    "$0" report  "$@"
    ;;
  fetch)         cmd_fetch "$@" ;;
  retry)         cmd_retry "$@" ;;
  normalize)     cmd_normalize ;;
  probe)         cmd_probe "$@" ;;
  collect-200)   cmd_collect_200 ;;
  un-status)     cmd_un_status "$@" ;;
  wikidata)      cmd_wikidata "$@" ;;
  iana)          cmd_iana "$@" ;;
  cia)           cmd_cia "$@" ;;
  un-desa)       cmd_un_desa "$@" ;;
  oecd)          cmd_oecd "$@" ;;
  cross-check)   cmd_cross_check "$@" ;;
  build-qid)     cmd_build_qid ;;
  -h|--help|help) usage 0 ;;
  *)
    echo "ERR: sous-commande inconnue '$cmd'" >&2
    usage 1
    ;;
esac

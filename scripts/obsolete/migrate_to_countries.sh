#!/usr/bin/env bash
# Migre la structure domains/<portal>/ vers countries/<ISO3>_<slug>/<portal>/
#
# Le code ISO 3166-1 alpha-3 et le slug UN-short-name sont obtenus depuis
# Wikidata (toutes les entités P31=Q6256 avec une propriété P298 ISO3 + label
# anglais), avec 9 overrides pour les libellés CSV qui diffèrent de Wikidata.

set -u
cd "$(dirname "$0")/.."

WD_TSV=/tmp/iso3_mapping.tsv
if [ ! -s "$WD_TSV" ]; then
  echo "ERR: $WD_TSV manquant. Relancer la requête Wikidata d'abord." >&2
  exit 1
fi

declare -A WD_MAP
while IFS=$'\t' read -r iso3 name; do
  WD_MAP["$name"]="$iso3"
done < "$WD_TSV"

# Overrides : libellé CSV ≠ libellé Wikidata
declare -A OVERRIDES=(
  ["Bahamas"]="BHS"
  ["China"]="CHN"
  ["Cote d'Ivoire"]="CIV"
  ["Czechia"]="CZE"
  ["Gambia"]="GMB"
  ["Holy see"]="VAT"
  ["Micronesia"]="FSM"
  ["Netherlands"]="NLD"
  ["Sao Tome and Principe"]="STP"
)

slugify() {
  echo "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E "s/[^a-z0-9]+/_/g; s/^_//; s/_+\$//"
}

mkdir -p countries

missing=()
moved=0
while IFS=, read -r country region email portal url; do
  [ -z "$country" ] && continue
  if [ -n "${OVERRIDES[$country]:-}" ]; then
    iso3="${OVERRIDES[$country]}"
  else
    iso3="${WD_MAP[$country]:-}"
  fi
  if [ -z "$iso3" ]; then
    missing+=("$country")
    continue
  fi
  slug=$(slugify "$country")
  dir="countries/${iso3}_${slug}"
  mkdir -p "$dir"
  if [ -d "domains/$portal" ]; then
    if [ -d "$dir/$portal" ]; then
      echo "WARN: $dir/$portal existe déjà, fusion ignorée" >&2
    else
      git mv "domains/$portal" "$dir/$portal" 2>/dev/null || mv "domains/$portal" "$dir/$portal"
      moved=$((moved+1))
    fi
  fi
done < <(tail -n +2 data/world-governments.csv)

echo "Déplacé : $moved répertoires de domaines"
if [ "${#missing[@]}" -gt 0 ]; then
  echo "Sans mapping ISO3 (${#missing[@]}) :"
  printf '  %s\n' "${missing[@]}"
fi

# Cleanup
if [ -d domains ] && [ -z "$(ls -A domains)" ]; then
  rmdir domains
  echo "Supprimé : domains/ (vide)"
elif [ -d domains ]; then
  echo "Reste dans domains/ :"
  ls domains/
fi

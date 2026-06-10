#!/usr/bin/env python3
"""Populate ``iso2``, ``iso3`` and ``income_group`` columns in
``data/world-governments.csv``.

Run from the repository root with the project ``.venv`` active::

    uv pip install pycountry           # or: pip install pycountry
    python scripts/populate_iso_income.py

Sources
-------
- ``iso2`` / ``iso3``: ISO 3166-1 alpha-2 / alpha-3 via the ``pycountry`` library
  (vendored ISO list). A small explicit alias table covers the country name
  variants used in this repo that don't match pycountry exactly (e.g.
  ``Russia`` → ``Russian Federation``).
- ``income_group``: World Bank country-and-lending-group classification
  (FY2025; see
  https://datahelpdesk.worldbank.org/knowledgebase/articles/906519). Codes:
  ``H`` (High), ``UM`` (Upper-middle), ``LM`` (Lower-middle), ``L`` (Low).
  The classification is hardcoded as a Python dict so the script needs no
  network access. The World Bank refreshes this list every July — re-run the
  script after the next update and refresh ``INCOME_GROUPS`` from the source
  above.

This is a one-time enrichment helper, not part of the Babashka pipeline.
Running it again on the already-populated CSV is idempotent: existing values
are kept; only empty cells get filled.
"""

from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path

try:
    import pycountry
except ImportError:
    print("Error: pycountry not found. Install with: pip install pycountry")
    sys.exit(1)


CSV_PATH = Path("data/world-governments.csv")
BACKUP_PATH = Path("data/world-governments.csv.bak")

NEW_COLUMNS = ("iso2", "iso3", "income_group")


# Explicit aliases for country names that don't match pycountry directly.
COUNTRY_ALIASES: dict[str, str] = {
    "Russia": "Russian Federation",
    "Turkey": "Türkiye",
    "South Korea": "Korea, Republic of",
    "North Korea": "Korea, Democratic People's Republic of",
    "Iran": "Iran, Islamic Republic of",
    "Syria": "Syrian Arab Republic",
    "Vietnam": "Viet Nam",
    "Laos": "Lao People's Democratic Republic",
    "Tanzania": "Tanzania, United Republic of",
    "Bolivia": "Bolivia, Plurinational State of",
    "Venezuela": "Venezuela, Bolivarian Republic of",
    "Moldova": "Moldova, Republic of",
    "Brunei": "Brunei Darussalam",
    "Cape Verde": "Cabo Verde",
    "Ivory Coast": "Côte d'Ivoire",
    "Cote D Ivoire": "Côte d'Ivoire",
    "Czech Republic": "Czechia",
    "Czechia": "Czechia",
    "Macedonia": "North Macedonia",
    "North Macedonia": "North Macedonia",
    "Burma": "Myanmar",
    "Myanmar": "Myanmar",
    "Palestine": "Palestine, State of",
    "Palestinian Territories": "Palestine, State of",
    "Swaziland": "Eswatini",
    "Eswatini": "Eswatini",
    "East Timor": "Timor-Leste",
    "Timor-Leste": "Timor-Leste",
    "Vatican": "Holy See (Vatican City State)",
    "Holy See": "Holy See (Vatican City State)",
    "Holy see": "Holy See (Vatican City State)",
    "Vatican City": "Holy See (Vatican City State)",
    "Democratic Republic of the Congo": "Congo, The Democratic Republic of the",
    "Republic of the Congo": "Congo",
}


# Countries not in ISO 3166-1 that we still want to identify. ``XK`` is the
# Eurostat / IMF / EU placeholder for Kosovo, mirroring World Bank usage.
NON_ISO_COUNTRIES: dict[str, tuple[str, str]] = {
    "Kosovo": ("XK", "XKX"),
}


# World Bank income-group classification, FY2025 (keyed by ISO 3166-1 alpha-2).
# Source: https://datahelpdesk.worldbank.org/knowledgebase/articles/906519
INCOME_GROUPS: dict[str, str] = {
    "AE": "H",  "AF": "L",  "AL": "UM", "DZ": "UM", "AD": "H",  "AO": "LM", "AG": "H",  "AR": "H",
    "AM": "LM", "AU": "H",  "AT": "H",  "AZ": "UM", "BS": "H",  "BH": "H",  "BD": "LM", "BB": "H",
    "BY": "UM", "BE": "H",  "BZ": "UM", "BJ": "L",  "BT": "LM", "BO": "LM", "BA": "UM", "BW": "UM",
    "BR": "UM", "BN": "H",  "BG": "H",  "BF": "L",  "BI": "L",  "KH": "LM", "CM": "LM", "CA": "H",
    "CV": "LM", "CF": "L",  "TD": "L",  "CL": "H",  "CN": "UM", "CO": "UM", "KM": "LM", "CG": "LM",
    "CR": "UM", "HR": "H",  "CU": "UM", "CY": "H",  "CZ": "H",  "CI": "LM", "CD": "L",  "DK": "H",
    "DJ": "LM", "DM": "UM", "DO": "UM", "EC": "UM", "EG": "LM", "SV": "LM", "GQ": "UM", "ER": "L",
    "EE": "H",  "SZ": "LM", "ET": "L",  "FJ": "UM", "FI": "H",  "FR": "H",  "GA": "UM", "GM": "L",
    "GE": "UM", "DE": "H",  "GH": "LM", "GR": "H",  "GD": "UM", "GT": "LM", "GN": "L",  "GW": "L",
    "GY": "LM", "HT": "L",  "HN": "LM", "HK": "H",  "HU": "H",  "IS": "H",  "IN": "LM", "ID": "LM",
    "IR": "LM", "IQ": "LM", "IE": "H",  "IM": "H",  "IL": "H",  "IT": "H",  "JM": "UM", "JP": "H",
    "JO": "LM", "KZ": "UM", "KE": "LM", "KI": "LM", "KP": "L",  "KR": "H",  "KW": "H",  "KG": "LM",
    "LA": "LM", "LV": "H",  "LB": "LM", "LS": "LM", "LR": "L",  "LY": "LM", "LI": "H",  "LT": "H",
    "LU": "H",  "MO": "H",  "MG": "L",  "MW": "L",  "MY": "UM", "MV": "UM", "ML": "L",  "MT": "H",
    "MH": "UM", "MR": "LM", "MU": "UM", "MX": "UM", "FM": "UM", "MD": "LM", "MC": "H",  "MN": "LM",
    "MK": "UM",  # North Macedonia
    "ME": "UM", "MA": "LM", "MZ": "L",  "MM": "LM", "NA": "UM", "NR": "UM", "NP": "LM", "NL": "H",
    "NZ": "H",  "NI": "LM", "NE": "L",  "NG": "LM", "NO": "H",  "OM": "H",  "PK": "LM", "PW": "UM",
    "PA": "UM", "PG": "LM", "PY": "UM", "PE": "UM", "PH": "LM", "PL": "H",  "PT": "H",  "PR": "H",
    "QA": "H",  "RO": "H",  "RU": "UM", "RW": "L",  "KN": "H",  "LC": "UM", "VC": "UM", "WS": "LM",
    "SM": "H",  "ST": "LM", "SA": "H",  "SN": "L",  "RS": "UM", "SC": "H",  "SL": "L",  "SG": "H",
    "SK": "H",  "SI": "H",  "SB": "LM", "SO": "L",  "ZA": "UM", "SS": "L",  "ES": "H",  "LK": "LM",
    "SD": "LM", "SR": "UM", "SE": "H",  "CH": "H",  "SY": "LM", "TW": "H",  "TJ": "L",  "TZ": "L",
    "TH": "UM", "TL": "LM", "TG": "L",  "TO": "LM", "TT": "H",  "TN": "LM", "TR": "UM", "TM": "LM",
    "TV": "UM", "UG": "L",  "UA": "LM", "GB": "H",  "US": "H",  "UY": "H",  "UZ": "LM",
    "VU": "LM", "VE": "UM", "VN": "LM", "PS": "LM", "YE": "L",  "ZM": "LM", "ZW": "LM",
    "VA": "H",   # Holy See (Vatican City State)
    "XK": "UM",  # Kosovo (non-standard ISO; UM per World Bank FY2025)
}


def get_iso_codes(country_name: str) -> tuple[str | None, str | None]:
    """Return ``(iso2, iso3)`` for a country name, or ``(None, None)`` if not found."""
    # Non-ISO entries (Kosovo etc.) — explicit codes, no pycountry lookup.
    if country_name in NON_ISO_COUNTRIES:
        return NON_ISO_COUNTRIES[country_name]
    # Try aliases first (we know exactly what these map to).
    target = COUNTRY_ALIASES.get(country_name, country_name)
    try:
        country = pycountry.countries.lookup(target)
        return country.alpha_2, country.alpha_3
    except LookupError:
        pass
    # Fall back to fuzzy search — careful: this returns the best match which
    # may be a NEIGHBOUR for non-ISO entries. Use NON_ISO_COUNTRIES above for
    # those.
    try:
        country = pycountry.countries.search_fuzzy(target)[0]
        return country.alpha_2, country.alpha_3
    except (LookupError, IndexError):
        return None, None


def main() -> int:
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found. Run from the repository root.")
        return 1

    shutil.copy(CSV_PATH, BACKUP_PATH)
    print(f"Backup: {BACKUP_PATH}")

    with CSV_PATH.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        original_columns = list(reader.fieldnames or [])
        rows = list(reader)

    new_columns = [c for c in NEW_COLUMNS if c not in original_columns]
    columns = original_columns + new_columns

    iso2_count = iso3_count = income_count = 0
    unresolved: list[str] = []

    for row in rows:
        # Initialize missing columns to "" so the dict-writer round-trips cleanly.
        for c in new_columns:
            row.setdefault(c, "")

        country = (row.get("Country") or "").strip()
        if not country:
            continue

        iso2, iso3 = get_iso_codes(country)

        if iso2 and not row.get("iso2"):
            row["iso2"] = iso2
            iso2_count += 1
        if iso3 and not row.get("iso3"):
            row["iso3"] = iso3
            iso3_count += 1
        if iso2 and not row.get("income_group"):
            ig = INCOME_GROUPS.get(iso2)
            if ig:
                row["income_group"] = ig
                income_count += 1

        if not (iso2 and iso3):
            unresolved.append(country)

    tmp_path = CSV_PATH.with_suffix(CSV_PATH.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(CSV_PATH)
    print(f"Wrote: {CSV_PATH}")

    print()
    print(f"  iso2 populated:         {iso2_count}/{len(rows)}")
    print(f"  iso3 populated:         {iso3_count}/{len(rows)}")
    print(f"  income_group populated: {income_count}/{len(rows)}")
    if unresolved:
        print()
        print(f"Unresolved countries ({len(unresolved)}):")
        for c in unresolved:
            print(f"  - {c}")
    else:
        print()
        print("All countries resolved.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

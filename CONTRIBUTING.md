# Contributing

For each country, this repository lists the public-sector domain names
we want to consider when building the [State of Public
Code](https://stateofpubliccode.org). Understanding how a country's
administration works is challenging and anyone's help is very welcome!

## Find your country

Each country has a directory named after its alpha-3 code and its
English name: `countries/FRA_france/`, `countries/USA_united_states/`,
etc.

## The two files you edit

Every decision lives in two small CSV files of the country directory.
Everything else is generated from them and from official registries:
never edit `to_curate.csv`, `proposed.csv`, `summary.md` or
anything under `sources/`.
In `data/`, everything is generated except `world-governments.csv`,
the list of countries: open an issue to change it.

### `curated.csv`: the domains you confirm

One row per domain, as `domain,level,name,kind`:

```csv
domain,level,name,kind
gouv.fr,central,,
sp.gov.br,central-1,São Paulo,exact
abingdon.gov.uk,local,Abingdon Town Council,
```

- `domain` is a domain name
- `level` is one of `central`, `central-1` (the first tier below) or `local`.
- `name` is the body the domain belongs to.
- `kind` is usually empty: the domain counts with everything below it.
  Write `exact` when the domain counts alone, because lower tiers sit
  under it too: `sp.gov.br` is the state of São Paulo, but
  `campinas.sp.gov.br` is a municipality.

### `excluded.csv`: the domains you reject

One row per domain, as `domain,reason`, for what is not a public body
at all (a company, a private university, a typo, etc.):

```csv
domain,reason
example-company.fr,private company
```

A domain listed here never comes back in `to_curate.csv` nor in
`proposed.csv`.

## Tell us about an official registry

Some countries publish an authoritative list of their public bodies
with their domain names (the CISA `.gov` registry in the United
States, `lannuaire.service-public.fr` in France…). We import such a
registry directly: its domains need no curation. The "Authoritative
registries" section of your country's `summary.md` lists the ones we
use; when it is missing, or when you know a registry we do not use,
please tell us (see "Send your changes" below): it can save hundreds
of rows of curation.

## How to contribute

1. Open `to_curate.csv`, the domains our sources suggest that no one
   has decided on yet, best first. Its columns are those of
   `curated.csv`, `level` and `name` being our best guess.
2. For each domain, if it is right, move its row to `curated.csv`,
   fixing the level, the name or the kind if needed; if it is wrong,
   add it to `excluded.csv` with a reason.
3. Add the domains we missed to `curated.csv` directly.

`proposed.csv` holds the same suggestions with their `score` (0 to 10)
and the `sources` behind them, to help you decide. If you have
Babashka, `bb todo FRA` refreshes `to_curate.csv` after your
changes: a confirmed or excluded domain leaves it, with everything
below it.

## Check your changes

If you have [Babashka](https://babashka.org) installed, `bb check FRA`
tells whether your files are consistent, and what to write instead
when they are not.

If you know the public administration of a country, you can also read
`data/public-sector-domains-central+-policy.csv`, the compiled result
for every country, and report problems.

# Send your changes

Open a pull request on
[GitHub](https://github.com/SoftwareHeritage/world-gov-domain-names),
or email a patch or your list of domains to `bastien.guerry@inria.fr`.

# Conventional commits

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/)

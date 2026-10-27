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
never edit `proposed.csv`, `summary.md` or anything under `sources/`.
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

A domain listed here never comes back in `proposed.csv`.

## How to contribute

1. Open `proposed.csv`, the list of suggested domains.
2. For each domain, if it is right, copy its first three columns to
   `curated.csv`; if it is wrong, add it to `excluded.csv`.
3. Add the domains we missed to `curated.csv` directly.

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

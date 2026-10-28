# Contributing

For each country, this repository lists the public-sector domain names
we want to consider when building the [State of Public
Code](https://stateofpubliccode.org). Most of the work is automated:
official registries, Wikidata, ROR and certificate logs give us
thousands of candidate domains. What scripts cannot do is understand
how a country's administration is organised, and this is where your
help is the most valuable.

## Find your country

Each country has a directory named after its alpha-3 code and its
English name: `countries/FRA_france/`, `countries/USA_united_states/`,
etc.

## What only you can tell us

Three questions decide most of what we count for a country, and none
of them can be answered by a script. If you have only a few minutes,
spend them here.

### 1. Is there an official registry of public bodies?

Some countries publish an authoritative list of their public bodies
with their domain names: the CISA `.gov` registry in the United
States, `lannuaire.service-public.fr` in France, the CDDO list of
`gov.uk` domains in the United Kingdom. We import such a registry
directly and its domains need no curation: one registry can save
hundreds of rows of manual work.

The "Authoritative registries" section of your country's `summary.md`
lists the ones we already use. If you know one we do not use, tell us
its URL (see "Send your changes" below), ideally a machine-readable
export (CSV, JSON, API) with a website column. Directories that list
bodies of every level are useful too: we use them to propose
candidates.

### 2. Does a domain stand for everything below it?

By default, a confirmed domain counts with all its subdomains:
`gouv.fr` covers `numerique.gouv.fr`, `data.gouv.fr`, etc. This is
wrong when a lower tier of government also lives under that domain.
In Brazil, `sp.gov.br` is the State of São Paulo, but
`campinas.sp.gov.br` is a municipality: `sp.gov.br` must count alone.

For each `central` or `central-1` domain of your country in
`curated.csv`, check whether lower-tier bodies (municipalities,
districts, counties…) use its subdomains. If they do, write `exact` in
its `kind` column.

### 3. Which lower-tier bodies share a national root?

Many countries host several levels of government under a single root
domain: `gov.uk` holds both ministries and town councils, `gov.za`
both national departments and municipalities. We keep such a root as
`central`, so we need to take the lower-tier labels out of it, one by
one: each one is a `local` row in `curated.csv`.

```csv
domain,level,name,kind
gov.za,central,,
capetown.gov.za,local,City of Cape Town,
```

A `local` row only works for a label directly under the root
(`capetown.gov.za` under `gov.za`), and it takes that label out with
everything below it. You can see the hosts we found under a root in
`countries/<country>/sources/harvest/<root>.csv`.

These roots still mix levels and need someone who knows the country
(the source that would list their lower tier is given when we know
it):

| Root     | Mixed with                                    | Possible source             |
|----------|-----------------------------------------------|-----------------------------|
| `gov.za` | provinces and ~257 municipalities             | Municipal Demarcation Board |
| `gov.co` | departments, cities and municipalities        | DANE / DIVIPOLA             |
| `gov.ph` | provinces and local government units          | PSGC                        |
| `go.id`  | provinces, regencies and cities               | Kemendagri                  |
| `gov.vn` | provinces (bare names) and lower tiers        |                             |
| `gov.bd` | zila and upazila parishads                    |                             |
| `gov.pk` | provinces and cities                          |                             |
| `gov.tr` | metropolitan water utilities, local unions    |                             |
| `gov.ng` | two-letter state codes                        |                             |
| `gov.ke` | to check: nothing harvested yet               |                             |
| `go.th`  | to check: mixed policy, mostly central sample |                             |

When the lower tier follows a naming rule (`*-bld.gov.tr`, `*state.gov.ng`),
telling us the rule is enough: we can apply it in the scripts.

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

- `domain` is a domain name.
- `level` is `central` (central government and its bodies),
  `central-1` (the first tier below: state, region, Land…) or `local`
  (anything lower, only to take a label out of a shared root, see
  above).
- `name` is the body the domain belongs to.
- `kind` is empty (the domain counts with everything below it) or
  `exact` (the domain counts alone, see above).

### `excluded.csv`: the domains you reject

One row per domain, as `domain,reason`, for what is not a public body
at all (a company, a private university, a typo, etc.):

```csv
domain,reason
example-company.fr,private company
```

A domain listed here never comes back in `to_curate.csv` nor in
`proposed.csv`. Do not use it for public bodies of a lower tier: write
a `local` row in `curated.csv` instead.

## Review our suggestions

`to_curate.csv` holds the domains our sources suggest and that no one
has decided on yet, best first. Its columns are those of
`curated.csv`, `level` and `name` being our best guess.

1. For each domain you are sure about, move its row to `curated.csv`,
   fixing the level, the name or the kind if needed; if it is not a
   public body, add it to `excluded.csv` with a reason.
2. Add the domains we missed to `curated.csv` directly.

You do not need to go through the whole file: a few rows from the top
already help. `proposed.csv` holds the same suggestions with their
`score` (0 to 10) and the `sources` behind them, to help you decide.
If you have Babashka, `bb todo FRA` refreshes `to_curate.csv` after
your changes: a confirmed or excluded domain leaves it, with
everything below it.

## Check your changes

If you have [Babashka](https://babashka.org) installed, `bb check FRA`
tells whether your files are consistent, and what to write instead
when they are not.

If you know the public administration of a country, you can also read
`data/public-sector-domains-central+-policy.csv`, the compiled result
for every country, and report problems.

## Send your changes

Open a pull request on
[GitHub](https://github.com/SoftwareHeritage/world-gov-domain-names),
or email a patch, your list of domains or the URL of a registry to
`bastien.guerry@inria.fr`.

## Conventional commits

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/).

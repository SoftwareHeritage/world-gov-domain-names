# Contributing

For each country, this repository lists the public-sector domain names
we want to consider when building the [State of Public
Code](https://stateofpubliccode.org).

Part of the work is automated: official registries, Wikidata, ROR and
certificate logs give us thousands of candidate domains. What scripts
cannot do is understand how a country's administration is organised,
and this is where your help is the most valuable.

Each country has its directory: `countries/FRA_france/`, etc.

## Where your help is most wanted

### 1. Is there an official registry of public bodies?

Some countries publish an authoritative list of their public bodies
with their domain names: the CISA `.gov` registry in the United
States, `lannuaire.service-public.fr` in France, the CDDO list of
`gov.uk` domains in the United Kingdom. We import such a registry
directly and its domains need no curation.

The "Authoritative registries" section of your country's `summary.md`
lists the ones we identified. If you know one we do not use, please
share its URL with us.

### 2. Are local authorities websites under a national/regional domain?

A confirmed domain counts with all its subdomains: `gouv.fr` covers
`numerique.gouv.fr`, `data.gouv.fr`, etc. Sometimes, such subdomains
are for local authorities, which the State of Public Code does not
take into account: e.g. in Brazil, `sp.gov.br` is the State of São
Paulo, but `campinas.sp.gov.br` is a municipality. This requires two
possible actions in `curated.csv`:

- **Write the level without `*`** (`central-1`, not `central-1*`) when
  we know that not all subdomains should be included: the domain
  counts alone.

- **Otherwise, keep the `*`** (`central*`) and mark subdomains to
  exclude as `local`. E.g. `gov.za` holds both national departments
  and municipalities, so `capetown.gov.za` is marked as `local`.

The root domains that mix levels are listed in
[`data/mixed-roots.csv`](data/mixed-roots.csv) and need someone who
knows the country. For each of them, `to_curate.csv` lists every
domain directly under the root (`capetown.gov.za`, `dpme.gov.za`…)
until `curated.csv` gives it a level: `local` for a lower tier,
`central*` or `central-1*` otherwise, so that it does not come back.
Under a root written without `*`, like the Brazilian states, this is
how a body of the State gets in: `detran.sp.gov.br,central-1*`.
A level is suggested when the label names a first-tier subdivision
(`gauteng.gov.za`, `central-1*`).

When the lower tier follows a naming rule (`*-bld.gov.tr`,
`*state.gov.ng`), telling the rule will help a lot.

## You only need to edit two files

Every decision lives in two CSV files of the country directory.

Everything else is generated from them and from official registries:
never edit `to_curate.csv`, `proposed.csv`, `summary.md` or anything
under `sources/`. In `data/`, everything is generated except
`world-governments.csv`, the list of countries, and `mixed-roots.csv`,
the roots that mix levels: open an issue or send us an email to change
them.

### `curated.csv` for the domains you confirm

One row per domain, as `domain,level`:

```csv
domain,level
gouv.fr,central*
sp.gov.br,central-1
abingdon.gov.uk,local
```

- `domain` is a domain name.
- `level` is `central` (central government and its bodies),
  `central-1` (the first tier below: state, region, Land, etc.) or
  `local` (anything lower). A `*` after `central` or `central-1`
  means the domain counts with all its subdomains (`gouv.fr` covers
  `data.gouv.fr`); without it, the domain counts alone (see above).
  `local` never takes a `*`: the domain and everything below it are
  left out.

### `excluded.csv` for the domains you reject

One row per domain, as `domain,reason`, for what is not a public
body at all (a company, a private university, a typo, etc.):

```csv
domain,reason
example-company.fr,private company
```

A domain listed here never comes back in `to_curate.csv` nor in
`proposed.csv`. Do not use it for public bodies of a lower tier: write
a `local` row in `curated.csv` instead.

## Review our suggestions

`to_curate.csv` holds domains that our sources suggest but need to be
reviewed.

1. For each domain you are sure about, move its row to `curated.csv`,
   fixing the level if needed; if it is not a public body,
   add it to `excluded.csv` with a reason.
2. Add the domains we missed to `curated.csv` directly.

You do not need to go through the whole file: a few rows from the top
already help. 

You can also read `data/public-sector-domains-central+-policy.csv`,
the compiled result for every country, and report problems.

## Check your changes

With [Babashka](https://babashka.org), `bb check FRA` tells whether
your files are consistent, and what to write instead when they are
not; `bb todo FRA` refreshes `to_curate.csv`, from which a confirmed
or excluded domain leaves with everything below it.

## Send your changes

Open a pull request on
[GitHub](https://github.com/SoftwareHeritage/world-gov-domain-names),
or email a patch, your list of domains or the URL of a registry to
`bastien.guerry@inria.fr`.

## Conventional commits

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/).

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

- **Mark such domains as `exact` in the `kind` column** when we know
  for sure that all subdomains below them should be excluded.
  
- **Otherwise, keep `subtree` as the `kind` for these domains** and
  mark subdomains to exclude as `local`. E.g. `gov.za` holds both
  national departments and municipalities, so `capetown.gov.za` is
  marked as `local`.

These root domains mix levels and need someone who knows the country:

| Root     | Mixed with                                    |
|----------|-----------------------------------------------|
| `gov.za` | provinces and ~257 municipalities             |
| `gov.co` | departments, cities and municipalities        |
| `gov.ph` | provinces and local government units          |
| `go.id`  | provinces, regencies and cities               |
| `gov.vn` | provinces (bare names) and lower tiers        |
| `gov.bd` | zila and upazila parishads                    |
| `gov.pk` | provinces and cities                          |
| `gov.tr` | metropolitan water utilities, local unions    |
| `gov.ng` | two-letter state codes                        |
| `go.th`  | to check: mixed policy, mostly central sample |

When the lower tier follows a naming rule (`*-bld.gov.tr`,
`*state.gov.ng`), telling the rule will help a lot.

## You only need to edit two files

Every decision lives in two CSV files of the country directory.

Everything else is generated from them and from official registries:
never edit `to_curate.csv`, `proposed.csv`, `summary.md` or anything
under `sources/`. In `data/`, everything is generated except
`world-governments.csv`, the list of countries: open an issue or send
us an email to change it.

### `curated.csv` for the domains you confirm

One row per domain, as `domain,level,kind`:

```csv
domain,level,kind
gouv.fr,central,subtree
sp.gov.br,central-1,exact
abingdon.gov.uk,local,
```

- `domain` is a domain name.
- `level` is `central` (central government and its bodies),
  `central-1` (the first tier below: state, region, Land, etc.) or
  `local` (anything lower).
- `kind` is empty (the domain counts with everything below it, like
  `subtree`) or `exact` (the domain counts alone, see above).

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
   fixing the level or the kind if needed; if it is not a public body,
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

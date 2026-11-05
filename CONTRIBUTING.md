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
`data.gouv.fr`. Some roots also hold local authorities, which the
State of Public Code leaves out: `gov.za` holds national departments
and municipalities, `sp.gov.br` the State of São Paulo and its
municipalities (`campinas.sp.gov.br`). Such a root either:

- keeps its subdomains, and the local ones directly under it are
  marked `local` (`capetown.gov.za`);
- or counts alone, with a `!` (`sp.gov.br,central-1!`), and the bodies
  under it that belong to its level are added one by one
  (`detran.sp.gov.br,central-1`).

These roots are listed in [`data/mixed-roots.csv`](data/mixed-roots.csv).
For each of them, `to_curate.csv` lists every domain we found directly
under the root until `curated.csv` gives it a level. Some levels are
suggested, from a subdivision name (`gauteng.gov.za`, `central-1`) or
a naming rule (`jatengprov.go.id`, `central-1`; `bantulkab.go.id`,
`local`): check them. If you know such a rule for your country, tell us.

## You only need to edit two files

Every decision lives in two CSV files of the country directory.
Everything else is generated or maintained by us: never edit
`to_curate.csv`, `proposed.csv`, `summary.md` or anything under
`sources/`, and open an issue or send us an email to change a file of
`data/` (the list of countries, the mixed roots…).

### `curated.csv` for the domains you confirm

One row per domain, as `domain,level`:

```csv
domain,level
gouv.fr,central
sp.gov.br,central-1!
abingdon.gov.uk,local
```

`domain` is a domain name.

`level` can have one of five values:

- `central`, for domains for the central government and its bodies.
  All higher-level domains of such an entry will count. For example,
  the entry `gouv.fr,central` will count `gouv.fr`, `foo.gouv.fr`,
  `bar.foo.gouv.fr` and so on.
  - `central!` describes a central government domain but ignores
  its higher-level domains. The entry `gouv.fr,central!` will count
  `gouv.fr` but not `foo.gouv.fr` or `bar.foo.gouv.fr`.
- `central-1` and `central-1!` function identically to the previous
  levels, except that they are intended for the first tier below
  government organisations, such as state, region, Land and so on.
- `local` is for organisations at a lower level than the
  aforementioned ones, and does not count its higher-level domains.
  For example, `abingdon.gov.uk` describes a local council, and
  `foo.abingdon.gov.uk` or `bar.foo.abingdon.gov.uk` will not count.

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

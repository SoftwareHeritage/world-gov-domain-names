# Contributing

For each country, this repository lists public sector domain names
that we want to consider when building the [State of Public
Code](https://stateofpubliccode.org). But understanding how a
country's administration works is challenging and anyone's help is
very welcome!

## Where decisions live

In each `countries/<ISO3>_<slug>/` directory, two hand-edited files
carry the decisions:

- `curated.csv` contains the confirmed domains, one per line, as
  `domain,level,name`. `level` is one of `central`, `central-1` (first
  administrative level below the top one, e.g. state, Land, region)
  and `local`.
- `excluded.csv` contains what is out of scope altogether, one full
  hostname per line, as `domain,reason`.

Never edit `proposed.csv`, `summary.md` or anything under `sources/`
or `data/`: they are generated.

## How to contribute

- Go through `proposed.csv` (the worklist: each domain with a score
  and, when known, a level) and confirm a domain by adding one
  `domain,level,name` line to `curated.csv` or reject it by adding one
  `domain,reason` line to `excluded.csv`.
- Add a domain we missed to `curated.csv` directly, or a domain to
  reject to `excluded.csv` directly.

## Checking the classifications for your country

If you know the public administration of a country, the most useful
checks are:

- `data/public-sector-domains-central+-policy.csv` -- are the
  `subtree` and `exact` rows really the central government and the
  first tier below it?
- `countries/<ISO3>_<slug>/proposed.csv` and `summary.md` -- did we
  miss or misclassify domains?
- `countries/<ISO3>_<slug>/curated.csv` -- for shared roots like
  `gov.br`: are the lower-tier labels all listed as `local`? The
  first-tier bodies (`sp.gov.br`) belong there as `central-1`.
- First-tier bodies hosted inside a `local` subtree (a state agency
  such as `educacao.sp.gov.br`) match nothing until listed
  explicitly: telling us about them is especially useful.

Nothing has to be regenerated before sending a change: the maintainer
rebuilds the consolidated lists.

If you have [Babashka](https://babashka.org) installed, `bb pipeline
check <ISO3>_<slug>` tells whether your files are consistent (a domain
both curated and excluded, an unknown level, an invalid hostname).

## Sending your changes

The default repository for receiving contributions is the one on
[GitHub](https://github.com/SoftwareHeritage/world-gov-domain-names),
where we expect pull requests. Contributions by email are also
welcome: send a patch, or simply your list of domains, to
bastien.guerry@inria.fr. A change only needs to touch `curated.csv` or
`excluded.csv`.

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/).

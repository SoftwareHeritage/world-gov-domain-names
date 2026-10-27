# Contributing

For each country, this repository lists public sector domain names
that we want to consider when building the [State of Public
Code](https://stateofpubliccode.org). Understanding how a country's
administration works is challenging and anyone's help is very welcome!

## Files you can modify

In each `countries/<ISO3>_<slug>/` directory, two hand-edited files
carry some decisions:

- `curated.csv` contains domain names that some confirmed as public
  sector (as `domain,level,name`). `level` accepts three values:
  `central`, `central-1` (first administrative level below the top
  one) and `local`.
- `excluded.csv` contains what should be excluded, one hostname per
  line (as `domain,reason`).

Never edit `proposed.csv`, `summary.md` or anything under `sources/`
or `data/`: they are generated.

## How to contribute

- Go through `proposed.csv` and confirm a domain by adding one
  `domain,level,name` line to `curated.csv` or reject it by adding one
  `domain,reason` line to `excluded.csv`.
- Add a domain we missed to `curated.csv` directly, or a domain to
  reject to `excluded.csv` directly.

## Checking the classifications for your country

If you know the public administration of a country, you can also check
`data/public-sector-domains-central+-policy.csv` and report problems.

If you have [Babashka](https://babashka.org) installed, `bb check FRA`
(your country's ISO3 code) tells whether your files are consistent,
and what to write instead when they are not.

## Sending your changes

The repository for receiving contributions is on
[GitHub](https://github.com/SoftwareHeritage/world-gov-domain-names),
and accept pull requests. 

You can also contribute by email: send a patch or your list of domains
to bastien.guerry@inria.fr.

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/).

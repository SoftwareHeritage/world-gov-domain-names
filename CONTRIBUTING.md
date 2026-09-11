# Contributing

For each country, this repository lists public sector domain names
that we want to consider when building the [State of Public
Code](https://stateofpubliccode.org). But understanding how a
country's administration works is challenging and anyone's help is
very welcome!

In each `countries/<ISO3>_<slug>/` directory, contributing can happen
in these ways:

- Going through `proposed.csv` and moving one or several lines to `curated.csv` (the csv containing the manually curated names).
- Going through `proposed.csv` and moving one or several lines to `excluded.csv` (the csv containing the manually excluded names).
- Adding an excluded domain in `excluded.csv` directly.
- Adding a new domain in `curated.csv` directly.

Never edit `proposed.csv`, `summary.md` or anything under `sources/`:
they are generated.

Then open a merge request, or send a patch to bastien.guerry@inria.fr.

If you have [Babashka](https://babashka.org) installed, `bb pipeline
check <ISO3>_<slug>` tells you whether your files are consistent.

Commit messages follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/).

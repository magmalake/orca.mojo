- **The Hetzner deploy tooling is a release asset: deploying needs no
  checkout of orca.mojo (#247).** Each GitHub release attaches
  `orca-hetzner-<version>.tar.gz` (and `orca-hetzner.tar.gz`, for
  `releases/latest/download/`), which unpacks to `orca-hetzner-<version>/`
  with `up.sh`, `deploy.sh`, `down.sh`, `rescale.sh`, `backup.sh`, `server/`,
  `cloud-init.yaml`, `hetzner.env.example`, a README and a `VERSION` file;
  the scripts print that version beside the stack's name. The new
  `.github/workflows/release.yml` tags `v<version>` at a merged
  `release/orca-mojo-<version>` PR's commit, packages it
  (`deploy/hetzner/package.sh`) and creates the release, its notes the
  version's section of CHANGELOG.md; run by hand, it releases an earlier
  version at a given commit.

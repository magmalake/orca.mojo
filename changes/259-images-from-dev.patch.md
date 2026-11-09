- **Hetzner: images come from the census's `dev` branch.** The census's CI
  no longer builds an image on main or moves `latest` (mojo-census#90), so
  `hetzner.env.example` starts the first `up.sh` on `ORCA_TAG=dev` and
  docs/hetzner.md says where a sha comes from now.

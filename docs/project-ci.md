# A project's CI

orca publishes three reusable GitHub workflows for an orca project's own CI,
so that it doesn't repeat what every project needs: an image for each
architecture with the right CPU target, `orca config check`, the pushes and
a release for a tag, and the pixi workarounds. A project calls them pinned to
an orca tag, the same orca its `pixi.lock` pins, so it moves to new CI when
it moves to a new orca.

- [`project-image.yml`](../.github/workflows/project-image.yml) builds the
  project's image, checks its orca.toml files, tests it, and pushes it.
- [`project-test.yml`](../.github/workflows/project-test.yml) sets up pixi
  and runs the project's test tasks.
- [`project-release.yml`](../.github/workflows/project-release.yml) makes a
  tag's GitHub release, with its notes from `CHANGELOG.md`.

## A caller

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main, dev]
    tags: ['v*']
  pull_request:

jobs:
  test:
    uses: magmalake/orca.mojo/.github/workflows/project-test.yml@v0.24.0
    with: { tasks: "build test" }

  image:
    uses: magmalake/orca.mojo/.github/workflows/project-image.yml@v0.24.0
    permissions: { contents: read, packages: write }
    with:
      image: ghcr.io/mojourney/census
      configs: orca.toml orca.dev.toml
      push: ${{ github.event_name == 'push' && github.ref != 'refs/heads/main' }}
      extra-tags: ${{ github.ref == 'refs/heads/dev' && 'dev' || '' }}

  release:
    if: github.ref_type == 'tag'
    needs: [test, image]
    uses: magmalake/orca.mojo/.github/workflows/project-release.yml@v0.24.0
    permissions: { contents: write }
    with: { image: ghcr.io/mojourney/census }
```

Only the job that pushes needs `packages: write`, and only the release job
`contents: write`; a pull request never pushes, whatever `push` says.

## `project-image.yml`

One job per architecture, each on a runner of its own (no emulation), built
for a baseline CPU: `neoverse-n1` on arm64, the Hetzner CAX servers' CPU
(GitHub's arm64 runners are Neoverse N2, and a binary built for its SVE2
dies on a CAX with SIGILL; see [Images](hetzner.md#images)), and
`x86-64-v3` on amd64. Each job:

1. runs `prepare`, if given;
2. reads the orca-mojo version from `pixi.lock`, as `orca aws push` does;
3. builds the image: from
   [`deploy/project.Dockerfile`](../deploy/project.Dockerfile) of that orca
   release (`v<version>`, or `orca-ref`), `FROM`
   `ghcr.io/magmalake/orca-runtime:<version>`, or from the project's own
   `dockerfile`. The build args are `MOJO_TARGET_CPU`, `ORCA_SOURCE` (the
   repository's URL), `ORCA_COMMIT` (the commit), the version as
   `version-arg`, `ORCA_VERSION` and `ORCA_RUNTIME`, and `build-args`;
4. runs the image's `orca config check` on each of `configs`;
5. tests the image: its entrypoint is `/opt/orca/bin/orca` and runs, every
   binary and library finds what it links, and every pipeline answers
   `describe`; then `smoke`, if given, with the image in `$IMAGE`;
6. with `push`, pushes it as `<tag>-<arch>` for each tag.

Then `manifest` joins the architectures under each tag: the commit's short
sha (7 characters), the version (a tag's name without its `v`, or
`version`), and `extra-tags`.

| Input | Default | |
|---|---|---|
| `image` | (required) | the image's repository |
| `dockerfile` | orca's `deploy/project.Dockerfile` | the project's own Dockerfile |
| `context` | `.` | the build's context |
| `configs` | `orca.toml` | files for `orca config check`, separated by spaces |
| `version` | a tag's name without `v` | the project's version |
| `version-arg` | `PROJECT_VERSION` | the build arg it's passed as (the census's: `CENSUS_VERSION`) |
| `build-args` | | more, `KEY=VALUE` a line |
| `push` | `false` | push the image |
| `extra-tags` | | tags besides the sha and the version |
| `platforms` | `arm64 amd64` | the architectures |
| `arm64-runner`, `arm64-cpu` | `ubuntu-24.04-arm`, `neoverse-n1` | |
| `amd64-runner`, `amd64-cpu` | `ubuntu-24.04`, `x86-64-v3` | |
| `orca-version` | from `pixi.lock` | names the runtime image |
| `runtime` | `ghcr.io/magmalake/orca-runtime` | the runtime's repository |
| `orca-repository`, `orca-ref` | `magmalake/orca.mojo`, `v<orca-version>` | where the template comes from |
| `build-runtime` | `false` | build the runtime from `orca-ref` instead of pulling it |
| `prepare`, `smoke` | | shell commands, before the build and after the tests |
| `timeout-minutes` | `90` | |

Its outputs are `tags`, `version` and `pushed`. The runtime package must be
public for the build to pull it without logging in. The first orca release
with a runtime image is the first a project can pin for the template; a
project with its own Dockerfile can call it with any.

## `project-test.yml`

On each of `runners` (`["ubuntu-24.04-arm", "ubuntu-24.04"]`): git's
background maintenance off, then setup-pixi with pixi `v0.78.0`, then
`pixi run <task>` for each of `tasks` (`test`), in `environment` if given.
Git maintenance goes off before setup-pixi restores rattler's git cache,
because in a cached repository it races rattler's `git clone --local`
(mojo-census#135). Pull requests read pixi's cache and don't write it.
`prepare` runs before pixi installs, and may write the manifest
(`manifest-path`, `pixi.toml`); `timeout-minutes` is 60.

## `project-release.yml`

For a tag: the version's section of `CHANGELOG.md` (`changelog`), as
`shelf release apply` writes it (`## <version> - <date>`), plus a line
naming `image`'s version and short-sha tags, becomes the release's notes;
the title is `<repository name> <version>` (`title`). A release that exists
already gets the notes again. It needs `contents: write`. `version` and
`tag` default to the pushed tag; `dry-run` writes the notes to the job's
summary instead.

## orca's own CI

`ci.yml`'s `project-image`, `project-test` and `project-release` jobs call
the three workflows on a scratch project
([`tests/example-project.sh`](../tests/example-project.sh)), which adds the
PR's commit of orca-mojo, `examples/toy.mojo` and an orca.toml. The image is
built on a runtime built from the same commit (`build-runtime`); nothing is
pushed, and the release is a dry run.

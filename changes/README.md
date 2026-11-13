# Changelog fragments

Each PR adds its changelog entry here as one file instead of editing
CHANGELOG.md, so open PRs don't conflict there:

```sh
shelf changes new <pr>-<slug> --bump minor   # writes changes/<pr>-<slug>.minor.md
```

The file holds the entry as it will read in the changelog: one bullet, a bold
title and at most one plain sentence on what changed for a user (say so if
upgrading needs an action), then the PR number, `(#123)`, in one or two lines.
Details belong in the PR. A PR with several changes may add a bullet for each.
The level comes from the file name (`.patch.md`, `.minor.md`, `.breaking.md`;
none is a patch). `shelf release apply` writes the fragments under the new
version in CHANGELOG.md and deletes them; this README stays, which is how it
knows the repository keeps fragments. CI runs `shelf changes check`: a PR that
changes code needs a fragment, unless it's labelled `no-changelog`.

A change that breaks the public API (the modules `src/morca/__init__.mojo`
lists: removing or renaming a name, changing a signature or what it does)
needs a `.breaking.md` fragment that says what to change. A change only
inside `morca._internal` never does, however large.

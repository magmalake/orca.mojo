# Changelog fragments

Each PR adds its changelog entry here as one file instead of editing
CHANGELOG.md, so open PRs don't conflict there:

```sh
shelf changes new <pr>-<slug> --bump minor   # writes changes/<pr>-<slug>.minor.md
```

The file holds the entry as it will read in the changelog (a `- **Lead
sentence.** …` bullet, more than one if the PR has several). The level comes
from the file name (`.patch.md`, `.minor.md`, `.breaking.md`; none is a
patch). `shelf release apply` writes the fragments under the new version in
CHANGELOG.md and deletes them; this README stays, which is how it knows the
repository keeps fragments. CI runs `shelf changes check`: a PR that changes
code needs a fragment, unless it's labelled `no-changelog`.

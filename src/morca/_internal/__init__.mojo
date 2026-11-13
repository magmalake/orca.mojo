"""Internals (no stability promise): everything of morca's that isn't its
public API.

The scheduler, the run stores and their SQL (`morca._internal.db`), the
runners (local, Kubernetes, EC2, Hetzner, worker pools), the web UI and its
HTTP API, the command line and the cloud tooling live here. They change
between releases without a changelog note. A pipeline imports from `morca`
and `morca.tables` (and `morca.keyed`, `morca.civil`), which `morca`'s
docstring lists with their stability promise; an import from
`morca._internal` is visibly one that can break.
"""

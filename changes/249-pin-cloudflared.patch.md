- **The Hetzner stack's tunnel runs a pinned cloudflared.** `CLOUDFLARED_IMAGE`
  defaults to `cloudflare/cloudflared:2026.10.7` instead of `latest`, which
  `orca-stack apply` pulled on every deploy, so an upstream release
  restarted the tunnel on the next one. It's still overridable in
  `hetzner.env`; docs/hetzner.md#images says how to bump it.

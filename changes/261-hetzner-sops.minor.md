- **Hetzner: secret files encrypted with SOPS.** A `*_FILE` in `hetzner.env`
  named `*.sops` is decrypted on its way to the server, never to a file on
  the laptop (docs/hetzner.md#secrets-on-your-laptop-sops) (#261).

- **Hetzner: secret files encrypted with SOPS.** A secret `*_FILE` in
  `hetzner.env` named `*.sops` or `*.sops.<ext>` is decrypted with `sops -d`
  and piped to the server, never written to a file on the laptop, so the
  tokens can live in a private repository encrypted with age
  (docs/hetzner.md#secrets-on-your-laptop-sops).

# Local Postgres installation — 2026-11-09

- `brew update` ran. Postgres major versions on offer: 12 to 18. There is no `postgresql@19` formula, so
  `postgresql@18` is the newest stable major version (PLAN §5).
- `HOMEBREW_NO_INSTALL_CLEANUP=1 brew upgrade postgresql@18`: 18.1_1 → **18.6_1**. Both kegs are on disk
  (`18.1_1`, `18.6_1`), so the binaries of the already running service stay available.
- New dependencies installed by the upgrade: `icu4c@79`, `openssl@4`, `json-c`; upgraded: `ca-certificates`, `krb5`,
  `libunistring`, `gettext`.
- The Postgres service on port 5432 that belongs to other work of the owner was not touched: same process id and
  start time (2026-11-05) before and after the upgrade. It keeps running 18.1 until the owner restarts it.
- `/opt/homebrew/opt/postgresql@18/bin/postgres --version` → `postgres (PostgreSQL) 18.6 (Homebrew)`.
- Before `tools/localpg` existed, nothing of this build listened on ports 55432–55449.
- `brew update` printed an error about an unrelated tap that no longer exists upstream (a fonts cask tap). It is not
  part of this build and was left alone.

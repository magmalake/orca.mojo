"""morca's SQL: every query on the `orca` schema, behind typed functions.

Each module owns the queries of one table or concern, and each function
takes typed arguments and returns typed rows (structs), counts or ids,
never a result set. A function runs on the connection or the transaction
its caller passes (`mut conn: Connection` or `mut tx: Transaction`), so
the caller keeps deciding what is one transaction and what it locks; a
query run both ways has a function for each, over one SQL text.

| Module | Owns |
|---|---|
| `morca.db.watchdog` | `orca.watchdog_rules`, `orca.watchdog_trips` and the rules' readings |
| `morca.db.pipelines` | `orca.pipelines`' paused flag and names |
| `morca.db.audit` | `orca.audit` |
| `morca.db.system` | `orca.system_minutes`, `orca.system_containers` |
| `morca.db.fleet` | `orca.fleet_samples`, `orca.fleet_hourly` and the fleet page's reads |

**Layering.** These modules import only the standard library, `postgres`
and each other: never a module of morca's that runs queries through them,
so nothing above can import its way back in. The rows a query returns are
declared here, and the modules that used to declare them import them from
here (so `from morca.watchdog import Lease` keeps working).

`tests/sql_in_db.sh` (CI's format job) fails when SQL appears in a string
outside `src/morca/db/` and `src/morca/migrations.mojo`, but in the files
`tests/sql_allow.txt` still lists; that list only shrinks.
"""

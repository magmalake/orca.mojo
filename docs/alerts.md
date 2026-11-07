# Alerts to your phone

Problems otherwise show only in the UI. With an [ntfy](https://ntfy.sh)
topic set, the scheduler pushes the important ones to the ntfy phone app,
each with a link into the UI. The code is `src/orca/notify.mojo` (sending)
and `src/orca/alerts.mojo` (what is sent).

```sh
export ORCA_NOTIFY_URL=https://ntfy.sh/$(openssl rand -hex 16)   # subscribe to this topic in the app
export ORCA_UI_URL=https://orca.example.com                      # where the notifications link to
orca notify test                                                 # the phone shows "orca: test alert"
```

Anyone who knows the topic can read what is sent to it, so treat its name
as a secret. On the Hetzner server, `NTFY_TOPIC_FILE` in `hetzner.env` does
this for you: see [hetzner.md](hetzner.md#alerts-on-your-phone-ntfy).

## What is sent

| Kind | When | Priority |
|---|---|---|
| `watchdog` | a [watchdog](watchdog.md) rule tripped and paused pipelines | high; a low "resolved" once it is acknowledged or its pipelines are unpaused |
| `run-failed` | a run failed after its last retry | default |
| `fill-stalled` | a [fill](orca-toml.md#fills) has had no fill run finish in `ORCA_NOTIFY_FILL_STALL_HOURS` (2) | high; "moving again" once it is |
| `disk` | the control host's fullest disk is over `ORCA_NOTIFY_DISK_PERCENT` (85) | high; "fine again" once it is under |
| `worker-lost` | a [remote worker](workers.md) holding runs has been quiet for `ORCA_NOTIFY_WORKER_SECONDS` (300) | default |
| anything | a script ran `orca notify --kind <kind> ...` | as given |

Each is one POST of plain text with ntfy's `Title`, `Priority`, `Tags` and
`Click` (the run's page or Ops › Control on `ORCA_UI_URL`) headers.

The same alert, by kind and subject, is sent at most once per
`ORCA_NOTIFY_MIN_SECONDS` (1800). What was sent is kept in Postgres
(`orca.notifications`), so a restart doesn't send it again. A send that
fails (ntfy down, a timeout) is logged without the URL and tried again on a
later tick, after a wait that doubles from 30 seconds to 15 minutes. It
never blocks or fails the scheduler.

## Settings

The scheduler reads these. None is logged or put in a message.

| Variable | Default | |
|---|---|---|
| `ORCA_NOTIFY_URL` | unset: no alerts | a topic URL, or just a server (`https://ntfy.example.com`) when the topic comes from the secret |
| `ORCA_NOTIFY_SECRET` | unset | the id of a secret holding the topic (a name, or a whole URL), read with `ORCA_SECRET_FETCH` |
| `ORCA_NOTIFY_TOKEN`, `ORCA_NOTIFY_TOKEN_SECRET` | unset | an ntfy access token for a protected topic, as a value or a secret's id |
| `ORCA_UI_URL` | unset: no link | the UI's public address |
| `ORCA_NOTIFY_MIN_SECONDS` | 1800 | how long the same alert is held back |
| `ORCA_NOTIFY_TIMEOUT_MS` | 5000 | how long one send may take |
| `ORCA_NOTIFY_CHECK_SECONDS` | 10 | how often the scheduler checks disk, workers, fills and ended trips |
| `ORCA_NOTIFY_FILL_STALL_HOURS` | 2 | see `fill-stalled` |
| `ORCA_NOTIFY_DISK_PERCENT` | 85 | see `disk` |
| `ORCA_NOTIFY_WORKER_SECONDS` | 300 | see `worker-lost` |

With only `ORCA_NOTIFY_SECRET` set, a topic name is sent to
`https://ntfy.sh`.

## From a script

```sh
orca notify --kind backup-failed --subject nightly --priority high "pg_dump failed: $err"
```

`orca notify --kind K [--subject S] [--title T] [--priority high|default|low]
[--tags a,b] [--click URL] [--resolved] [message ...]` sends an alert held
back like the scheduler's. The Hetzner server's nightly `orca-stack backup`
calls it when the backup fails.

## Where it shows

Ops › Control and `/api/system` (`notify`) say whether alerts are on and
list the last ones sent.

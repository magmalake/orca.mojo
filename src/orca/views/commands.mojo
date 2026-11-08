"""The `orca` command lines the pages show beside their buttons, so what
the UI does can be done from a shell, quoted for `sh`.
"""

from orca.views.runs import RunRow


def cancel_cli(r: RunRow) -> String:
    return String(
        "orca cancel ",
        shell_quote(r.pipeline),
        " ",
        shell_quote(r.partitions),
        "  (its queued, running and retrying runs over these partitions)",
    )


def retry_cli(r: RunRow) -> String:
    return String(
        "<",
        r.pipeline,
        " binary> resume ",
        r.id,
        "  (in that shell's process; orca has no command that queues it)",
    )


def shell_quote(s: StringSlice) -> String:
    """`s` as one shell word: as it is if it's only letters, digits and
    `-_.,:/@=+%`, else in single quotes."""
    var plain = s.byte_length() > 0
    for b in s.as_bytes():
        var c = Int(b)
        if not (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 45
            or c == 95
            or c == 46
            or c == 44
            or c == 58
            or c == 47
            or c == 64
            or c == 61
            or c == 43
            or c == 37
        ):
            plain = False
            break
    if plain:
        return String(s)
    return String("'", String(s).replace("'", "'\\''"), "'")


def backfill_cmd(name: String, keys: String, rerun: Bool) -> String:
    """`orca backfill [--rerun] <pipeline> <keys>`, as `orca` reads it."""
    return String(
        "orca backfill ",
        "--rerun " if rerun else "",
        shell_quote(name),
        " ",
        shell_quote(keys) if keys else String("<range>"),
    )


def remote_cmd(cmd: String, url: String = "<url>") -> String:
    """`cmd`, an `orca ...` line, against the UI at `url` from anywhere:
    `orca login <url> && orca --remote <url> ...`. The pages show `<url>`,
    which their script makes the address the page was loaded from."""
    return String(
        "orca login ", url, " && orca --remote ", url, String(cmd[byte=4:])
    )


def run_cmd(binary: String, keys: String) -> String:
    """The binary's `run --partitions <keys>`: one run, in that process."""
    return String(
        shell_quote(binary),
        " run --partitions ",
        shell_quote(keys) if keys else String("<range>"),
    )


def rebuild_cmd(dataset: String, keys: String) -> String:
    """`orca rebuild <dataset> <keys>`, as `orca` reads it."""
    return String(
        "orca rebuild ",
        shell_quote(dataset),
        " ",
        shell_quote(keys) if keys else String("<range>"),
    )

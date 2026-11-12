"""Where a pipeline binary's code came from: its git repository and commit,
given at build time.

    mojo build -D ORCA_SOURCE=https://github.com/me/lake \\
        -D ORCA_COMMIT=$(git rev-parse HEAD) \\
        -D ORCA_DIRTY=$(git diff --quiet HEAD && echo 0 || echo 1) ...

`describe` reports them, `orca register` and each run record them as a
version of the pipeline (`orca.pipeline_versions`), and `orca ui` links a
run to the commit it ran. A binary built without them has an empty source
and is shown as such.
"""

from std.sys import get_defined_string


def source_repo() -> String:
    return String(get_defined_string["ORCA_SOURCE", ""]())


def source_commit() -> String:
    """The commit, or "" if what was defined isn't hex. `mojo build -D`
    reads a value of digits alone as a number, which a real SHA, with 40
    characters, practically never is."""
    var c = String(get_defined_string["ORCA_COMMIT", ""]())
    return c if is_hex(c) else String()


def is_hex(s: String) -> Bool:
    for b in s.as_bytes():
        var digit = b >= UInt8(ord("0")) and b <= UInt8(ord("9"))
        if not digit and not (b >= UInt8(ord("a")) and b <= UInt8(ord("f"))):
            return False
    return True


def source_dirty() -> Bool:
    """Whether the tree it was built from had changes the commit lacks."""
    comptime d = get_defined_string["ORCA_DIRTY", "0"]()
    return d != "0" and d != "" and d != "false"


def web_url(repo: String) -> String:
    """`repo`'s page, for the forms a remote is written in: https with or
    without `.git`, and `git@host:owner/name`; "" for anything else."""
    var r = String(repo.strip())
    if r.startswith("git@"):
        var colon = r.find(":")
        if colon < 0:
            return ""
        var ssh = String(
            "https://", r[byte=4:colon], "/", r[byte = colon + 1 :]
        )
        r = ssh^
    elif r.startswith("ssh://git@"):
        var ssh = String("https://", r[byte=10:])
        r = ssh^
    if not r.startswith("https://") and not r.startswith("http://"):
        return ""
    if r.endswith(".git"):
        var bare = String(r[byte = : r.byte_length() - 4])
        r = bare^
    return String(r.rstrip("/"))


def commit_url(repo: String, commit: String) -> String:
    """The commit's tree on its host, or "" when either is unknown. GitHub,
    GitLab and Gitea all serve a tree at `<repo>/tree/<sha>`."""
    var web = web_url(repo)
    if not web or not commit:
        return ""
    if not is_hex(commit):
        return ""
    return String(web, "/tree/", commit)

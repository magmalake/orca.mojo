"""The login page (`/login`, `morca._internal.web.auth`)."""

from morca._internal.html import Html
from morca._internal.json import quote
from morca._internal.ui.layout import page_html
from morca._internal.web.page import Page


def safe_next(next: String) -> String:
    """Where to go after logging in: a path on this site, else `/`."""
    if next.startswith("/") and not next.startswith("//") and "\\" not in next:
        return next
    return "/"


def login_page(next: String, why: String, status: Int = 200) -> Page:
    var h = Html()
    h.raw('<div class="auth"><h1>Log in</h1>')
    if why:
        h.raw('<p class="error">')
        h.text(why)
        h.raw("</p>")
    h.raw('<form class="login" method="post" action="/login">')
    h.raw('<input type="hidden" name="next" value="')
    h.text(safe_next(next))
    h.raw(
        '"><label>Name <input name="name" autocomplete="username"',
        ' required></label><label>Password <input type="password"',
        ' name="password" autocomplete="current-password" required></label>',
        '<button type="submit">Log in</button></form>',
        '<p class="muted">Reading needs no login; starting, resuming,',
        " rerunning, backfilling, cancelling and rebuilding do. Users are",
        " added with <code>morca user add</code>.</p></div>",
    )
    return Page(
        status,
        String('{"error": ', quote(why), "}") if why else String(
            '{"login": "/login"}'
        ),
        page_html("Log in", "", h),
    )

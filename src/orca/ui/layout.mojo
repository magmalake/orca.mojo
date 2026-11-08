"""Every page's frame (`page_html`): its head, the nav, the slots the
server fills in with who's logged in (`orca.web.server`), and the scripts
every page has (`_POLL` for live fragments, `_TABS`, `_TIPS`, `_PEEK` for
code on hover); and what every page draws with: cards, facts, help, action
buttons, and the 404 and 400 pages.
"""

from std.os import getenv

from orca.html import Html, escape
from orca.json import quote
from orca.ui.step_cards import STEP_CARDS_JS
from orca.ui.terms import header_help
from orca.views.pipelines import comma_list
from orca.web.page import Page


def docs_link(anchor: StaticString) -> String:
    """A small link to the docs' section, beside a heading."""
    var base = getenv("ORCA_DOCS_URL", "https://mlake.dev/s/orca/docs/")
    return String(
        ' <a class="doc" href="',
        escape(String(base, "#", anchor)),
        '" target="_blank" rel="noopener">docs</a>',
    )


comptime _POLL = """
const ago = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
const full = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "long" });
function relative(t) {
  const s = (t - Date.now()) / 1000, a = Math.abs(s);
  if (a < 45) return ago.format(Math.round(s), "second");
  if (a < 2700) return ago.format(Math.round(s / 60), "minute");
  if (a < 79200) return ago.format(Math.round(s / 3600), "hour");
  if (a < 2592000) return ago.format(Math.round(s / 86400), "day");
  if (a < 31536000) return ago.format(Math.round(s / 2592000), "month");
  return ago.format(Math.round(s / 31536000), "year");
}
function times(root) {
  for (const el of root.querySelectorAll("time.rel")) {
    const t = Date.parse(el.dateTime);
    if (isNaN(t)) continue;
    el.textContent = relative(t);
    el.title = full.format(t) + " (" + el.dateTime + ")";
  }
}
times(document);
setInterval(() => times(document), 30000);
document.addEventListener("submit", (e) => {
  const m = e.target.dataset && e.target.dataset.confirm;
  const get = e.submitter && e.submitter.formMethod === "get";
  if (m && !get && !confirm(m)) e.preventDefault();
});
document.addEventListener("change", (e) => {
  const t = e.target;
  if (!t.matches || !t.matches("input.pick__all")) return;
  for (const x of t.closest("table").querySelectorAll('input[name="run"]')) x.checked = t.checked;
});
document.addEventListener("keydown", (e) => {
  const t = e.target;
  if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
  if (t.closest && t.closest("input, textarea, select, [contenteditable]")) return;
  const a = document.querySelector(e.key === "ArrowLeft" ? 'a[rel="prev"]' : e.key === "ArrowRight" ? 'a[rel="next"]' : "x-none");
  if (a) a.click();
});
const sel = (el) => {
  const s = document.getSelection(), f = document.activeElement;
  if (f && el.contains(f) && f.matches("input, textarea, select")) return true;
  return s && !s.isCollapsed && el.contains(s.anchorNode);
};
const rowKey = (tr) => { const a = tr.querySelector("a[href]"); return a ? a.getAttribute("href") : null; };
const firstSeen = (el) => {
  for (const tr of el.querySelectorAll("tbody tr")) {
    const b = tr.getBoundingClientRect();
    if (b.bottom > 0) return b.top < window.innerHeight ? [rowKey(tr), b.top] : null;
  }
  return null;
};
for (const el of document.querySelectorAll("[data-poll]")) {
  let every = (parseFloat(el.dataset.pollEvery) || 2) * 1000;
  let timer = 0, last = null, done = false, bad = 0;
  const swap = (html) => {
    if (html === last || sel(el)) return;
    last = html;
    const open = [...el.querySelectorAll("details")].map((d) => d.open);
    const pos = [...el.querySelectorAll(".scroll, pre")].map((x) => [x.scrollLeft, x.scrollTop]);
    const seen = el.getBoundingClientRect().top < 0 ? firstSeen(el) : null;
    const ticked = new Set([...el.querySelectorAll('input[name="run"]:checked')].map((x) => x.value));
    el.innerHTML = html;
    el.querySelectorAll('input[name="run"]').forEach((x) => { if (ticked.has(x.value)) x.checked = true; });
    el.querySelectorAll("details").forEach((d, i) => { if (open[i]) d.open = true; });
    el.querySelectorAll(".scroll, pre").forEach((x, i) => {
      if (pos[i]) { x.scrollLeft = pos[i][0]; x.scrollTop = pos[i][1]; }
    });
    if (seen && seen[0]) {
      const tr = [...el.querySelectorAll("tbody tr")].find((t) => rowKey(t) === seen[0]);
      if (tr) window.scrollBy(0, tr.getBoundingClientRect().top - seen[1]);
    }
    times(el);
    el.dispatchEvent(new Event("orca:swap", { bubbles: true }));
  };
  const tick = async () => {
    timer = 0;
    if (done || document.visibilityState === "hidden") return;
    let r;
    try {
      r = await fetch(el.dataset.poll);
      bad = 0;
      const n = parseFloat(r.headers.get("X-Poll-Every"));
      if (n > 0) every = n * 1000;
      if (r.ok) swap(await r.text());
      if (r.status === 286) { done = true; return; }
      timer = setTimeout(tick, every);
    } catch (e) { timer = setTimeout(tick, 5000 * Math.min(2 ** bad++, 12)); }
  };
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && !timer && !done) tick();
  });
  timer = setTimeout(tick, every);
}
"""
"""What makes a page live: each element with `data-poll` is replaced every
two seconds (`data-poll-every` seconds) by what its path returns, until that answers 286 (htmx's "stop
polling"), which a fragment does once what it shows has ended. A fragment's
`X-Poll-Every` header sets the seconds until the next ask (a runs list's
newest page asks often while a run goes on, slowly otherwise). It waits while the tab is hidden and
asks at once when it's shown again, backs off when a request fails, leaves
the page alone when nothing changed, text in it is selected or a field in
it has the focus, and keeps
open `<details>` and scrolled boxes as they were, and the table row at the
top of the window where it was when rows are added above it (a row is
known by its first link) and the runs ticked for a bulk action ticked,
and tells the page
(an `orca:swap` event, so `_TABS` shows the tab chosen again). Each
`<time class="rel">` reads as how long ago it was, in the viewer's locale,
with the full local time in its tooltip; without scripts it stays UTC."""


comptime _TABS = """
(() => {
  const get = (k) => { try { return localStorage.getItem(k); } catch (e) { return null; } };
  const put = (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} };
  const chosen = {};
  const show = (sec, v, save) => {
    const tabs = [...sec.querySelectorAll("[role=tab][data-view]")];
    if (!tabs.some((a) => a.dataset.view === v)) return;
    for (const a of tabs) {
      a.setAttribute("aria-selected", String(a.dataset.view === v));
      const panel = document.getElementById(a.getAttribute("aria-controls"));
      if (panel) panel.hidden = a.dataset.view !== v;
    }
    if (save) { chosen[sec.dataset.tabs] = v; put(sec.dataset.tabs, v); }
  };
  const init = (root) => {
    for (const sec of root.querySelectorAll("[data-tabs]")) {
      const asked = new URLSearchParams(location.search).get(sec.dataset.tabsParam);
      show(sec, chosen[sec.dataset.tabs] || asked || get(sec.dataset.tabs), false);
    }
  };
  document.addEventListener("click", (e) => {
    const a = e.target.closest && e.target.closest("[data-tabs] [role=tab][data-view]");
    if (!a) return;
    e.preventDefault();
    show(a.closest("[data-tabs]"), a.dataset.view, true);
  });
  document.addEventListener("orca:swap", (e) => init(e.target));
  init(document);
})();
"""
"""Tabs that switch without loading the page (`data-tabs`, the key their
choice is kept under in `localStorage`, so it's remembered per viewer for
every page that has them): each `role=tab` link with a `data-view` shows
the panel it `aria-controls`. A link's `?<data-tabs-param>=` still says
which tab to show, and without scripts the links load it."""


comptime _TIPS = """
(() => {
  let tip = null, on = null;
  const show = (el) => {
    if (!tip) {
      tip = document.createElement("div");
      tip.className = "help__tip tip";
      tip.id = "tip";
      tip.setAttribute("role", "tooltip");
      document.body.append(tip);
    }
    on = el;
    tip.textContent = el.dataset.tip;
    tip.style.display = "block";
    el.setAttribute("aria-describedby", "tip");
    const r = el.getBoundingClientRect(), w = tip.offsetWidth, h = tip.offsetHeight;
    const below = r.bottom + 6 + h <= innerHeight || r.top < h + 6;
    tip.style.top = (below ? r.bottom + 6 : r.top - h - 6) + "px";
    tip.style.left = Math.max(8, Math.min(r.left, innerWidth - w - 8)) + "px";
  };
  const hide = () => {
    if (tip) tip.style.display = "none";
    if (on) on.removeAttribute("aria-describedby");
    on = null;
  };
  const of = (t) => t && t.closest && t.closest("[data-tip]");
  document.addEventListener("mouseover", (e) => { const el = of(e.target); if (el && el !== on) show(el); });
  document.addEventListener("mouseout", (e) => { const el = of(e.target); if (el && !el.contains(e.relatedTarget)) hide(); });
  document.addEventListener("focusin", (e) => { const el = of(e.target); if (el) show(el); });
  document.addEventListener("focusout", (e) => { if (of(e.target)) hide(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") hide(); });
  addEventListener("scroll", hide, true);
  // A "?" in a table, which scrolls sideways: its help is fixed (CSS), so
  // not cut off by the table's box, and placed here under its button.
  const place = (h) => {
    const t = h.querySelector(".help__tip"), b = h.querySelector(".help__btn");
    if (!t || !b) return;
    const r = b.getBoundingClientRect(), w = t.offsetWidth, ht = t.offsetHeight;
    const below = r.bottom + 6 + ht <= innerHeight || r.top < ht + 6;
    t.style.top = (below ? r.bottom + 6 : r.top - ht - 6) + "px";
    t.style.left = Math.max(8, Math.min(r.left - 10, innerWidth - w - 8)) + "px";
  };
  const help = (e) => { const h = e.target.closest && e.target.closest(".scroll .help"); if (h) requestAnimationFrame(() => place(h)); };
  document.addEventListener("mouseover", help);
  document.addEventListener("focusin", help);
})();
"""
"""What a value from a fixed set means (`data-tip`, `orca.ui.terms`): one
floating box, shown under the value on hover, keyboard focus (each such
value is a tab stop) or a tap, above it where there's no room, and
hidden on scroll or Escape; to a screen reader, the value's description.
And a table header's or cell's "?", whose help is placed under it."""


comptime _PEEK = """
(() => {
  const GH = /^https:[/][/]github[.]com[/]([^/]+)[/]([^/]+)[/]blob[/]([0-9a-f]{7,40})[/]([^#?]+)#L([0-9]+)$/;
  const MAX = 24;
  const files = new Map();
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const lead = (l) => l.length - l.trimStart().length;
  const Q3 = '"'.repeat(3);
  let box = null, cur = null, showT = 0, hideT = 0;
  // The file, coloured by orca (/code): its lines and their HTML.
  const load = (url) => {
    if (!files.has(url)) files.set(url, fetch("/code?u=" + encodeURIComponent(url)).then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); }));
    return files.get(url);
  };
  // From the def (and its decorators) through its docstring and body, up
  // to MAX lines; it ends at the next line no deeper than the def's.
  function snippet(lines, at) {
    let from = at - 1;
    while (from > 0 && lines[from - 1].trimStart().startsWith("@")) from--;
    const indent = lead(lines[at - 1] || "");
    const out = [];
    let doc = false, i = from;
    for (; i < lines.length && out.length < MAX; i++) {
      const l = lines[i];
      if (doc) { out.push([i + 1, l]); if (l.includes(Q3)) doc = false; continue; }
      if (i > at - 1 && l.trim() && lead(l) <= indent && !")]}".includes(l.trimStart()[0])) {
        if (!l.trimStart().startsWith(Q3)) break;
        doc = l.split(Q3).length < 3;
      }
      out.push([i + 1, l]);
    }
    while (out.length && !out[out.length - 1][1].trim()) out.pop();
    return { out, more: i < lines.length && out.length >= MAX };
  }
  function place(a) {
    const r = a.getBoundingClientRect(), h = box.offsetHeight, w = box.offsetWidth;
    const below = r.bottom + 6 + h <= innerHeight || r.top < h + 6;
    box.style.top = (scrollY + (below ? r.bottom + 6 : r.top - h - 6)) + "px";
    box.style.left = (scrollX + Math.max(8, Math.min(r.left, innerWidth - w - 8))) + "px";
  }
  async function show(a) {
    const m = GH.exec(a.href);
    if (!m) return;
    cur = a;
    if (!box) {
      box = document.createElement("div");
      box.className = "peek";
      box.id = "peek";
      box.setAttribute("role", "tooltip");
      box.addEventListener("mouseenter", () => clearTimeout(hideT));
      box.addEventListener("mouseleave", () => hide(150));
      document.body.append(box);
    }
    const [, owner, repo, sha, path, line] = m;
    const at = Number(line);
    box.innerHTML = '<div class="peek__head">' + esc(path + ":" + at) + ' <span>at ' + esc(sha.slice(0, 7)) + "</span></div>" + '<p class="peek__msg">Loading…</p>';
    box.hidden = false;
    a.setAttribute("aria-describedby", "peek");
    place(a);
    let body;
    try {
      const src = await load("https://github.com/" + owner + "/" + repo + "/blob/" + sha + "/" + path);
      const s = snippet(src.lines, at);
      body = "<pre>" + s.out.map(([n, l]) => '<span class="peek__line' + (n === at ? " peek__line--at" : "") + '"><span class="peek__ln">' + n + "</span>" + (src.html[n - 1] ?? esc(l)) + "</span>").join("") + (s.more ? '<span class="peek__line"><span class="peek__ln"></span>…</span>' : "") + "</pre>";
    } catch (e) {
      body = '<p class="peek__msg">Can&#39;t load the source here (a private repository, or offline). The link opens it.</p>';
    }
    if (cur !== a) return;
    box.lastElementChild.outerHTML = body;
    place(a);
  }
  function hide(after) {
    clearTimeout(showT);
    clearTimeout(hideT);
    hideT = setTimeout(() => {
      if (box) box.hidden = true;
      if (cur) cur.removeAttribute("aria-describedby");
      cur = null;
    }, after);
  }
  const link = (t) => { const a = t.closest && t.closest("a.src"); return a && GH.test(a.href) ? a : null; };
  document.addEventListener("mouseover", (e) => {
    const a = link(e.target);
    if (!a) return;
    if (a.title) { a.dataset.title = a.title; a.removeAttribute("title"); }
    clearTimeout(hideT);
    if (a === cur) return;
    clearTimeout(showT);
    showT = setTimeout(() => show(a), 250);
  });
  document.addEventListener("mouseout", (e) => {
    const a = link(e.target);
    if (a && !a.contains(e.relatedTarget) && !(box && box.contains(e.relatedTarget))) hide(200);
  });
  document.addEventListener("focusin", (e) => {
    const a = link(e.target);
    if (a) { clearTimeout(hideT); show(a); }
    const h = e.target.closest && e.target.closest(".help");
    if (h) h.classList.remove("help--shut");
  });
  document.addEventListener("focusout", (e) => { if (link(e.target)) hide(0); });
  document.addEventListener("mouseover", (e) => {
    const h = e.target.closest && e.target.closest(".help");
    if (h && !h.contains(e.relatedTarget)) h.classList.remove("help--shut");
  });
  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    if (cur) hide(0);
    for (const h of document.querySelectorAll(".help:hover, .help:focus-within")) h.classList.add("help--shut");
  });
})();
"""
"""Code on hover: a link to a line on GitHub (`a.src`) shows, after a
moment or on keyboard focus, that function's source, from its def through
its docstring, coloured, from `/code` (which fetches the file from
raw.githubusercontent.com and colours it, `orca.highlight`) once per file; the
link itself is unchanged, so without scripts (or for a private repository)
it still opens the file. Escape closes it and any help shown."""


comptime USER_SLOT = "<!--orca-user-->"
"""Where `serve` puts who's logged in, or a link to log in."""


comptime _MARK = (
    '<svg class="top__mark" width="22" height="22" viewBox="0 0 16 16"'
    ' aria-hidden="true"><rect width="16" height="16" rx="3"'
    ' fill="var(--lake)"/><path d="M4.5 5L8 11.5M11.5 5L8 11.5"'
    ' stroke="var(--bg)" stroke-width="1.3"/><circle cx="4.5" cy="4.5" r="2"'
    ' fill="var(--bg)"/><circle cx="11.5" cy="4.5" r="2" fill="var(--bg)"/>'
    '<circle cx="8" cy="11.5" r="2" fill="var(--bg)"/></svg>'
)
"""orca's icon, the tab's three connected nodes, in the theme's colours."""


def _extra_links(mut h: Html):
    """The deployment's own nav links after Docs, from `ORCA_UI_LINKS`:
    `label=url` entries separated by commas, e.g. `Census ↗=https://...`.
    Entries without an http(s) url are left out."""
    for entry in comma_list(getenv("ORCA_UI_LINKS")):
        var eq = entry.find("=")
        if eq <= 0:
            continue
        var label = String(entry[byte=:eq].strip())
        var url = String(entry[byte = eq + 1 :].strip())
        if not label or not (
            url.startswith("https://") or url.startswith("http://")
        ):
            continue
        h.raw('<a href="')
        h.text(url)
        h.raw('" target="_blank" rel="noopener">')
        h.text(label)
        h.raw("</a>")


comptime _ICON = (
    '<link rel="icon" href="data:image/svg+xml,%3Csvg'
    " xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Crect"
    " width='16' height='16' rx='3' fill='%231d4ed8'/%3E%3Cpath d='M4.5 5L8"
    " 11.5M11.5 5L8 11.5' stroke='%23fff' stroke-width='1.3'/%3E%3Ccircle"
    " cx='4.5' cy='4.5' r='2' fill='%23fff'/%3E%3Ccircle cx='11.5' cy='4.5'"
    " r='2' fill='%23fff'/%3E%3Ccircle cx='8' cy='11.5' r='2'"
    " fill='%23fff'/%3E%3C/svg%3E\">"
)
"""A blue DAG of three nodes as the tab icon, so orca's tab stands apart
from the census's amber bar chart."""


def _posthog(mut h: Html):
    """PostHog product analytics in the page head when `ORCA_POSTHOG_KEY` is
    a project key (`phc_` and letters, digits, `_` or `-`), nothing otherwise.

    The same snippet as the magmalake.org website: PostHog's `array.js` and
    its API go through `/ph` on the UI's own origin, a first-party proxy
    (the mojourney-ph Worker in magmalake/magmalake.org; the UI itself
    doesn't serve it), so ad-blockers keep it. `persistence: 'memory'` sets
    no cookie and writes no `localStorage`, so there is nothing to consent
    to. `site: "orca"` tells its events apart in a shared PostHog project.
    """
    var key = getenv("ORCA_POSTHOG_KEY")
    if not key.startswith("phc_"):
        return
    for byte in key.as_bytes():
        var b = Int(byte)
        if not (
            (b >= ord("a") and b <= ord("z"))
            or (b >= ord("A") and b <= ord("Z"))
            or (b >= ord("0") and b <= ord("9"))
            or b == ord("_")
            or b == ord("-")
        ):
            return
    h.raw(
        '<script defer src="/ph/static/array.js"></script>',
        '<script>window.addEventListener("DOMContentLoaded",function(){',
        'if(window.posthog){posthog.init("',
        key,
        "\",{api_host:location.origin+'/ph',",
        "ui_host:'https://us.posthog.com',defaults:'2025-05-24',",
        "persistence:'memory',capture_exceptions:true});",
        "posthog.register({site:'orca'});}});</script>",
    )


def page_html(title: String, active: StaticString, body: Html) -> String:
    var h = Html()
    h.raw(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        _ICON,
        "<title>",
    )
    h.text(title, " · orca")
    h.raw(
        '</title><link rel="stylesheet" href="/static/orca.css">',
    )
    _posthog(h)
    h.raw(
        "</head>",
        '<body class="',
        AUTH_SLOT,
        '">',
        '<header class="top"><a class="top__brand" href="/">',
        _MARK,
        "orca</a><nav>",
    )
    for item in [
        ("/", "Pipelines"),
        ("/runs", "Runs"),
        ("/datasets", "Datasets"),
    ]:
        h.raw('<a href="', item[0], '"')
        if item[0] == active:
            h.raw(' aria-current="page"')
        h.raw(">", item[1], "</a>")
    # Ops: a link to Control, with Control and Fleet under it. The menu is
    # plain links, shown by CSS (hover, keyboard focus) and, on a phone,
    # always, so it needs no JavaScript.
    h.raw('<div class="menu"><a href="/ops"')
    if active.startswith("/ops/"):
        h.raw(' aria-current="page"')
    h.raw(' aria-haspopup="true">Ops</a><div class="menu__sub">')
    for item in [
        ("/ops/control", "Control"),
        ("/ops/fleet", "Fleet"),
        ("/ops/workers", "Workers"),
    ]:
        h.raw('<a href="', item[0], '"')
        if item[0] == active:
            h.raw(' aria-current="page"')
        h.raw(">", item[1], "</a>")
    h.raw("</div></div>")
    h.raw('<a href="')
    h.text(getenv("ORCA_DOCS_URL", "https://mlake.dev/s/orca/docs/"))
    h.raw('" target="_blank" rel="noopener">Docs</a>')
    _extra_links(h)
    h.raw("</nav>", USER_SLOT, "</header><main>")
    h.raw(body.out)
    h.raw(
        "</main><script>",
        _POLL,
        "</script><script>",
        _TABS,
        "</script><script>",
        _TIPS,
        "</script><script>",
        _PEEK,
        "</script><script>",
        STEP_CARDS_JS,
        "</script></body></html>",
    )
    return h^.finish()


comptime AUTH_SLOT = "orca-auth"
"""The `<body>`'s class until the page is answered: `anon` when login is on
and no one is logged in, which hides the inline action buttons and shows
their "Log in to ..." links instead."""


comptime HERE_SLOT = "__orca_here__"
"""Where a "Log in to ..." link comes back to: the page's own path and
query, encoded, filled in when the page is answered."""


def act_button(
    mut h: Html,
    action: String,
    label: StaticString,
    verb: StaticString,
    cli: String,
    danger: Bool = False,
    step: String = "",
):
    """A small form posting to `action`, beside the status it changes: a
    button labelled `label` whose title is the command line that does the
    same, with "Log in to `verb`" in its place while login is needed. A
    `danger` one asks first, where there's JavaScript."""
    h.raw('<form class="act" method="post" action="')
    h.text(action)
    h.raw('"')
    if danger:
        h.raw(' data-confirm="')
        h.text(label, "?")
        h.raw('"')
    h.raw(">")
    if step:
        h.raw('<input type="hidden" name="step" value="')
        h.text(step)
        h.raw('">')
    h.raw('<button type="submit" class="act__btn', " danger" if danger else "")
    h.raw('" title="')
    h.text(cli)
    h.raw(
        '">', label, '</button></form><a class="act__login" href="/login?next='
    )
    h.raw(HERE_SLOT, '" title="')
    h.text(cli)
    h.raw('">Log in to ', verb, "</a>")


def help_tip(
    mut h: Html,
    id: String,
    key: String,
    text: StaticString,
    docs: StaticString = "",
):
    """A "?" after a fact's name (`Html.help`)."""
    h.help(id, key, text, docs)


def _strip_tags(s: StringSlice) -> String:
    """`s`'s text: its tags dropped, and a sort arrow (▲ ▼)."""
    var out = String()
    var inside = False
    for cp in s.codepoint_slices():
        if cp == "<":
            inside = True
        elif cp == ">":
            inside = False
        elif not inside:
            out.write(cp)
    return String(out.replace("▲", "").replace("▼", "").strip())


def with_header_help(html: String, prefix: String) -> String:
    """`html` with a "?" in every table header `orca.ui.terms` has help for,
    by its text (and the table its `data-help` names), as `help_tip` writes
    one; ids `help-<prefix>-<n>`. One pass over the page, so every table,
    whoever wrote it, gets them."""
    var out = String()
    var at = 0
    var n = 0
    while True:
        var open = html.find("<th", at)
        if open < 0:
            break
        var gt = html.find(">", open)
        var close = html.find("</th>", open)
        if gt < 0 or close < 0 or gt > close:
            break
        # Only <th> and <th ...>, not <thead>.
        var next = String(html[byte = open + 3 : open + 4])
        if next != ">" and next != " ":
            out.write(html[byte = at : open + 3])
            at = open + 3
            continue
        var tag = String(html[byte=open:gt])
        var inner = String(html[byte = gt + 1 : close])
        var table = String()
        var dh = tag.find('data-help="')
        if dh >= 0:
            var q = tag.find('"', dh + 11)
            table = String(tag[byte = dh + 11 : q])
        var label = _strip_tags(inner)
        var text = header_help(label, table)
        out.write(html[byte=at:close])
        if text and 'class="help"' not in inner:
            var h = Html()
            help_tip(h, String(prefix, "-", n), label, text)
            out.write(h^.finish())
            n += 1
        at = close
    out.write(html[byte=at:])
    return out^


def fact_term(
    mut h: Html,
    key: StaticString,
    id: StaticString,
    help: StaticString,
    docs: StaticString = "",
):
    """A fact's `<dt>`: its name with its help."""
    h.raw("<dt>", key)
    help_tip(h, id, key, help, docs)
    h.raw("</dt>")


def fact_item(
    mut h: Html,
    key: StaticString,
    id: StaticString,
    help: StaticString,
    docs: StaticString = "",
):
    """A fact's `<dt>`, its name with its help, then its `<dd>` opened."""
    fact_term(h, key, id, help, docs)
    h.raw("<dd>")


comptime LIVE_EVERY = 5
"""Seconds between refreshes of a list with a run going on."""


comptime IDLE_EVERY = 30
"""Seconds between refreshes of a list's newest page with nothing going on:
slow, only to show the runs a schedule or sensor starts."""


def card_open(
    mut h: Html,
    id: StaticString,
    title: StaticString,
    help: StaticString,
    docs: StaticString = "",
    facts: Bool = True,
):
    """A run page's card opened: its anchor `id`, then its heading with a
    "?" (`help`, its id `card-<id>`), closed. The caller ends the
    `<section>`."""
    h.raw(
        '<section class="card',
        " card--facts" if facts else "",
        '" id="',
        id,
        '"><h2>',
        title,
    )
    help_tip(h, String("card-", id), title, help, docs)
    h.raw("</h2>")


def not_found(what: String) -> Page:
    var h = Html()
    h.raw("<h1>Not found</h1><p>")
    h.text(what)
    h.raw("</p>")
    return Page(
        404,
        String('{"error": ', quote(what), "}"),
        page_html("Not found", "", h),
    )


def bad_request(why: String) -> Page:
    """A 400: the form asked for something the UI never does."""
    var h = Html()
    h.raw("<h1>Refused</h1><p>")
    h.text(why)
    h.raw("</p>")
    return Page(
        400, String('{"error": ', quote(why), "}"), page_html("Refused", "", h)
    )

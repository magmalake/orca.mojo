"""The stylesheet `orca ui` serves at `/static/orca.css`: magmalake.dev's
colours and type, light and dark, and system fonts (nothing is fetched)."""

comptime STYLE = """:root {
  --bg: #fbf9f7; --surface: #ffffff; --border: #e3dcd4;
  --text: #17151a; --muted: #6b645d; --faint: #948c84;
  --ember: #c33c10; --lake: #0d6a70; --ok: #1f7a3d; --bad: #b42318;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  --sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  color-scheme: light dark;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0e0d11; --surface: #17161d; --border: #2a2833;
    --text: #eeeae4; --muted: #a49d95; --faint: #7b746d;
    --ember: #ff7a45; --lake: #4ecdc4; --ok: #5fd08a; --bad: #ff7b72;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 15px/1.5 var(--sans); }
a { color: var(--lake); }
code, pre { font-family: var(--mono); font-size: 0.9em; }
pre { white-space: pre-wrap; }
.top { display: flex; align-items: baseline; gap: 1.5rem; padding: 0.8rem 1rem;
  border-bottom: 1px solid var(--border); background: var(--surface); }
.top__brand { font-weight: 700; color: var(--ember); text-decoration: none; }
.top nav { display: flex; gap: 1rem; }
.top nav a { color: var(--muted); text-decoration: none; }
.top nav a[aria-current] { color: var(--text); font-weight: 600; }
main { max-width: 72rem; margin: 0 auto; padding: 1.5rem 1rem 3rem; }
h1 { font-size: 1.6rem; margin: 0.2rem 0 1rem; }
h2 { font-size: 1.15rem; margin: 2rem 0 0.6rem; }
.crumb { margin: 0; color: var(--muted); font-size: 0.875rem; }
.muted, .empty { color: var(--muted); }
.more { font-size: 0.875rem; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; background: var(--surface);
  border: 1px solid var(--border); font-size: 0.875rem; }
th, td { text-align: left; padding: 0.45rem 0.7rem; border-bottom: 1px solid var(--border);
  vertical-align: top; }
th { color: var(--faint); font-weight: 600; font-size: 0.75rem; text-transform: uppercase;
  letter-spacing: 0.05em; }
td { white-space: nowrap; }
td.error { white-space: normal; color: var(--bad); font-family: var(--mono); font-size: 0.8rem; max-width: 32rem; }
.facts { display: grid; grid-template-columns: max-content 1fr; gap: 0.3rem 1.2rem; margin: 0; }
.facts dt { color: var(--muted); }
.facts dd { margin: 0; overflow-wrap: anywhere; }
.status { font-family: var(--mono); font-size: 0.75rem; padding: 0.05rem 0.45rem;
  border-radius: 999px; border: 1px solid currentColor; white-space: nowrap; }
.status--succeeded { color: var(--ok); }
.status--failed { color: var(--bad); }
.status--running { color: var(--ember); }
.status--queued, .status--pending, .status--skipped, .status--cancelled { color: var(--faint); }
.filters { display: flex; flex-wrap: wrap; gap: 0.6rem 1rem; align-items: end; margin-bottom: 1rem; }
.filters label { display: grid; gap: 0.2rem; font-size: 0.8rem; color: var(--muted); }
input, button, select { font: inherit; padding: 0.3rem 0.5rem; border: 1px solid var(--border);
  border-radius: 4px; background: var(--surface); color: var(--text); }
button { cursor: pointer; }
button:hover { border-color: var(--lake); }
button.danger { color: var(--bad); }
button.danger:hover { border-color: var(--bad); }
.actions { display: flex; flex-wrap: wrap; gap: 0.6rem 1.5rem; align-items: end; margin: 1rem 0; }
form.action { display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: end; margin: 0; }
form.action label { display: grid; gap: 0.2rem; font-size: 0.8rem; color: var(--muted); }
pre.log { background: var(--surface); border: 1px solid var(--border); padding: 0.8rem 1rem;
  font-size: 0.8rem; line-height: 1.45; max-height: 70vh; overflow: auto; white-space: pre-wrap; }
pre.error { color: var(--bad); background: var(--surface); border: 1px solid var(--border);
  border-left: 3px solid var(--bad); padding: 0.6rem 0.9rem; }
.dag { display: block; margin: 0.5rem 0 0.5rem; font-family: var(--mono); }
.dag rect { fill: var(--surface); stroke: var(--border); stroke-width: 1.5; }
.dag__edge { fill: none; stroke: var(--faint); stroke-width: 1.5; }
.dag__head { fill: var(--faint); }
.dag__name { font-size: 13px; font-weight: 600; fill: var(--text); }
.dag__kind { font-size: 11px; fill: var(--muted); }
.dag__box--succeeded rect { stroke: var(--ok); }
.dag__box--failed rect { stroke: var(--bad); stroke-width: 2; }
.dag__box--running rect { stroke: var(--ember); stroke-width: 2; }
.dag__box--skipped rect, .dag__box--pending rect, .dag__box--queued rect,
.dag__box--cancelled rect { stroke-dasharray: 4 3; }
h2 .status { vertical-align: middle; margin-left: 0.4rem; }
"""

"""morca's pages: HTML, each page in a module of its own, over the read
models of `morca.views`; `morca.web.routes` says which path is which.

Every path answers HTML, or JSON to `Accept: application/json` or
`?format=json`, built from the same rows, so a script and a person see the
same thing (`morca.web.page.respond`). A page of something still going on
(a queued or running run, a running attempt) refreshes its live part from
a fragment path every few seconds (`_POLL` in `morca.ui.layout`), which
answers 286 once there's nothing more to show, and the page stops asking.
Beyond that, a few scripts are conveniences only: `/runs` filters as a
choice changes, Start runs updates its preview and command lines as you
type, and Cancel, rerun and rebuild ask before posting. Every form works
without them.

The run page and the pipeline page are a view (`morca.views.runs.RunView`,
`morca.views.pipelines.PipelineView`) drawn twice, as HTML here and as
JSON in `morca.api`, which `morca.web.routes` puts together. The other
pages still build their HTML and their JSON in one function, over the
rows of `morca.views`; they can be split the same way as they change.

| Module | |
|---|---|
| `morca.ui.home` | `/`: pipelines, recent runs, the timeline |
| `morca.ui.pipeline` | `/pipelines/<name>`: Overview, Runs and Operate, a key's page |
| `morca.ui.start` | Start runs, on the Operate tab |
| `morca.ui.coverage` | a pipeline's days or months and their state |
| `morca.ui.runs` | `/runs`: the runs list, a page at a time |
| `morca.ui.run` | `/runs/<id>`: a run's cards, steps, machines and files |
| `morca.ui.attempt` | an attempt's log and facts |
| `morca.ui.machines` | a run's machines and their metrics |
| `morca.ui.datasets` | datasets, a dataset, a snapshot's lineage, a rebuild |
| `morca.ui.ops` | Control, the watchdog, `/metrics`, and Fleet and Workers' redirects |
| `morca.ui.pools` | Ops' Pools: every pool's machines, a machine's page |
| `morca.ui.code` | `/code`: a file at a commit, coloured, for code on hover |
| `morca.ui.login` | the login page |
| `morca.ui.layout` | every page's frame and what pages draw with |
| `morca.ui.parts` | pieces several pages draw: cells, a runs table, facts |
| `morca.ui.step_cards` | a card per step, on hover |
| `morca.ui.terms` | what the UI's words mean: header and value help |
| `morca.ui.style` | the stylesheet, `/static/morca.css` |
| `morca.ui.viz` | the views drawn in the browser: a run's timeline, the home page's |
"""

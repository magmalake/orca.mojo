"""morca's pages: HTML, each page in a module of its own, over the read
models of `morca._internal.views`; `morca._internal.web.routes` says which path is which.

Every path answers HTML, or JSON to `Accept: application/json` or
`?format=json`, built from the same rows, so a script and a person see the
same thing (`morca._internal.web.page.respond`). A page of something still going on
(a queued or running run, a running attempt) refreshes its live part from
a fragment path every few seconds (`_POLL` in `morca._internal.ui.layout`), which
answers 286 once there's nothing more to show, and the page stops asking.
Beyond that, a few scripts are conveniences only: `/runs` filters as a
choice changes, Start runs updates its preview and command lines as you
type, and Cancel, rerun and rebuild ask before posting. Every form works
without them.

The run page and the pipeline page are a view (`morca._internal.views.runs.RunView`,
`morca._internal.views.pipelines.PipelineView`) drawn twice, as HTML here and as
JSON in `morca._internal.api`, which `morca._internal.web.routes` puts together. The other
pages still build their HTML and their JSON in one function, over the
rows of `morca._internal.views`; they can be split the same way as they change.

| Module | |
|---|---|
| `morca._internal.ui.home` | `/`: pipelines, recent runs, the timeline |
| `morca._internal.ui.pipeline` | `/pipelines/<name>`: Overview, Runs and Operate, a key's page |
| `morca._internal.ui.start` | Start runs, on the Operate tab |
| `morca._internal.ui.coverage` | a pipeline's days or months and their state |
| `morca._internal.ui.runs` | `/runs`: the runs list, a page at a time |
| `morca._internal.ui.run` | `/runs/<id>`: a run's cards, steps, machines and files |
| `morca._internal.ui.attempt` | an attempt's log and facts |
| `morca._internal.ui.machines` | a run's machines and their metrics |
| `morca._internal.ui.datasets` | datasets, a dataset, a snapshot's lineage, a rebuild |
| `morca._internal.ui.ops` | Control, the watchdog, `/metrics`, and Fleet and Workers' redirects |
| `morca._internal.ui.pools` | Ops' Pools: every pool's machines, a machine's page |
| `morca._internal.ui.code` | `/code`: a file at a commit, coloured, for code on hover |
| `morca._internal.ui.login` | the login page |
| `morca._internal.ui.layout` | every page's frame and what pages draw with |
| `morca._internal.ui.parts` | pieces several pages draw: cells, a runs table, facts |
| `morca._internal.ui.step_cards` | a card per step, on hover |
| `morca._internal.ui.terms` | what the UI's words mean: header and value help |
| `morca._internal.ui.style` | the stylesheet, `/static/morca.css` |
| `morca._internal.ui.viz` | the views drawn in the browser: a run's timeline, the home page's |
"""

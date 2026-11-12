"""orca's pages: HTML, each page in a module of its own, over the read
models of `orca.views`; `orca.web.routes` says which path is which.

Every path answers HTML, or JSON to `Accept: application/json` or
`?format=json`, built from the same rows, so a script and a person see the
same thing (`orca.web.page.respond`). A page of something still going on
(a queued or running run, a running attempt) refreshes its live part from
a fragment path every few seconds (`_POLL` in `orca.ui.layout`), which
answers 286 once there's nothing more to show, and the page stops asking.
Beyond that, a few scripts are conveniences only: `/runs` filters as a
choice changes, Start runs updates its preview and command lines as you
type, and Cancel, rerun and rebuild ask before posting. Every form works
without them.

The run page and the pipeline page are a view (`orca.views.runs.RunView`,
`orca.views.pipelines.PipelineView`) drawn twice, as HTML here and as
JSON in `orca.api`, which `orca.web.routes` puts together. The other
pages still build their HTML and their JSON in one function, over the
rows of `orca.views`; they can be split the same way as they change.

| Module | |
|---|---|
| `orca.ui.home` | `/`: pipelines, recent runs, the timeline |
| `orca.ui.pipeline` | `/pipelines/<name>`: Overview, Runs and Operate, a key's page |
| `orca.ui.start` | Start runs, on the Operate tab |
| `orca.ui.coverage` | a pipeline's days or months and their state |
| `orca.ui.runs` | `/runs`: the runs list, a page at a time |
| `orca.ui.run` | `/runs/<id>`: a run's cards, steps, machines and files |
| `orca.ui.attempt` | an attempt's log and facts |
| `orca.ui.machines` | a run's machines and their metrics |
| `orca.ui.datasets` | datasets, a dataset, a snapshot's lineage, a rebuild |
| `orca.ui.ops` | Control, the watchdog, `/metrics`, and Fleet and Workers' redirects |
| `orca.ui.pools` | Ops' Pools: every pool's machines, a machine's page |
| `orca.ui.code` | `/code`: a file at a commit, coloured, for code on hover |
| `orca.ui.login` | the login page |
| `orca.ui.layout` | every page's frame and what pages draw with |
| `orca.ui.parts` | pieces several pages draw: cells, a runs table, facts |
| `orca.ui.step_cards` | a card per step, on hover |
| `orca.ui.terms` | what the UI's words mean: header and value help |
| `orca.ui.style` | the stylesheet, `/static/orca.css` |
| `orca.ui.viz` | the views drawn in the browser: a run's timeline, the home page's |
"""

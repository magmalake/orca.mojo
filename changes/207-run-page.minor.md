- **A run's page is cards, like a pipeline's.** The list of facts at the
  top is now Status (with Resume, Rerun and Cancel), Partitions &
  trigger, Code (the pipeline, steps and tasks on one line, source on
  hover, the code hash in small print), Resources (the run's own peak
  memory and CPU) and Settings, each fact with a "?" that explains it.
  Steps is one card with Table, Timeline and Graph tabs (the table
  first, the choice remembered in the browser, `?steps=` without
  scripts), so they aren't drawn twice. Machines says what its numbers
  are: the whole machine for the local runner and control placement,
  saying so plainly when other runs shared it, the run's own instances on
  EC2, the node on Kubernetes. Each section has an anchor (`#status`,
  `#steps`, `#machines`, `#tables`, ...); the JSON is unchanged.

- **Lineage reads Upstream and Downstream.** A snapshot's page calls
  "Made from" and "Made from it" Upstream and Downstream, its tables'
  columns Written by and Read, and the run page's Snapshots card says
  read and wrote, as the dataset page's Written by and Read by do. The
  diagram has a legend and a "?" that says how to read it, and a box's
  second line is the pipeline and partition that wrote it, the snapshot,
  run and step on hover, instead of a cut-off id.

- **Graphs are drawn top to bottom.** `/datasets`' graph of pipelines and
  datasets was 5,000 px wide; it and a snapshot's lineage are now laid out
  downward, a row per level, at most two boxes to a row (about 400 px, a
  phone's width), arrows pointing down. A cycle (a pipeline reading what
  it writes) no longer pushes a box to the far end: its edge goes back up
  round the side, and an edge that skips a level goes round the boxes
  rather than through them.

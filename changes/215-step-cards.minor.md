- **A card per step, on hover.** In a pipeline's Steps card (Graph and
  Table) and a run's (Table, Timeline and Graph), hovering, focusing or
  tapping a step shows its function's signature, coloured, and its
  docstring's first paragraph, from the code at the commit (`/code?u=&at=`
  answers just the `def` and its docstring), with a link to the line; its
  kind with what it means, `after=`, input and output types; the datasets
  it reads and writes, linked; and on a pipeline's page what its attempts
  in the last 20 runs took (p50 and max), their peak memory and how many
  failed, or on a run's page that attempt's start, end, duration, status,
  peak memory, CPU and error. All but the docstring is in the page, so a
  hover asks the server nothing.

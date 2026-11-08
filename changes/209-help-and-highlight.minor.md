- **Help on every table, and on the values in it.** Every table's column
  headers have a "?" (Steps, Next runs, Runs, Versions, Machines, Fleet,
  Workers, the watchdog's, alerts' and the rest), added as each page is
  answered from one dictionary, `orca.terms`. Values from a fixed set
  (statuses, step kinds, triggers, placements, read and wrote, a
  setting's source) are dotted underlined and say what they mean on
  hover, keyboard focus or a tap, from the same dictionary, without a "?"
  in every cell. A "?" in a table that scrolls sideways is no longer cut
  off by it.

- **The Configuration table explains itself and links its sources.** Each
  key has a "?", a placement or fills value its meaning, and From links to
  the orca.toml line at the registered commit (`orca register` now keeps
  each file value's line and the file's path in its checkout) or, for a
  default from the pipeline's code, to its `Pipeline[...]` line. A flag,
  the environment and orca's own defaults have no line to link.

- **Code on hover is coloured.** `/code?u=` fetches a GitHub file at a
  commit for the UI and colours it with `orca.highlight`, a small Mojo
  tokenizer (keywords, defined names, strings, comments, numbers,
  decorators), in the theme's colours.

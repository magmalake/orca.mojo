- **Actions ask in a dialog in the UI's colours, not the browser's.**
  Cancel, Rerun, Rerun from here, Rebuild downstream, a list's bulk Cancel
  and a pipeline's Cancel queued runs ask in one `<dialog>`, light or dark,
  saying what will happen ("Cancel run 4418?" / "It leaves the queue and
  won't start."), with the safe button focused. Escape or a click outside
  is no, and the focus goes back. Without scripts the forms post as before.
  The run page's Status card Cancel and the bulk Cancel now ask too; Retry
  and Backfill again don't.
- **Code on hover on the lists.** A runs table's pipeline shows its
  declaration at the commit the run ran, a failed run names its failed step
  linked to its function, and the home page's pipelines (List and Timeline)
  show theirs at the registered code. A card shows on hover, keyboard focus
  or a first tap, survives a live refresh, and fetches nothing until a
  hover.
- **Every code card leads with its docstring's first sentence**, then the
  rest in muted text. A pipeline without a docstring of its own uses its
  module's.

"""`Ctx`: what a step gets besides its input.

Phase 1 starts with the step's identity and a logger. The catalog, pinned
scans and the run's dista runtime join it in later PRs (see DESIGN.md).
"""


struct Ctx(Movable):
    """The run, step and partition a step is running for."""

    var pipeline: String
    var run: Int
    var step: String
    var partition: String
    """The partition key, e.g. `2024-01`; empty for a gather step."""
    var attempt: Int

    def __init__(
        out self,
        pipeline: String,
        run: Int,
        step: String,
        partition: String,
        attempt: Int = 1,
    ):
        self.pipeline = pipeline
        self.run = run
        self.step = step
        self.partition = partition
        self.attempt = attempt

    def log(self, msg: String):
        """Writes a line tagged with the step and partition."""
        if self.partition:
            print("[", self.step, " ", self.partition, "] ", msg, sep="")
        else:
            print("[", self.step, "] ", msg, sep="")

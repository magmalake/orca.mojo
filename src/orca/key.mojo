"""`Key`: what a partition key is, whatever it counts.

`Month` and `Day` are keys. A key is written as text (`2024-01`,
`2024-01-31`), read back by `parse`, ordered, and has a next key, which is
what a range of keys needs. `parse_keys` reads the ranges and lists the
command line takes.
"""


trait Key(Copyable, Deinitable, Equatable, Movable, Writable):
    @staticmethod
    def parse(s: StringSlice) raises -> Self:
        """Reads the key as `write_to` writes it, and nothing else."""
        ...

    def next(self) -> Self:
        """The key after this one."""
        ...

    def __lt__(self, other: Self) -> Bool:
        ...


def parse_keys[
    K: Key
](s: StringSlice, first: StringSlice = "") raises -> List[K]:
    """Reads a key, `A..B` (both ends included), or a comma-separated list of
    either, in the order given. A key given twice, or one before `first`
    (when given), is an error."""
    var out = List[K]()
    for part in s.split(","):
        var ends = part.split("..")
        if len(ends) == 1:
            out.append(K.parse(ends[0]))
        elif len(ends) == 2:
            var k = K.parse(ends[0])
            var last = K.parse(ends[1])
            if last < k:
                raise Error("range runs backwards: '", part, "'")
            while True:
                out.append(k.copy())
                if k == last:
                    break
                k = k.next()
        else:
            raise Error("not a range: '", part, "'")
    if first:
        var f = K.parse(first)
        for i in range(len(out)):
            if out[i] < f:
                raise Error(
                    out[i], " is before the pipeline's first partition, ", f
                )
    var seen = Dict[String, Bool]()
    for i in range(len(out)):
        var text = String(out[i])
        if text in seen:
            raise Error(out[i], " given twice")
        seen[text^] = True
    return out^

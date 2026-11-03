"""`Name`: the partition key of a keyed pipeline (`orca.keyed`).

A name is 1 to 128 bytes of `[A-Za-z0-9._:-]` without two dots in a row,
such as a log's UUID. Neither a comma nor `..` can appear in one, so a list
of names on the command line or in `runs.partitions` reads back
unambiguously. Names order by their bytes. A name has no next or previous
name and falls in no date: it's a `Key` but not a `TimeKey`.
"""

from orca.key import Key

comptime MAX_NAME = 128


def _name_byte(b: Byte) -> Bool:
    return (
        (b >= UInt8(ord("a")) and b <= UInt8(ord("z")))
        or (b >= UInt8(ord("A")) and b <= UInt8(ord("Z")))
        or (b >= UInt8(ord("0")) and b <= UInt8(ord("9")))
        or b == UInt8(ord("."))
        or b == UInt8(ord("_"))
        or b == UInt8(ord(":"))
        or b == UInt8(ord("-"))
    )


struct Name(Key):
    var text: String

    def __init__(out self, var text: String):
        """A name already checked; `parse` checks one."""
        self.text = text^

    @staticmethod
    def parse(s: StringSlice) raises -> Name:
        var b = s.as_bytes()
        if len(b) == 0 or len(b) > MAX_NAME:
            raise Error(
                "not a name (1 to ",
                MAX_NAME,
                " bytes of A-Z a-z 0-9 . _ : -): '",
                s,
                "'",
            )
        for i in range(len(b)):
            if not _name_byte(b[i]) or (
                i > 0
                and b[i] == UInt8(ord("."))
                and b[i - 1] == UInt8(ord("."))
            ):
                raise Error(
                    "not a name (1 to ",
                    MAX_NAME,
                    " bytes of A-Z a-z 0-9 . _ : -,",
                    " no '..'): '",
                    s,
                    "'",
                )
        return Name(String(s))

    def __eq__(self, other: Self) -> Bool:
        return self.text == other.text

    def __lt__(self, other: Self) -> Bool:
        return self.text < other.text

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.text)


def parse_names(s: StringSlice) raises -> List[Name]:
    """A comma-separated list of names, each once. A range is refused."""
    if ".." in s:
        raise Error(
            "keyed partitions take a list of names, not a range: '", s, "'"
        )
    var out = List[Name]()
    var seen = Dict[String, Bool]()
    for part in s.split(","):
        var n = Name.parse(part.strip())
        if n.text in seen:
            raise Error(n, " given twice")
        seen[n.text] = True
        out.append(n^)
    return out^


def sort_texts(mut l: List[String]):
    """Sorts `l` in place, by bytes: a merge sort, as there can be thousands
    of keys."""
    if len(l) < 2:
        return
    var tmp = l.copy()
    var width = 1
    var n = len(l)
    while width < n:
        var i = 0
        while i < n:
            var mid = min(i + width, n)
            var hi = min(i + 2 * width, n)
            var a = i
            var b = mid
            var k = i
            while k < hi:
                if b >= hi or (a < mid and not (l[b] < l[a])):
                    tmp[k] = l[a]
                    a += 1
                else:
                    tmp[k] = l[b]
                    b += 1
                k += 1
            i += 2 * width
        for k in range(n):
            l[k] = tmp[k]
        width *= 2

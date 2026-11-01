"""`Month`: the partition key of a monthly pipeline.

A month is written `YYYY-MM`, and a range of them `2024-01..2024-12`, both
ends included. A run is over a list of months; a mapped step runs once for
each.
"""


@fieldwise_init
struct Month(Copyable, Equatable, Movable, Writable):
    """A calendar month, `month` from 1 to 12."""

    var year: Int
    var month: Int

    @staticmethod
    def parse(s: StringSlice) raises -> Month:
        """Reads `YYYY-MM`."""
        var parts = s.split("-")
        if len(parts) != 2 or parts[0].byte_length() != 4:
            raise Error("not a month (YYYY-MM): '", s, "'")
        if parts[1].byte_length() != 2:
            raise Error("not a month (YYYY-MM): '", s, "'")
        var m = Month(Int(parts[0]), Int(parts[1]))
        if m.month < 1 or m.month > 12:
            raise Error("not a month (YYYY-MM): '", s, "'")
        return m^

    def next(self) -> Month:
        """The month after this one."""
        if self.month == 12:
            return Month(self.year + 1, 1)
        return Month(self.year, self.month + 1)

    def __eq__(self, other: Self) -> Bool:
        return self.year == other.year and self.month == other.month

    def __lt__(self, other: Self) -> Bool:
        if self.year != other.year:
            return self.year < other.year
        return self.month < other.month

    def write_to(self, mut writer: Some[Writer]):
        if self.year < 1000:
            writer.write("0")
        if self.year < 100:
            writer.write("0")
        if self.year < 10:
            writer.write("0")
        writer.write(self.year, "-")
        if self.month < 10:
            writer.write("0")
        writer.write(self.month)


def parse_months(s: StringSlice) raises -> List[Month]:
    """Reads `YYYY-MM`, `YYYY-MM..YYYY-MM` (both ends included), or a
    comma-separated list of either, in the order given."""
    var out = List[Month]()
    for part in s.split(","):
        var ends = part.split("..")
        if len(ends) == 1:
            out.append(Month.parse(ends[0]))
        elif len(ends) == 2:
            var m = Month.parse(ends[0])
            var last = Month.parse(ends[1])
            if last < m:
                raise Error("month range runs backwards: '", part, "'")
            while True:
                out.append(m.copy())
                if m == last:
                    break
                m = m.next()
        else:
            raise Error("not a month range: '", part, "'")
    for i in range(len(out)):
        for j in range(i + 1, len(out)):
            if out[i] == out[j]:
                raise Error("month ", out[i], " given twice")
    return out^

"""Writing HTML: every value escaped, markup only where it's said to be.

```mojo
var h = Html()
h.raw("<td>")
h.text(step.error)        # `<script>` comes out as text
h.raw("</td>")
h.link("/runs/7", "run 7")
```

`orca ui` writes its pages through this rather than a template engine: a
page is tables of records, and writing it in Mojo keeps each value's type
until the moment it's escaped.
"""


def escape(s: StringSlice) -> String:
    """`s` with `&`, `<`, `>`, `"` and `'` as character references, safe in
    text and in a quoted attribute."""
    var out = String()
    for cp in s.codepoint_slices():
        if cp == "&":
            out.write("&amp;")
        elif cp == "<":
            out.write("&lt;")
        elif cp == ">":
            out.write("&gt;")
        elif cp == '"':
            out.write("&quot;")
        elif cp == "'":
            out.write("&#39;")
        else:
            out.write(cp)
    return out^


def url_encode(s: StringSlice) -> String:
    """`s` as one path segment or query value: unreserved bytes as they are,
    every other byte as `%XX`."""
    comptime hex = "0123456789ABCDEF"
    var out = String()
    for b in s.as_bytes():
        var c = Int(b)
        if (
            (c >= 48 and c <= 57)
            or (c >= 65 and c <= 90)
            or (c >= 97 and c <= 122)
            or c == 45
            or c == 46
            or c == 95
            or c == 126
        ):
            out.write(chr(c))
        else:
            out.write("%", hex[byte=c >> 4], hex[byte=c & 15])
    return out^


def url_decode(s: StringSlice) -> String:
    """`%XX` and `+` in a query value decoded; a malformed escape is kept as
    it is."""
    var b = s.as_bytes()
    var out = List[UInt8](capacity=len(b))
    var i = 0
    while i < len(b):
        var c = b[i]
        if c == 43:
            out.append(32)
        elif c == 37 and i + 2 < len(b):
            var hi = _hex(b[i + 1])
            var lo = _hex(b[i + 2])
            if hi >= 0 and lo >= 0:
                out.append(UInt8(hi * 16 + lo))
                i += 3
                continue
            out.append(c)
        else:
            out.append(c)
        i += 1
    return String(from_utf8_lossy=Span(out))


def _hex(c: UInt8) -> Int:
    if c >= 48 and c <= 57:
        return Int(c) - 48
    if c >= 65 and c <= 70:
        return Int(c) - 55
    if c >= 97 and c <= 102:
        return Int(c) - 87
    return -1


comptime _CODE_ICON = (
    '<svg class="src__icon" viewBox="0 0 16 16" width="12" height="12"'
    ' aria-hidden="true" focusable="false"><path d="M5.5 4.5 2 8l3.5 3.5M10.5'
    ' 4.5 14 8l-3.5 3.5" fill="none" stroke="currentColor"'
    ' stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>'
)
"""`</>`, before a link to code: drawn in the link's own colour, so it
follows the theme."""


struct Html(Movable, Writable):
    """A page being written."""

    var out: String

    def __init__(out self):
        self.out = String()

    def raw[*Ts: Writable](mut self, *parts: *Ts):
        """Markup, written as it is. Only ever literals of this module's
        callers, never a value from the database or a request."""
        comptime for i in range(parts.__len__()):
            self.out.write(parts[i])

    def text[*Ts: Writable](mut self, *values: *Ts):
        """Values, each written and then escaped."""
        comptime for i in range(values.__len__()):
            self.out.write(escape(String(values[i])))

    def link(mut self, href: StringSlice, label: StringSlice):
        """`<a href="href">label</a>`, both escaped."""
        self.out.write('<a href="', escape(href), '">', escape(label), "</a>")

    def link_out(mut self, href: StringSlice, label: StringSlice):
        """A link that opens in a new tab: to code, or another site."""
        self.out.write(
            '<a href="',
            escape(href),
            '" target="_blank" rel="noopener">',
            escape(label),
            "</a>",
        )

    def code(
        mut self, href: StringSlice, label: StringSlice, commit: StringSlice
    ):
        """A link to source code on its host, set apart from links inside
        orca: an icon, `label` (where, as `file.mojo:26`) in monospace, and
        ↗; it opens in a new tab, and its title names the commit."""
        var short = String(commit[byte = : min(7, commit.byte_length())])
        var host = (
            String(" on GitHub") if "://github.com/"
            in String(href) else String()
        )
        self.out.write(
            '<a class="src" href="',
            escape(href),
            '" target="_blank" rel="noopener" title="View source',
            host,
        )
        if short:
            self.out.write(" at ", escape(short))
        self.out.write(
            '">',
            _CODE_ICON,
            '<span class="src__at">',
            escape(label),
            '</span><span class="src__out" aria-hidden="true">↗</span></a>',
        )

    def cell[*Ts: Writable](mut self, *values: *Ts):
        """One `<td>` holding the values, escaped."""
        self.out.write("<td>")
        comptime for i in range(values.__len__()):
            self.out.write(escape(String(values[i])))
        self.out.write("</td>")

    def mono[*Ts: Writable](mut self, *values: *Ts):
        """A `<td>` in the monospace font: an id, a partition, a time."""
        self.out.write('<td class="mono">')
        comptime for i in range(values.__len__()):
            self.out.write(escape(String(values[i])))
        self.out.write("</td>")

    def num[*Ts: Writable](mut self, *values: *Ts):
        """A `<td>` of a count, right-aligned in tabular figures."""
        self.out.write('<td class="num">')
        comptime for i in range(values.__len__()):
            self.out.write(escape(String(values[i])))
        self.out.write("</td>")

    def chip(mut self, href: StringSlice, label: StringSlice):
        """A link to a run, as a small chip of its number."""
        self.out.write(
            '<a class="chip" href="', escape(href), '">', escape(label), "</a>"
        )

    def status(mut self, s: StringSlice):
        """A status as a badge, its class named after it (a space in the
        status is a hyphen in the class)."""
        var e = escape(s)
        self.out.write(
            '<span class="status status--',
            e.replace(" ", "-"),
            '">',
            e,
            "</span>",
        )

    def finish(deinit self) -> String:
        """The page written."""
        return self.out^

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.out)

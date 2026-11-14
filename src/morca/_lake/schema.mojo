"""Iceberg schemas and partition specs as `morca.tables` passes them to
liblake: their JSON, which the library parses."""


# ── schemas and specs ───────────────────────────────────────────────────────


struct Schema(Copyable, Movable, Writable):
    """An Iceberg schema, as its JSON (the spec's Appendix C). The library
    parses it; a schema that doesn't parse fails the call it's given to."""

    var json: String

    def __init__(out self, var json: String):
        self.json = json^

    @staticmethod
    def parse(text: String) -> Schema:
        return Schema(text.copy())

    def to_json(self) -> String:
        return self.json.copy()

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.json)


@fieldwise_init
struct PartitionField(Copyable, Movable):
    """One field of a partition spec: the source column's id, the partition
    field's own id (1000 and up), its name and transform (`identity`,
    `day`, `bucket[16]`, ...)."""

    var source_id: Int
    var field_id: Int
    var name: String
    var transform: String

    @staticmethod
    def single(
        source_id: Int, field_id: Int, var name: String, var transform: String
    ) -> Self:
        """As iceberg's `PartitionField.single` is called."""
        return Self(source_id, field_id, name^, transform^)


def parse_transform(s: String) -> String:
    """A transform, as iceberg's `parse_transform` is called; the library
    parses it."""
    return s.copy()


struct PartitionSpec(Copyable, Movable):
    """An Iceberg partition spec, as its JSON."""

    var json: String

    def __init__(out self, var json: String):
        self.json = json^

    def __init__(out self, spec_id: Int, fields: List[PartitionField]):
        var j = String('{"spec-id":', spec_id, ',"fields":[')
        for k in range(len(fields)):
            ref f = fields[k]
            j.write(
                "," if k > 0 else "",
                '{"source-id":',
                f.source_id,
                ',"field-id":',
                f.field_id,
                ',"name":"',
                f.name,
                '","transform":"',
                f.transform,
                '"}',
            )
        j.write("]}")
        self.json = j^

    @staticmethod
    def unpartitioned(spec_id: Int = 0) -> Self:
        return Self(spec_id, List[PartitionField]())

    @staticmethod
    def parse(text: String) -> Self:
        return Self(text.copy())

    def to_json(self) -> String:
        return self.json.copy()

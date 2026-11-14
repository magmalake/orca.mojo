"""A library that says it's liblake of another ABI version, for
tests/lake.sh: morca.tables must refuse it, naming both versions."""


@export("lake_abi_version")
def lake_abi_version() abi("C") -> UInt32:
    return 999

"""`orca_maintain`: snapshot expiry for the warehouse's datasets, which
`orca expire` and the scheduler run (see orca.maintain). A project that
writes tables builds it beside its `orca`."""

from orca.maintain import maintain_main


def main() raises:
    maintain_main()

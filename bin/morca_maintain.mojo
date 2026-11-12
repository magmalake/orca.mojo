"""`morca_maintain`: snapshot expiry for the warehouse's datasets, which
`morca expire` and the scheduler run (see morca.maintain). A project that
writes tables builds it beside its `morca`."""

from morca.maintain import maintain_main


def main() raises:
    maintain_main()

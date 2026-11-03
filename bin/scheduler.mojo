"""`orca`: the scheduler, a binary that links no pipeline.

    pixi run orca
    ./build/orca register ./build/taxi [...]

The command is `orca.command`, which a project's own binary calls the same
way.
"""

from orca.command import orca_command


def main() raises:
    orca_command()

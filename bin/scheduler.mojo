"""`morca`: the scheduler, a binary that links no pipeline.

    pixi run morca
    ./build/morca register ./build/taxi [...]

The command is `morca.command`, which a project's own binary calls the same
way.
"""

from morca.command import morca_command


def main() raises:
    morca_command()

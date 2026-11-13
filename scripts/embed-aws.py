#!/usr/bin/env python3
"""Writes src/morca/_internal/aws_assets.mojo from deploy/aws/morca.yaml and
deploy/project.Dockerfile, so that `morca aws` carries them in the package.

    python3 scripts/embed-aws.py          # rewrite it
    python3 scripts/embed-aws.py --check  # fail if it's out of date
"""
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
out = root / "src/morca/_internal/aws_assets.mojo"


def literal(path):
    text = (root / path).read_text()
    assert '"""' not in text, path
    return '"""' + text.replace("\\", "\\\\") + '"""'


body = f'''"""The stack template and the project Dockerfile `morca aws` writes out:
deploy/aws/morca.yaml and deploy/project.Dockerfile, embedded by
scripts/embed-aws.py. Edit those, then run it; `pixi run test-aws-assets`
fails while this is out of date.
"""

comptime STACK_TEMPLATE: StaticString = {literal("deploy/aws/morca.yaml")}

comptime PROJECT_DOCKERFILE: StaticString = {literal("deploy/project.Dockerfile")}
'''

if "--check" in sys.argv:
    if not out.exists() or out.read_text() != body:
        sys.exit("src/morca/_internal/aws_assets.mojo is out of date: run python3 scripts/embed-aws.py")
    print("ok   aws assets")
else:
    out.write_text(body)

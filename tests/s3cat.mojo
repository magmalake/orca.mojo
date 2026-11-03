"""What the tests read of object storage.

Through objectstore's `FileIOResolver` and the `AWS_*` environment:

    s3cat ls <prefix>     every location under it, one per line
    s3cat get <location>  its bytes, to stdout
"""

from std.sys import argv, exit

from objectstore.fileio import FileIOResolver


def main() raises:
    var args = argv()
    if len(args) != 3:
        print("usage: s3cat ls <prefix> | s3cat get <location>")
        exit(2)
    var r = FileIOResolver()
    if args[1] == "ls":
        for loc in r.list(String(args[2])):
            print(loc)
    elif args[1] == "get":
        print(String(from_utf8_lossy=Span(r.read_all(String(args[2])))), end="")
    else:
        exit(2)

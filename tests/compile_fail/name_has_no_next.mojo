# expect: 'Name' value has no attribute 'next'
from morca import Name


def main() raises:
    print(Name.parse("a").next())

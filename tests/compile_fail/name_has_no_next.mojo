# expect: 'Name' value has no attribute 'next'
from orca import Name


def main() raises:
    print(Name.parse("a").next())

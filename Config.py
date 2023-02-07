import sys


class Definitions:
    @staticmethod
    def root_dir() -> str:
        return sys.path[1]

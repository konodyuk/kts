import argparse
import sys
from .file_system import build_file_system


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand",
                                       title='subcommands',
                                       description='valid subcommands',
                                       help='Use init to initialize kts project inside of this folder.')
    subparsers.add_parser('init')
    args = parser.parse_args(sys.argv[1:])
    if args.subcommand == 'init':
        build_file_system()
    else:
        parser.print_help()

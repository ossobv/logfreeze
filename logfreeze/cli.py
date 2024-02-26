from argparse import ArgumentParser

from .config import AppConfig
from .input import test_input
from .sink import test_sink


def main():
    parser = ArgumentParser(prog='logfreeze')
    parser.add_argument(
        '-c', '--config', required=True, help='toml config filename')
    subparser = parser.add_subparsers(dest='command', required=True)
    testinput = subparser.add_parser(
        'testinput', help='test connecting to input')
    testsink = subparser.add_parser(
        'testsink', help='test connecting to sink')

    args = parser.parse_args()
    del testinput, testsink

    appconfig = AppConfig.from_filename(args.config)
    print(appconfig)
    print()

    if args.command == 'testinput':
        test_input(appconfig.inputs[0])
    elif args.command == 'testsink':
        test_sink(appconfig.sinks[0])
    else:
        raise NotImplementedError(args.command)

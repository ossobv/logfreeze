from argparse import ArgumentParser

from . import action
from .config import AppConfig


def main():
    parser = ArgumentParser(prog='logfreeze')
    parser.add_argument(
        '-c', '--config', required=True, help='toml config filename')
    subparser = parser.add_subparsers(dest='command', required=True)

    test_input_connect = subparser.add_parser(
        'test-input-connect', help='test connecting to input')
    test_input_connect.add_argument('input', help='name of the input')

    test_sink_connect = subparser.add_parser(
        'test-sink-connect', help='test connecting to sink')
    test_sink_connect.add_argument('sink', help='name of the sink')

    args = parser.parse_args()

    appconfig = AppConfig.from_filename(args.config)
    print(appconfig)
    print()

    if args.command == 'test-input-connect':
        action.test_input_connect(appconfig, args.input)
    elif args.command == 'test-sink-connect':
        action.test_sink_connect(appconfig, args.sink)
    else:
        raise NotImplementedError(args.command)

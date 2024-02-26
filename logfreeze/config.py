from collections import namedtuple

try:
    import tomllib
except ImportError:
    import toml as tomllib

from .input.config import InputConfig


class AppConfig(namedtuple('AppConfig', 'inputs filters sinks')):
    @classmethod
    def from_filename(cls, filename):
        with open(filename, 'r') as fp:
            data = tomllib.load(fp)

        inputs = []
        for input_name, input_data in data.pop('input', {}).items():
            inputs.append(InputConfig.from_data(input_name, input_data))

        filters = []
        for filter_name, filter_data in data.pop('filter', {}).items():
            filters.append(FilterConfig.from_data(filter_name, filter_data))

        sinks = []
        for sink_name, sink_data in data.pop('sink', {}).items():
            sinks.append(SinkConfig.from_data(sink_name, sink_data))

        assert not data, ('leftover data', data)

        return cls(
            inputs=inputs,
            filters=filters,
            sinks=sinks)

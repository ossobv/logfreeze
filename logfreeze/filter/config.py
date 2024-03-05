from collections import namedtuple


class FilterConfig(namedtuple('FilterConfig', (
        'name condition'))):

    @classmethod
    def from_data(cls, name, data):
        condition = data.pop('condition')
        if condition:
            condition = cls._parse_condition(condition)

        assert not data, ('leftover data', data)
        assert condition is not None

        return cls(
            name=name,
            condition=condition)

    @classmethod
    def _parse_condition(cls, condition):
        return condition  # FIXME

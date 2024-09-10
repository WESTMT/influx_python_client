import json


class BaseFunction:
    BASE_TEMPLATE = '{name}({params})'
    NAME: str = None

    def __init__(self, /, **kwargs):
        self._arguments = kwargs

    def _prepare_params(self) -> str:
        output_strings = []
        for key, value in self._arguments.items():
            if isinstance(value, list):
                value = json.dumps(value)
            output_strings.append(f'{key}: {value}')
        return ', '.join(output_strings)

    def __str__(self) -> str:
        func_str = self.BASE_TEMPLATE.format(name=self.NAME, params=self._prepare_params())
        return func_str


class range_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/range/
    """
    NAME = 'range'


class filter_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/filter/
    """
    NAME = 'filter'


class limit_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/limit/
    """
    NAME = 'limit'


class aggregate_window_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/aggregateWindow/
    """
    NAME = 'aggregateWindow'

class group_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/group/
    """
    NAME = 'group'


class last_func(BaseFunction):
    """
    https://docs.influxdata.com/flux/v0/stdlib/universe/last/
    """
    NAME = 'last'


# Implementing the FillFunction class
class fill_func(BaseFunction):
    NAME = 'fill'

    def __init__(self, /, value=None, usePrevious=None, column=None):
        if sum(arg is not None for arg in [value, usePrevious, column]) != 1:
            raise ValueError("Exactly one of 'value', 'usePrevious', or 'column' must be provided.")

        if value is not None:
            super().__init__(value=value)
        elif usePrevious is not None:
            # Ensure that `usePrevious` is correctly output as `true` or `false` in Flux syntax
            super().__init__(usePrevious=str(usePrevious).lower())
        elif column is not None:
            super().__init__(column=column)

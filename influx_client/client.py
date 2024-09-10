import logging
from typing import Callable

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from influx_client.functions import range_func, group_func, limit_func, filter_func
from influx_client.validators import validate_iso_8601_timestamp

logger = logging.getLogger(__name__)


class DBClient:
    def __init__(self, url: str, token: str, bucket: str = None, org: str = 'westrade'):
        self.org = org
        self.bucket = bucket

        self._write_api_sync = None
        self._delete_api = None
        self._query_api = None

        self._client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.org
        )

    def write_sync(self, points: list, bucket: str = None):
        if bucket is None:
            bucket = self.bucket

        logger.info(f'Writing to {bucket} bucket')

        if not self._write_api_sync:
            self._write_api_sync = self._client.write_api(write_options=SYNCHRONOUS)

        try:
            self._write_api_sync.write(bucket=bucket, org=self.org, record=points)
            logger.info(f'{len(points)} points written to InfluxDB')
        except Exception:
            logger.exception('Error writing to InfluxDB')

    def delete(self, start_timestamp: str, stop_timestamp: str,
               measurement: str = None, tags: dict = None, bucket: str = None):
        """
        https://docs.influxdata.com/influxdb/v2/write-data/delete-data/

        Example:
            client.delete(
            '2024-08-29T08:44:12.395661+00:00', '2024-09-01T08:44:12.395661+00:00,
            measurement='elastic_net_v4.pkl', tags={'station': '04D11D0D'})
        """

        if not all((validate_iso_8601_timestamp(start_timestamp), validate_iso_8601_timestamp(stop_timestamp))):
            raise ValueError('Start and stop timestamps should be in RF3339 format,'
                             ' see https://docs.influxdata.com/influxdb/v2/reference/glossary/#rfc3339-timestamp.')

        if bucket is None:
            bucket = self.bucket

        if tags is None:
            tags = {}

        if not self._delete_api:
            self._delete_api = self._client.delete_api()

        self._delete_api.delete(
            start=start_timestamp,
            stop=stop_timestamp,
            predicate=self._build_delete_predicate(measurement=measurement, **tags),
            bucket=bucket,
        )

    def query(self, start_range: str, stop_range: str = None, query_functions: list[Callable] = None,
              group_columns: list[str] = None, limit_groups: int = 0, bucket: str = None, json: bool = True):
        """
        https://docs.influxdata.com/influxdb/v2/query-data/get-started/query-influxdb/

        Example:
            client.query(
                start_range='-1d',
                limit_groups=10,
                query_functions=[filter_func(fn='(r) => r._measurement == "04D11D0D"')]
            )
        """

        # Initialize the range function
        range_ = range_func(start=start_range, stop=stop_range) if stop_range else range_func(start=start_range)

        # Initialize lists for different types of functions
        filter_functions = []
        group_function = group_func(columns=group_columns) if group_columns else group_func()
        other_functions = []

        # Sort the query_functions into appropriate lists
        for func in (query_functions or []):
            if isinstance(func, filter_func):
                filter_functions.append(func)
            elif isinstance(func, group_func):
                group_function = func  # This overrides the default group function if one is provided in the list
            else:
                other_functions.append(func)

        # Combine all functions in the desired order
        functions = [range_] + filter_functions + [group_function] + other_functions


        return self._query(query_string=self._build_query(functions, bucket or self.bucket), json=json)

    def raw(self, query_string: str, json: bool = True):
        """
        client.raw('from(bucket: "sensors")|> range(start: -2d)|> group()
        |> limit(n: 10)|> filter(fn: (r) => r._measurement == "0020D47E")')
        """
        return self._query(query_string, json)

    def _query(self, query_string: str, json: bool = True):
        if not self._query_api:
            self._query_api = self._client.query_api()

        try:
            response = self._query_api.query(query_string)
            if json:
                return response.to_json()
            return response
        except ApiException as ex:
            logger.exception(f'Error querying InfluxDB: {ex}')
            raise ValueError(f'Error during query: "{ex.message}"')

    @staticmethod
    def _build_delete_predicate(measurement: str = None, **kwargs) -> str:
        """
        https://docs.influxdata.com/influxdb/v2/reference/syntax/delete-predicate/
        """

        _delimiter = ' AND '
        predicate = ''

        if measurement is None and not len(kwargs):
            raise ValueError('You should provide at least one tag or specify a measurement.')

        tags = _delimiter.join((f'{key}="{value}"' for key, value in kwargs.items()))
        if measurement:
            predicate += f'_measurement="{measurement}"'
        if tags:
            predicate += f'{_delimiter if measurement else ""}{tags}'

        logger.info(predicate)

        return predicate

    @staticmethod
    def _build_query(query_functions: list, bucket: str) -> str:
        query = f'from(bucket: "{bucket}")'

        for query_function in query_functions:
            query += f'|> {query_function}'

        logger.info(query)
        return query

    def close(self):
        self._client.close()

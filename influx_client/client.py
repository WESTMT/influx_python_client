import logging
from typing import Callable

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from influx_client.functions import range_func, group_func, limit_func

logger = logging.getLogger(__name__)


class DBClient:
    def __init__(self, url: str, token: str, bucket: str, org: str = 'westrade'):
        self.org = org
        self.bucket = bucket

        self._write_api_sync = None
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

        range_ = range_func(start=start_range, stop=stop_range) if stop_range else range_func(start=start_range)
        functions = [range_]

        if group_columns:
            functions.append(group_func(columns=group_columns))
        else:
            functions.append(group_func())

        if limit_groups:
            functions.append(limit_func(n=limit_groups))

        functions.extend(query_functions or [])

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
    def _build_query(query_functions: list, bucket: str) -> str:
        query = f'from(bucket: "{bucket}")'

        for query_function in query_functions:
            query += f'|> {query_function}'

        logger.info(query)
        return query

    def close(self):
        self._client.close()

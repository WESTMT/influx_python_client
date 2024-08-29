from datetime import datetime
from typing import Any, Callable, Optional, ClassVar, List

from pydantic import BaseModel, ValidationError
from influxdb_client import Point, WritePrecision


class FieldType:
    TIME = 'time'
    MEASUREMENT = 'measurement'
    TAG = 'tag'
    FIELD = 'field'


class BaseInfluxSerializer(BaseModel):
    WRITE_PRECISION: ClassVar[str] = WritePrecision.S

    @property
    def measurement(self) -> str:
        for field, info in self.model_fields.items():
            if FieldType.MEASUREMENT in info.metadata:
                return getattr(self, field)

    @property
    def tags(self) -> List[dict]:
        tags = []
        for field, info in self.model_fields.items():
            if FieldType.TAG in info.metadata:
                tags.append(
                    {'key': field, 'value': getattr(self, field)}
                )
        return tags

    @property
    def fields(self) -> List[dict]:
        fields = []
        for field, info in self.model_fields.items():
            if FieldType.FIELD in info.metadata:
                fields.append(
                    {'field': field, 'value': getattr(self, field)}
                )
        return fields

    @property
    def time(self) -> datetime:
        for field, info in self.model_fields.items():
            if FieldType.TIME in info.metadata:
                return getattr(self, field)

    def to_point(self, write_precision: Optional[str] = None) -> Point:
        if write_precision is None:
            write_precision = self.WRITE_PRECISION

        point = Point(self.measurement)
        [point.tag(**tag) for tag in self.tags]
        [point.field(**field) for field in self.fields]
        point.time(self.time, write_precision)
        return point


def invalid_to_none(v: Any, handler: Callable[[Any], Any]) -> Any:
    try:
        return handler(v)
    except ValidationError:
        return None

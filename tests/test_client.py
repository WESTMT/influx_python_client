import pytest

from influx_client.client import DBClient


def test_build_delete_predicate_only_measurement():
    predicate = DBClient._build_delete_predicate(measurement='test')

    assert predicate == '_measurement="test"'


def test_build_delete_predicate_only_tags():
    predicate = DBClient._build_delete_predicate(tag='test', tag_2=1)

    assert predicate == 'tag="test" AND tag_2="1"'


def test_build_delete_predicate_with_tags_and_measurement():
    predicate = DBClient._build_delete_predicate(measurement='zzz', tag='test', tag_2=1)

    assert predicate == '_measurement="zzz" AND tag="test" AND tag_2="1"'


def test_build_delete_predicate_with_empty_input():
    with pytest.raises(ValueError):
        DBClient._build_delete_predicate()

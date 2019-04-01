import asyncio
import datetime
import functools

import databases
import pytest
import sqlalchemy
from pydantic import validator

import ormantic as orm

DATABASE_URL = "sqlite:///test.db"
database = databases.Database(DATABASE_URL, force_rollback=True)
metadata = sqlalchemy.MetaData()


def time():
    return datetime.datetime.now().time()


class Example(orm.Model):
    id: orm.Integer(primary_key=True) = None
    created: orm.DateTime() = None
    created_day: orm.Date() = None
    created_time: orm.Time() = None
    description: orm.Text() = ""
    value: orm.Float(allow_null=True) = None
    data: orm.JSON() = {}

    class Mapping:
        table_name = "example"
        metadata = metadata
        database = database

    @validator("created", pre=True, always=True)
    def set_created_now(cls, v):
        return v or datetime.datetime.now()

    @validator("created_day", pre=True, always=True)
    def set_created_day_now(cls, v):
        return v or datetime.date.today()

    @validator("created_time", pre=True, always=True)
    def set_created_time_now(cls, v):
        return v or datetime.datetime.now().time()


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    engine = sqlalchemy.create_engine(DATABASE_URL)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@async_adapter
async def test_model_crud():
    async with database:
        await Example.objects.create()

        example = await Example.objects.get()
        assert example.created.year == datetime.datetime.now().year
        assert example.created_day == datetime.date.today()
        assert example.description == ""
        assert example.value is None
        assert example.data == {}

        await example.update(data={"foo": 123}, value=123.456)
        example = await Example.objects.get()
        assert example.value == 123.456
        assert example.data == {"foo": 123}

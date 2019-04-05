import asyncio
import datetime
import functools
import enum

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


class IntEnum(enum.Enum):
    one = 1
    two = 2
    three = 3


class StrEnum(enum.Enum):
    one = "uno"
    two = "dos"
    three = "tres"


class Example(orm.Model):
    id: orm.Integer(primary_key=True) = None
    created: orm.DateTime() = None
    created_day: orm.Date() = None
    created_time: orm.Time() = None
    description: orm.Text() = ""
    value: orm.Float(allow_null=True) = None
    data: orm.JSON() = {}
    int_enum: orm.Enum(IntEnum, allow_null=True) = None
    str_enum: orm.Enum(StrEnum, allow_null=True) = None

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
        assert example.int_enum is None
        assert example.str_enum is None

        await example.update(
            data={"foo": 123},
            value=123.456,
            int_enum=IntEnum.one,
            str_enum=StrEnum.three,
        )
        example = await Example.objects.get()
        assert example.value == 123.456
        assert example.data == {"foo": 123}
        assert example.int_enum == IntEnum.one
        assert example.str_enum == StrEnum.three

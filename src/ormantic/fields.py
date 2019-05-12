import json
import re
from datetime import datetime, date, time
from typing import Type, Any

import pydantic
import sqlalchemy
import enum


class ColumnFactory(object):

    func_count = 0
    primary_key = False
    allow_null = False
    allow_blank = False
    index = False
    unique = False
    constraints = []
    column_type = None

    @classmethod
    def get_column(cls, name: str) -> sqlalchemy.Column:
        return sqlalchemy.Column(
            name,
            cls.column_type,
            *cls.constraints,
            primary_key=cls.primary_key,
            nullable=cls.allow_null and not cls.primary_key,
            index=cls.index,
            unique=cls.unique,
        )


def String(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
    allow_blank: bool = False,
    strip_whitespace: bool = False,
    min_length: int = None,
    max_length: int = None,
    curtail_length: int = None,
    regex: str = None,
) -> Type[str]:

    assert max_length is not None, "max_length required field (> 0)."
    assert max_length > 0, "max_length > 0 is required"

    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        allow_blank=allow_blank,
        strip_whitespace=strip_whitespace,
        min_length=min_length,
        max_length=max_length,
        curtail_length=curtail_length,
        regex=regex and re.compile(regex),
        column_type=sqlalchemy.String(length=max_length),
    )

    return type("String", (pydantic.ConstrainedStr, ColumnFactory), namespace)


def Text(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
    allow_blank: bool = False,
    strip_whitespace: bool = False,
) -> Type[str]:

    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        allow_blank=allow_blank,
        strip_whitespace=strip_whitespace,
        column_type=sqlalchemy.Text(),
    )

    return type("Text", (pydantic.ConstrainedStr, ColumnFactory), namespace)


def Integer(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
    minimum: int = None,
    maximum: int = None,
    multiple_of: int = None,
) -> Type[int]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        ge=minimum,
        le=maximum,
        multiple_of=multiple_of,
        column_type=sqlalchemy.Integer(),
    )
    return type("Integer", (pydantic.ConstrainedInt, ColumnFactory), namespace)


def Float(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
    minimum: float = None,
    maximum: float = None,
    multiple_of: int = None,
) -> Type[int]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        ge=minimum,
        le=maximum,
        multiple_of=multiple_of,
        column_type=sqlalchemy.Float(),
    )
    return type("Float", (pydantic.ConstrainedFloat, ColumnFactory), namespace)


def Boolean(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[bool]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.Boolean(),
    )
    return type("Boolean", (int, ColumnFactory), namespace)


def DateTime(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[datetime]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.DateTime(),
    )
    return type("DateTime", (datetime, ColumnFactory), namespace)


def Date(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[date]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.Date(),
    )
    return type("Date", (date, ColumnFactory), namespace)


def Time(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[time]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.Time(),
    )
    return type("Time", (time, ColumnFactory), namespace)


def JSON(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[Any]:
    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.JSON(),
    )

    class Json(object):
        @classmethod
        def __get_validators__(cls) -> "CallableGenerator":
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> Any:
            try:
                if isinstance(v, str):
                    return json.loads(v)
                else:
                    return v
            except ValueError:
                raise errors.JsonError()
            except TypeError:
                raise errors.JsonTypeError()

    return type("JSON", (Json, ColumnFactory), namespace)


def ForeignKey(to, *, allow_null: bool = False) -> Type[object]:
    fk_string = to.Mapping.table_name + "." + to.Mapping.pk_name
    to_field = to.__fields__[to.Mapping.pk_name]
    namespace = dict(
        to=to,
        allow_null=allow_null,
        constraints=[sqlalchemy.schema.ForeignKey(fk_string)],
        column_type=to_field.type_.column_type,
    )

    class ForeignKeyField(object):
        @classmethod
        def __get_validators__(cls) -> "CallableGenerator":
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> Any:
            return v

    return type("ForeignKey", (ForeignKeyField, ColumnFactory), namespace)


def Enum(
    enum_type: Type[enum.Enum],
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[enum.Enum]:

    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.Enum(enum_type),
    )

    class EnumField(object):
        @classmethod
        def __get_validators__(cls) -> "CallableGenerator":
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> Any:
            return v

    return type("Enum", (EnumField, ColumnFactory), namespace)


def StringArray(
    *,
    primary_key: bool = False,
    allow_null: bool = False,
    index: bool = False,
    unique: bool = False,
) -> Type[str]:

    namespace = dict(
        primary_key=primary_key,
        allow_null=allow_null,
        index=index,
        unique=unique,
        column_type=sqlalchemy.ARRAY(sqlalchemy.String),
    )

    class StringArrayField(object):
        @classmethod
        def __get_validators__(cls) -> "CallableGenerator":
            yield cls.validate

        @classmethod
        def validate(cls, v: Any) -> Any:
            return v

    return type("StringArray", (StringArrayField, ColumnFactory), namespace)

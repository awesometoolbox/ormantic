import typing

import pydantic
import sqlalchemy
from functools import partial
from pydantic.class_validators import Validator, make_generic_validator
from .exceptions import MultipleMatches, NoMatch

FILTER_OPERATORS = {
    "any": "any_",
    "exact": "__eq__",
    "iexact": "ilike",
    "contains": "like",
    "icontains": "ilike",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
}


class QuerySet:
    def __init__(
        self, model_cls=None, filter_clauses=None, select_related=None
    ):
        self.model_cls = model_cls
        self.filter_clauses = [] if filter_clauses is None else filter_clauses
        self._select_related = [] if select_related is None else select_related

    def __get__(self, instance, owner):
        return self.__class__(model_cls=owner)

    @property
    def database(self):
        return self.model_cls.Mapping.database

    @property
    def table(self):
        return self.model_cls.Mapping.table

    @property
    def pk_name(self):
        return self.model_cls.Mapping.pk_name

    def build_select_expression(self, functions=None):
        tables = [self.table]
        select_from = self.table

        for item in self._select_related:
            model_cls = self.model_cls
            for part in item.split("__"):
                model_cls = model_cls.__fields__[part].type_.to
                select_from = sqlalchemy.sql.join(
                    select_from, model_cls.Mapping.table
                )
                tables.append(model_cls.Mapping.table)

        expr = sqlalchemy.sql.select(functions or tables)
        expr = expr.select_from(select_from)

        if self.filter_clauses:
            if len(self.filter_clauses) == 1:
                clause = self.filter_clauses[0]
            else:
                clause = sqlalchemy.sql.and_(*self.filter_clauses)
            expr = expr.where(clause)

        return expr

    def filter(self, **kwargs):
        filter_clauses = self.filter_clauses
        select_related = list(self._select_related)

        for key, value in kwargs.items():
            if "__" in key:
                parts = key.split("__")

                # Determine if we should treat the final part as a
                # filter operator or as a related field.
                if parts[-1] in FILTER_OPERATORS:
                    op = parts[-1]
                    field_name = parts[-2]
                    related_parts = parts[:-2]
                else:
                    op = "exact"
                    field_name = parts[-1]
                    related_parts = parts[:-1]

                model_cls = self.model_cls
                if related_parts:
                    # Add any implied select_related
                    related_str = "__".join(related_parts)
                    if related_str not in select_related:
                        select_related.append(related_str)

                    # Walk the relationships to the actual model class
                    # against which the comparison is being made.
                    for part in related_parts:
                        model_cls = model_cls.__fields__[part].type_.to

                column = model_cls.Mapping.table.columns[field_name]

            else:
                op = "exact"
                column = self.table.columns[key]

            # Map the operation code onto SQLAlchemy's ColumnElement
            # https://docs.sqlalchemy.org/en/latest/core/sqlelement.html
            op_attr = FILTER_OPERATORS[op]

            if op in ["contains", "icontains"]:
                value = "%" + value + "%"

            if isinstance(value, Model):
                value = value.pk

            clause = getattr(column, op_attr)(value)
            filter_clauses.append(clause)

        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=filter_clauses,
            select_related=select_related,
        )

    def select_related(self, related):
        if not isinstance(related, (list, tuple)):
            related = [related]

        related = list(self._select_related) + related
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=related,
        )

    async def exists(self) -> bool:
        expr = self.build_select_expression()
        expr = sqlalchemy.exists(expr).select()
        return await self.database.fetch_val(expr)

    async def count(self) -> int:
        expr = self.build_select_expression(
            functions=[sqlalchemy.func.count()]
        )
        return await self.database.fetch_val(expr)

    async def all(self, **kwargs):
        if kwargs:
            return await self.filter(**kwargs).all()

        expr = self.build_select_expression()
        rows = await self.database.fetch_all(expr)
        return [
            self.model_cls.from_row(row, select_related=self._select_related)
            for row in rows
        ]

    async def get(self, **kwargs):
        if kwargs:
            return await self.filter(**kwargs).get()

        expr = self.build_select_expression().limit(2)
        rows = await self.database.fetch_all(expr)

        if not rows:
            raise NoMatch()
        if len(rows) > 1:
            raise MultipleMatches()
        return self.model_cls.from_row(
            rows[0], select_related=self._select_related
        )

    async def create(self, **kwargs):
        # instance and validation
        instance = self.model_cls(**kwargs)
        data = instance.table_dict()

        # pop id if None
        pk_column = getattr(self.table.c, self.pk_name)
        if data.get(pk_column, -1) is None:
            data.pop(pk_column)

        # Build the insert expression.
        expr = self.table.insert()
        expr = expr.values(**data)

        # Execute the insert, and return a new model instance.
        result = await self.database.execute(expr)

        if result is not None:
            instance.pk = result

        return instance

    async def insert_many(
        self, rows: typing.Iterable["Model"], batch_size=1000
    ):
        values = []
        expr = self.table.insert()
        for row in rows:
            values.append(row.table_dict())
            if len(values) == batch_size:
                await self.database.execute_many(expr, values)
                values = []

        if len(values):
            await self.database.execute_many(expr, values)

    async def delete_many(self, **kwargs):
        if kwargs:
            return await self.filter(**kwargs).delete_many()

        expr = self.build_select_expression()
        expr = self.table.delete(whereclause=expr._whereclause)
        await self.database.execute(expr)


class MetaModel(pydantic.main.MetaModel):
    @typing.no_type_check
    def __new__(mcs: type, name, bases, namespace):
        new_model = super().__new__(mcs, name, bases, namespace)

        if hasattr(new_model, "Mapping"):
            columns = []

            for name, field in new_model.__fields__.items():
                if field.type_.primary_key:
                    new_model.Mapping.pk_name = name
                columns.append(field.type_.get_column(name))

            new_model.Mapping.table = sqlalchemy.Table(
                new_model.Mapping.table_name,
                new_model.Mapping.metadata,
                *columns
            )

            new_model.objects = QuerySet(new_model)

        return new_model


class Model(pydantic.BaseModel, metaclass=MetaModel):

    # noinspection PyMissingConstructor
    def __init__(self, **data):
        if "pk" in data:
            data[self.Mapping.pk_name] = data.pop("pk")

        if typing.TYPE_CHECKING:
            self.__values__: Dict[str, Any] = {}
            self.__fields_set__: "SetStr" = set()

        pk_only = data.pop("__pk_only__", False)
        values, fields_set, _ = pydantic.validate_model(
            self, data, raise_exc=not pk_only
        )

        object.__setattr__(self, "__values__", values)
        object.__setattr__(self, "__fields_set__", fields_set)

    @property
    def pk(self):
        return getattr(self, self.Mapping.pk_name)

    @typing.no_type_check
    def __setattr__(self, name, value):
        if name == "pk":
            setattr(self, self.Mapping.pk_name, value)
        else:
            super(Model, self).__setattr__(name, value)

    async def update(self, *columns, **new_values):
        # Get self column values and update with new_values provided.
        data = self.table_dict()
        data.update(new_values)

        # Filter data by columns + new value keys, only if columns specified.
        if columns:
            columns = set(columns).union(new_values.keys())
            data = dict((k, v) for k, v in data.items() if k in columns)

        # Build the update expression.
        pk_column = getattr(self.Mapping.table.c, self.Mapping.pk_name)
        expr = self.Mapping.table.update()
        expr = expr.values(**data).where(pk_column == self.pk)

        # Perform the update.
        rows_updated = await self.Mapping.database.execute(expr)

        # Update the model instance.
        for key, value in new_values.items():
            setattr(self, key, value)

        return rows_updated

    async def delete(self):
        # Build the delete expression.
        pk_column = getattr(self.Mapping.table.c, self.Mapping.pk_name)
        expr = self.Mapping.table.delete().where(pk_column == self.pk)

        # Perform the delete.
        await self.Mapping.database.execute(expr)

    async def load(self):
        # Build the select expression.
        pk_column = getattr(self.Mapping.table.c, self.Mapping.pk_name)
        expr = self.Mapping.table.select().where(pk_column == self.pk)

        # Perform the fetch.
        row = await self.Mapping.database.fetch_one(expr)

        # Update the instance.
        for key, value in dict(row).items():
            setattr(self, key, value)

    async def insert(self):
        # Build the insert expression.
        expr = self.Mapping.table.insert()
        expr = expr.values(**self.table_dict())

        # Execute the insert, and return a new model instance.
        result = await self.Mapping.database.execute(expr)

        if result is not None:
            setattr(self, "pk", result)

        return result

    async def upsert(self):
        rows_updated = 0
        if self.pk is not None:
            rows_updated = await self.update()

        if rows_updated in {0, None}:
            await self.insert()

    @classmethod
    def from_row(cls, row, select_related=None):
        """
        Instantiate a model instance, given a database row.
        """
        select_related = select_related or []
        item = {}

        # Instantiate any child instances first.
        for related in select_related:
            if "__" in related:
                first_part, remainder = related.split("__", 1)
                model_cls = cls.__fields__[first_part].type_.to
                item[first_part] = model_cls.from_row(
                    row, select_related=[remainder]
                )
            else:
                model_cls = cls.__fields__[related].type_.to
                item[related] = model_cls.from_row(row)

        # Pull out the regular column values.
        for column in cls.Mapping.table.columns:
            if column.name not in item:
                col_type = cls.__fields__[column.name].type_
                value = row[column]
                if col_type.__name__ == "ForeignKey":
                    item[column.name] = col_type.to(pk=value, __pk_only__=True)
                else:
                    item[column.name] = value

        return cls(**item)

    def table_dict(self) -> "DictStrAny":
        get_key = self._get_key_factory(False)
        get_key = partial(get_key, self.fields)

        def _get_td_value(v: typing.Any) -> typing.Any:
            if isinstance(v, Model):
                return v.pk
            elif isinstance(v, list):
                return [_get_td_value(v_) for v_ in v]
            elif isinstance(v, dict):
                return {k_: _get_td_value(v_) for k_, v_ in v.items()}
            elif isinstance(v, set):
                return {_get_td_value(v_) for v_ in v}
            elif isinstance(v, tuple):
                return tuple(_get_td_value(v_) for v_ in v)
            else:
                return v

        def _td_iter():
            for k, v in self.__values__.items():
                yield k, _get_td_value(v)

        return {get_key(k): v for k, v in _td_iter()}

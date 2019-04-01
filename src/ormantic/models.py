import typing

import pydantic
import sqlalchemy
from pydantic.class_validators import Validator, make_generic_validator
from .exceptions import MultipleMatches, NoMatch

FILTER_OPERATORS = {
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

    def build_select_expression(self):
        tables = [self.table]
        select_from = self.table

        for item in self._select_related:
            model_cls = self.model_cls
            select_from = self.table
            for part in item.split("__"):
                model_cls = model_cls.__fields__[part].to
                select_from = sqlalchemy.sql.join(
                    select_from, model_cls.Mapping.table
                )
                tables.append(model_cls.Mapping.table)

        expr = sqlalchemy.sql.select(tables)
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
                        model_cls = model_cls.__fields__[part].to

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
        expr = self.build_select_expression()
        expr = sqlalchemy.func.count().select().select_from(expr)
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

        # Build the insert expression.
        expr = self.table.insert()
        expr = expr.values(**instance.dict())

        # Execute the insert, and return a new model instance.
        result = await self.database.execute(expr)
        instance.pk = result
        return instance


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
    def __init__(self, **kwargs):
        if "pk" in kwargs:
            kwargs[self.Mapping.pk_name] = kwargs.pop("pk")
        super().__init__(**kwargs)

    @property
    def pk(self):
        return getattr(self, self.Mapping.pk_name)

    @typing.no_type_check
    def __setattr__(self, name, value):
        if name == "pk":
            setattr(self, self.Mapping.pk_name, value)
        else:
            super(Model, self).__setattr__(name, value)

    async def update(self, **kwargs):
        # Build the update expression.
        pk_column = getattr(self.Mapping.table.c, self.Mapping.pk_name)
        expr = self.Mapping.table.update()
        expr = expr.values(**kwargs).where(pk_column == self.pk)

        # Perform the update.
        await self.Mapping.database.execute(expr)

        # Update the model instance.
        for key, value in kwargs.items():
            setattr(self, key, value)

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
                model_cls = cls.__fields__[first_part].to
                item[first_part] = model_cls.from_row(
                    row, select_related=[remainder]
                )
            else:
                model_cls = cls.__fields__[related].to
                item[related] = model_cls.from_row(row)

        # Pull out the regular column values.
        for column in cls.Mapping.table.columns:
            if column.name not in item:
                item[column.name] = row[column]

        return cls(**item)

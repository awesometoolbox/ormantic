# Ormantic

The `ormantic` package is an async ORM for Python, with support for Postgres,
MySQL, and SQLite. Ormatic is a fork from [`ORM`][orm] for the purpose of
integrating with [`FastAPI`][fastapi] and is built with:

* [SQLAlchemy core][sqlalchemy-core] for query building.
* [`databases`][databases] for cross-database async support.
* [`pydantic`][pydantic] for data validation.

Because ORM is built on SQLAlchemy core, you can use Alembic to provide
database migrations.

**Note**: Use `ipython` to try this from the console, since it supports `await`.

```python
import databases
import sqlalchemy
import ormantic as orm

database = databases.Database("sqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class Note(orm.Model):
    id: orm.Integer(primary_key=True) = None
    text: orm.String(max_length=100)
    completed: orm.Boolean() = False

    class Mapping:
        table_name = "notes"
        database = database
        metadata = metadata


# Create the database
engine = sqlalchemy.create_engine(str(database.url))
metadata.create_all(engine)

# .create()
await Note.objects.create(text="Buy the groceries.", completed=False)
await Note.objects.create(text="Call Mum.", completed=True)
await Note.objects.create(text="Send invoices.", completed=True)

# .all()
notes = await Note.objects.all()

# .filter()
notes = await Note.objects.filter(completed=True).all()

# exact, iexact, contains, icontains, lt, lte, gt, gte, in
notes = await Note.objects.filter(text__icontains="mum").all()

# .get()
note = await Note.objects.get(id=1)

# .update()
await note.update(completed=True)

# .delete()
await note.delete()

note = await Note.objects.get(id=2)
assert note.pk == note.id == 2
```

Ormantic supports loading and filtering across foreign keys...

```python
import databases
import sqlalchemy
import ormantic as orm

database = databases.Database("sqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class Album(orm.Model):
    id: orm.Integer(primary_key=True) = None
    name: orm.String(max_length=100)

    class Mapping:
        table_name = "album"
        metadata = metadata
        database = database

class Track(orm.Model):
    id: orm.Integer(primary_key=True) = None
    album: orm.ForeignKey(Album)
    title: orm.String(max_length=100)
    position: orm.Integer()

    class Mapping:
        table_name = "track"
        metadata = metadata
        database = database

# Create the database
engine = sqlalchemy.create_engine(str(database.url))
metadata.create_all(engine)

# Create some records to work with.
malibu = await Album.objects.create(name="Malibu")
await Track.objects.create(album=malibu, title="The Bird", position=1)
await Track.objects.create(album=malibu, title="Heart don't stand a chance", position=2)
await Track.objects.create(album=malibu, title="The Waters", position=3)

fantasies = await Album.objects.create(name="Fantasies")
await Track.objects.create(album=fantasies, title="Help I'm Alive", position=1)
await Track.objects.create(album=fantasies, title="Sick Muse", position=2)

# Fetch an instance, without loading a foreign key relationship on it.
track = await Track.objects.get(title="The Bird")

# We have an album instance, but it only has the primary key populated
print(track.album)       # Album(id=1) [sparse]
print(track.album.pk)    # 1
print(track.album.name)  # Raises AttributeError

# Load the relationship from the database
await track.album.load()
assert track.album.name == "Malibu"

# This time, fetch an instance, loading the foreign key relationship.
track = await Track.objects.select_related("album").get(title="The Bird")
assert track.album.name == "Malibu"

# Fetch instances, with a filter across an FK relationship.
tracks = Track.objects.filter(album__name="Fantasies")
assert len(tracks) == 2

# Fetch instances, with a filter and operator across an FK relationship.
assert Track.objects.filter(album__name__iexact="fantasies").count() == 2
```

## Data types

The following keyword arguments are supported on all field types.

* `primary_key`
* `allow_null`
* `index`
* `unique`

The following column types are supported:

* `orm.Boolean()`
* `orm.Date()`
* `orm.DateTime()`
* `orm.Enum()`
* `orm.Float()`
* `orm.Integer()`
* `orm.JSON()`
* `orm.String(max_length)`
* `orm.StringArray()`
* `orm.Text()`
* `orm.Time()`

[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[orm]: https://github.com/encode/orm/
[fastapi]: https://github.com/tiangolo/fastapi/
[databases]: https://github.com/encode/databases/
[pydantic]: https://github.com/samuelcolvin/pydantic/

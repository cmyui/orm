from __future__ import annotations

from typing import Any, TypeVar

from orm.columns import Column

T = TypeVar("T")


class TableMeta(type):
    def __new__(
        cls: type["TableMeta"],
        name: str,
        bases: tuple[type[Any], ...],
        classdict: dict[str, Any],
    ) -> TableMeta:
        columns: list[Column] = []
        # constraints: list[Constraint] = []
        for v in classdict.values():
            if isinstance(v, Column):
                columns.append(v)
            # elif isinstance(v, Constraint):
            #     constraints.append(v)
        classdict["__columns__"] = tuple(columns)
        # classdict["__constraints__"] = tuple(constraints)
        return super().__new__(cls, name, bases, classdict)


class Table(metaclass=TableMeta):
    __tablename__: str
    __primary_key__: str
    __columns__: tuple[Column, ...]
    # __constraints__: tuple[Constraint, ...]


def table_instance(cls: type[T]) -> T:
    # XXX:HACK super based way to make them all instances
    # basically we get `Table` instead of `type[Table]`
    return cls()

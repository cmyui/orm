from __future__ import annotations

from typing import TYPE_CHECKING

from orm.columns import SqlLiteral
from orm.queries import Join, JoinType, Order, Query
from orm.tables import Table

if TYPE_CHECKING:
    from orm.columns import Column, Expression
    from orm.queries import Query


class Insert(Query):
    def __init__(self) -> None:
        self._into_table: Table | None = None
        self._values: list[tuple[Column, SqlLiteral]] = []
        # TODO: RETURNING
        super().__init__()

    def into_table(self, table: Table) -> Insert:
        self._into_table = table
        return self

    def values(self, values: list[tuple[Column, str]]) -> Insert:
        # TODO: should these ALWAYS be sql literals? i think no
        self._values = [(column, SqlLiteral(value)) for column, value in values]
        return self

    def convert_to_sql(self) -> str:
        assert self._into_table is not None, "into_table() must be set for insert()"

        sql = "INSERT INTO "
        sql += self._into_table.__tablename__
        sql += " ("
        sql += ", ".join([column._column_name for column, _ in self._values])
        sql += ") VALUES ("
        sql += ", ".join([value.convert_to_sql() for _, value in self._values])
        sql += ")"
        return sql


def insert() -> Insert:
    return Insert()

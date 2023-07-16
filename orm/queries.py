from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from orm.tables import Table

if TYPE_CHECKING:
    from orm.columns import Column, Expression


# TODO: should this be an ABC?
class Query(ABC):
    @abstractmethod
    def convert_to_sql(self) -> str:
        raise NotImplementedError


class JoinType(Enum):
    OUTER = "OUTER"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"


class Join:
    def __init__(
        self,
        type: JoinType,
        table: Table,
        conditions: list[Expression],
    ) -> None:
        self._table = table
        self._conditions = conditions
        self._type = type


class Order(Enum):
    ASC = "ASC"
    DESC = "DESC"


class Select(Query):
    # NOTE: allow table references here for `t.*` behaviour
    def __init__(self, expressions: list[Expression | Table]) -> None:
        self._expressions = expressions
        self._from_table: Table | None = None
        self._joins: list[Join] = []
        self._conditions: list[Expression] = []
        self._order_by: tuple[Column, Order] | None = None
        self._offset: int | None = None
        self._limit: int | None = None
        super().__init__()

    def from_table(self, table: Table) -> Select:
        self._from_table = table
        return self

    def outer_join(self, table: Table, conditions: list[Expression]) -> Select:
        self._joins.append(Join(JoinType.OUTER, table, conditions))
        return self

    def inner_join(self, table: Table, conditions: list[Expression]) -> Select:
        self._joins.append(Join(JoinType.INNER, table, conditions))
        return self

    def left_join(self, table: Table, on: list[Expression]) -> Select:
        self._joins.append(Join(JoinType.LEFT, table, on))
        return self

    def right_join(self, table: Table, conditions: list[Expression]) -> Select:
        self._joins.append(Join(JoinType.RIGHT, table, conditions))
        return self

    def where(self, conditions: list[Expression]) -> Select:
        self._conditions.extend(conditions)
        return self

    def order_by(self, column: Column, order: Order = Order.ASC) -> Select:
        self._order_by = (column, order)
        return self

    def offset(self, offset: int) -> Select:
        self._offset = offset
        return self

    def limit(self, limit: int) -> Select:
        self._limit = limit
        return self

    def convert_to_sql(self) -> str:
        assert self._from_table is not None, "from_table must be set for select()"

        query = "SELECT "
        query += ", ".join(
            (
                f"{expression.__tablename__}.*"
                if isinstance(expression, Table)
                # ^^ handle special case for table.*
                # vv handle normal case
                else expression.convert_to_sql()
            )
            for expression in self._expressions
        )
        query += " FROM "
        query += self._from_table.__tablename__
        if self._joins:
            query += " "
            query += " ".join(
                f"{join._type.value} JOIN {join._table.__tablename__} ON "
                + " AND ".join(
                    condition.convert_to_sql() for condition in join._conditions
                )
                for join in self._joins
            )
        if self._conditions:
            query += " WHERE "
            query += " AND ".join(
                condition.convert_to_sql() for condition in self._conditions
            )
        if self._offset is not None:
            query += f" OFFSET {self._offset}"
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
        return query


# NOTE: allow table references here for `t.*` behaviour
def select(expressions: list[Expression | Table]) -> Select:
    return Select(expressions=expressions)

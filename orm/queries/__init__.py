from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from orm.tables import Table

if TYPE_CHECKING:
    from orm.columns import Expression


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

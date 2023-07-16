from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from enum import Enum, auto
from typing import Any, Literal, Optional, Self, TypeAlias

from orm._typing import UNSET, Unset


class Constraint:
    def __init__(
        self,
        columns: Sequence[str],
        constraint_type: Literal["unique"],  # TODO: others
    ) -> None:
        self.columns = columns
        self.constraint_type = constraint_type


class OperationType(Enum):
    # unary ops
    NEG = auto()
    POS = auto()
    INVERT = auto()

    # binary ops
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()
    MOD = auto()
    POW = auto()
    FLOORDIV = auto()
    EQ = auto()
    NE = auto()
    GT = auto()
    GE = auto()
    LT = auto()
    LE = auto()
    IN = auto()
    NOT_IN = auto()
    IS_NULL = auto()
    IS_NOT_NULL = auto()
    LIKE = auto()
    NOT_LIKE = auto()
    ILIKE = auto()
    NOT_ILIKE = auto()
    BETWEEN = auto()
    NOT_BETWEEN = auto()

    def convert_to_sql(self) -> str:
        return {
            OperationType.NEG: "-",
            OperationType.POS: "+",
            OperationType.INVERT: "~",
            OperationType.IS_NULL: "IS",
            OperationType.ADD: "+",
            OperationType.SUB: "-",
            OperationType.MUL: "*",
            OperationType.DIV: "/",
            OperationType.MOD: "%",
            OperationType.POW: "**",
            OperationType.FLOORDIV: "//",
            OperationType.EQ: "=",
            OperationType.NE: "!=",
            OperationType.GT: ">",
            OperationType.GE: ">=",
            OperationType.LT: "<",
            OperationType.LE: "<=",
            OperationType.IN: "IN",
            OperationType.NOT_IN: "NOT IN",
            OperationType.LIKE: "LIKE",
            OperationType.NOT_LIKE: "NOT LIKE",
            OperationType.ILIKE: "ILIKE",
            OperationType.NOT_ILIKE: "NOT ILIKE",
            OperationType.BETWEEN: "BETWEEN",
            OperationType.NOT_BETWEEN: "NOT BETWEEN",
        }[self]


PrimitiveSharedPyTypes: TypeAlias = int | str | float


class UnaryOperation:  # e.g. "NOT a.account_id", "NOT accounts.account_id"
    def __init__(
        self,
        # For these ops, we'll allow python literals & translate
        # them into sql literals in our api for convenience
        reference: Expression | PrimitiveSharedPyTypes,
        operation: OperationType,
    ) -> None:
        if isinstance(reference, PrimitiveSharedPyTypes):
            reference = SqlLiteral(reference)
        self._reference = reference
        self._operation = operation

    def convert_to_sql(self) -> str:
        return "".join(
            [
                self._operation.convert_to_sql(),
                self._reference.convert_to_sql(),
            ]
        )


class BinaryOperation:  # e.g. "a + b", "accounts.account_id + 1"
    def __init__(
        self,
        # For these ops, we'll allow python literals & translate
        # them into sql literals in our api for convenience
        left_reference: Expression | PrimitiveSharedPyTypes,
        right_reference: Expression | PrimitiveSharedPyTypes,
        operation: OperationType,
    ) -> None:
        if isinstance(left_reference, PrimitiveSharedPyTypes):
            left_reference = SqlLiteral(left_reference)
        if isinstance(right_reference, PrimitiveSharedPyTypes):
            right_reference = SqlLiteral(right_reference)
        self._left_reference = left_reference
        self._right_reference = right_reference
        self._operation = operation

    def convert_to_sql(self) -> str:
        return " ".join(
            [
                self._left_reference.convert_to_sql(),
                self._operation.convert_to_sql(),
                self._right_reference.convert_to_sql(),
            ]
        )


class SqlLiteral:  # e.g. "1"
    def __init__(self, value: Any) -> None:
        self._value = value

    def convert_to_sql(self) -> str:
        if isinstance(self._value, int):
            return str(self._value)
        elif isinstance(self._value, str):
            return f"'{self._value}'"
        elif isinstance(self._value, float):
            return str(self._value)
        elif isinstance(self._value, datetime):
            return f"'{self._value.isoformat()}'"
        else:
            raise NotImplementedError(
                f"No implementation for this type: {type(self._value)}"
            )


class Column:  # e.g. "a.account_id", "accounts.account_id"
    def __init__(
        self,
        table_name: str,
        column_name: str,
        primary_key: bool = False,
        nullable: bool = False,
        default: Optional[PrimitiveSharedPyTypes] | Unset = UNSET,
    ) -> None:
        self._table_name = table_name
        self._column_name = column_name
        self._nullable = nullable
        self._primary_key = primary_key
        self._default = default
        super().__init__()

    # TODO: this is probably quite DRY-able

    def __neg__(self) -> UnaryOperation:
        return UnaryOperation(self, OperationType.NEG)

    def __pos__(self) -> UnaryOperation:
        return UnaryOperation(self, OperationType.POS)

    def __invert__(self) -> UnaryOperation:
        return UnaryOperation(self, OperationType.INVERT)

    # NOTE: we allow some primitive python types here for convenience

    def __eq__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.EQ)

    def __ne__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.NE)

    def __gt__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.GT)

    def __ge__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.GE)

    def __lt__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.LT)

    def __le__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.LE)

    def __contains__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.IN)

    def __add__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.ADD)

    def __sub__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.SUB)

    def __mul__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.MUL)

    def __truediv__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.DIV)

    def __mod__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.MOD)

    def __pow__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.POW)

    def __floordiv__(self, other: Self | PrimitiveSharedPyTypes) -> BinaryOperation:
        return BinaryOperation(self, other, OperationType.FLOORDIV)

    def convert_to_sql(self) -> str:
        return f"{self._table_name}.{self._column_name}"


class Integer(Column):
    ...


class String(Column):
    ...


class Float(Column):
    ...


class DateTime(Column):
    ...


class SqlEnum(Column):
    ...


Expression: TypeAlias = SqlLiteral | Column | UnaryOperation | BinaryOperation

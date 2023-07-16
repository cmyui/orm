from datetime import datetime
from typing import Any

from orm._typing import Unset
from orm.columns import Column, DateTime, Float, Integer, PrimitiveSharedPyTypes, String
from orm.functions import SqlFunction
from orm.tables import Table


def get_sql_type_from_column(column: Column) -> str:
    if isinstance(column, Integer):
        return "INTEGER"
    elif isinstance(column, String):
        return "TEXT"
    elif isinstance(column, DateTime):
        return "TIMESTAMP"
    elif isinstance(column, Float):
        return "FLOAT"
    else:
        raise NotImplementedError(f"No implementation for this type: {type(column)}")


def get_sql_type_from_py_type(py_type: Any) -> str:
    if isinstance(py_type, int):
        return "INTEGER"
    elif isinstance(py_type, str):
        return "TEXT"
    elif isinstance(py_type, datetime):
        return "TIMESTAMP"
    elif isinstance(py_type, float):
        return "FLOAT"
    elif py_type is None:
        return "NULL"
    else:
        raise NotImplementedError(f"No implementation for this type: {type(py_type)}")


def generate_up_migration_code(table: Table) -> str:
    """\
    A function to generate the up migration code for a table.

    CREATE TABLE accounts (
        account_id SERIAL PRIMARY KEY,
        account_type TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NULL DEFAULT NULL,
    );

    CREATE TABLE payments (
        payment_id SERIAL PRIMARY KEY,
        account_id INTEGER NOT NULL,
        amount FLOAT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NULL DEFAULT NULL,
    );
    """
    query = f"CREATE TABLE {table.__tablename__} (\n"
    for column in table.__columns__:
        nullable = "NULL" if column._nullable else "NOT NULL"
        primary_key = "PRIMARY KEY" if column._primary_key else ""
        if isinstance(column._default, Unset):
            default = ""
        else:
            if isinstance(column._default, SqlFunction):
                default = column._default.convert_to_sql()
            elif isinstance(column._default, PrimitiveSharedPyTypes | None):
                default = get_sql_type_from_py_type(column._default)
            else:
                raise NotImplementedError(
                    f"No implementation for this type: {type(column._default)}"
                )

            default = f"DEFAULT {default}"

        column_type = get_sql_type_from_column(column)
        if primary_key:
            column_type = "SERIAL"

        query += f"    {column._column_name} {column_type} {nullable}"
        if primary_key:
            query += f" {primary_key}"
        if default:
            query += f" {default}"
        query += ",\n"

    query = query[:-2]  # no trailing comma

    query += "\n);"
    return query

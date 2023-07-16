from __future__ import annotations

from types import TracebackType
from typing import Any

import databases

from orm.queries.select import Query


def construct_dsn(
    dialect: str,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
    driver: str | None = None,
) -> str:
    scheme = dialect
    if driver is not None:
        scheme += f"+{driver}"
    return f"{scheme}://{user}:{password}@{host}:{port}/{database}"


def build_query(query: Query) -> str:
    return query.convert_to_sql()


class Connection:
    def __init__(self, dsn: str) -> None:
        # TODO: eventually we may want to use asyncpg directly
        self._connection = databases.Database(dsn)

    async def __aenter__(self) -> Connection:
        await self._connection.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._connection.disconnect()

    async def fetch_one(self, query: Query | str) -> dict[str, Any] | None:
        if isinstance(query, Query):
            query = build_query(query)
        rec = await self._connection.fetch_one(query)

        # TODO: return an object of the result
        return dict(rec._mapping) if rec is not None else None

    async def fetch_all(self, query: Query | str) -> list[dict[str, Any]]:
        if isinstance(query, Query):
            query = build_query(query)
        print("fetch all", query)
        recs = await self._connection.fetch_all(query)
        return [dict(rec._mapping) for rec in recs]

    async def execute(self, query: Query | str) -> None:
        if isinstance(query, Query):
            query = build_query(query)
        await self._connection.execute(query)
        return None

    async def execute_many(self, query: Query | str, values: list[Any]) -> None:
        if isinstance(query, Query):
            query = build_query(query)
        await self._connection.execute_many(query, values)
        return None

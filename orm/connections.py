from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any

import databases

if TYPE_CHECKING:
    from orm.queries import Query


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

    async def fetch_one(self, query: Query) -> dict[str, Any] | None:
        sql = build_query(query)
        rec = await self._connection.fetch_one(sql)

        # TODO: return an object of the result
        return dict(rec._mapping) if rec is not None else None

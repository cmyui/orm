from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orm.tables import Table

TABLE_INSTANCES: dict[str, Table] = {}

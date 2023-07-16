#!/usr/bin/env python3
# postgres
# snowflake?
# mysql
# sqlite?
# mssql??
import asyncio
import hashlib

import asyncpg.exceptions

from orm import state
from orm.columns import DateTime, Float, Integer, String
from orm.connections import Connection, construct_dsn
from orm.functions import SqlFunction
from orm.queries import Order
from orm.queries.insert import insert
from orm.queries.select import select
from orm.sql_generation import generate_up_migration_code
from orm.tables import Table, table_instance


@table_instance
class Migrations(Table):
    __tablename__ = "migrations"
    __primary_key__ = "migration_id"

    migration_id = Integer("migrations", "migration_id", primary_key=True)
    migration_name = String("migrations", "migration_name")
    migration_hash = String("migrations", "migration_hash")
    created_at = DateTime("migrations", "created_at", default=SqlFunction.NOW)


@table_instance
class Accounts(Table):
    __tablename__ = "accounts"
    __primary_key__ = "account_id"

    account_id = Integer("accounts", "account_id", primary_key=True)
    account_type = String("accounts", "account_type")
    created_at = DateTime("accounts", "created_at", default=SqlFunction.NOW)
    updated_at = DateTime("accounts", "updated_at", nullable=True, default=None)


@table_instance
class Payments(Table):
    __tablename__ = "payments"
    __primary_key__ = "payment_id"

    payment_id = Integer("payments", "payment_id", primary_key=True)
    account_id = Integer("payments", "account_id")
    amount = Float("payments", "amount")
    created_at = DateTime("payments", "created_at", default=SqlFunction.NOW)
    updated_at = DateTime("payments", "updated_at", nullable=True, default=None)


async def async_main() -> int:
    dsn = construct_dsn(
        dialect="postgresql",
        user="postgres",
        password="lol123",
        host="localhost",
        port=5433,
        database="orm_testing",
        driver="asyncpg",
    )

    # run database migrations
    async with Connection(dsn) as connection:
        for table_name, table in state.TABLE_INSTANCES.items():
            migration_sql = generate_up_migration_code(table)
            migration_hash = hashlib.sha256(migration_sql.encode()).hexdigest()

            # check if migration has already been run
            query = (
                select([Migrations])
                .from_table(Migrations)
                .where(
                    [
                        Migrations.migration_name == table_name,
                        Migrations.migration_hash == migration_hash,
                    ]
                )
            )
            try:
                rec = await connection.fetch_one(query)
            except asyncpg.exceptions.UndefinedTableError:
                # if we're about to create the migrations table, let it pass
                # otherwise, raise the error because we won't be able to track
                # the creation of the table
                assert table_name == "migrations", "migrations table doesn't exist"
                rec = None

            if rec:
                continue

            await connection.execute(migration_sql)

            # insert migration record
            query = (
                insert()
                .into_table(Migrations)
                .values(
                    [
                        (Migrations.migration_name, table_name),
                        (Migrations.migration_hash, migration_hash),
                    ],
                )
            )
            await connection.execute(query)

    # run the application
    async with Connection(dsn) as connection:
        # SELECT a.account_id, a.account_type, p.*
        # FROM accounts AS a
        # LEFT JOIN payments AS p ON a.account_id = p.account_id
        # WHERE a.account_id = 1
        # ORDER BY a.account_id DESC
        # LIMIT 50
        # OFFSET 50

        query = (
            select([Accounts.account_id, Accounts.account_type, Payments])
            .from_table(Accounts)
            .left_join(Payments, on=[Accounts.account_id == Payments.account_id])
            .where([Accounts.account_id == 1])
            .order_by(Accounts.account_id, order=Order.DESC)
            .limit(50)
            .offset(0)
        )

        recs = await connection.fetch_all(query)

        print("Fetched results:")
        for result in recs:
            print(result)

    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    exit(main())

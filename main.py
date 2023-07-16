#!/usr/bin/env python3
# postgres
# snowflake?
# mysql
# sqlite?
# mssql??
import asyncio

from orm.columns import DateTime, Float, Integer, String
from orm.connections import Connection, construct_dsn
from orm.functions import SqlFunction
from orm.queries import Order, select
from orm.sql_generation import generate_up_migration_code
from orm.tables import Table, table_instance


@table_instance
class Accounts(Table):
    __tablename__ = "accounts"
    __primary_key__ = "account_id"

    # TODO: should we metaclass these to make it more DRY for our users?
    account_id = Integer("accounts", "account_id", primary_key=True)
    account_type = String("accounts", "account_type")
    created_at = DateTime("accounts", "created_at", default=SqlFunction.NOW)
    updated_at = DateTime("accounts", "updated_at", nullable=True, default=None)


@table_instance
class Payments(Table):
    __tablename__ = "payments"
    __primary_key__ = "payment_id"

    # TODO: should we metaclass these to make it more DRY for our users?
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

    create_accounts = generate_up_migration_code(Accounts)
    create_payments = generate_up_migration_code(Payments)

    print(create_accounts)
    print(create_payments)

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
            .offset(50)
        )
        await connection.fetch_one(query)

    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    exit(main())

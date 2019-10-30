import asyncpg
from typing import List, Optional, Tuple
from datetime import datetime, date
import asyncio

from . import bitfinex_api


async def get_database_pool(db_config) -> asyncpg.pool.Pool:
    db_pool: asyncpg.pool.Pool = await asyncpg.create_pool(**db_config)
    db_conn: asyncpg.Connection = await db_pool.acquire()
    await db_conn.execute('''
        CREATE TABLE IF NOT EXISTS currencies (
            id SERIAL UNIQUE,
            name varchar(3)
        );
    ''')
    await db_conn.execute('''
        CREATE TABLE IF NOT EXISTS rate (
            id SERIAL UNIQUE,
            currency_id SERIAL REFERENCES currencies(id),
            date TIMESTAMP,
            rate_usd REAL,
            volume REAL
        );
    ''')
    await db_conn.close()
    return db_pool


async def get_currencies(db_pool: asyncpg.pool.Pool) -> List[str]:
    async with db_pool.acquire() as db:
        return await db.fetch("SELECT id, name FROM currencies")


async def get_currencies_paged(
            db_pool: asyncpg.pool.Pool,
            page: int,
            page_size: int
        ) -> List[str]:
    page = int(page)
    assert page >= 0
    page_size = int(page_size)
    assert page_size > 0
    async with db_pool.acquire() as db:
        return await db.fetch(
            """
                SELECT id, name FROM currencies
                ORDER BY id DESC
                LIMIT $1 OFFSET $2
            """,
            page_size,
            page * page_size
        )


async def insert_currencies(
            db_pool: asyncpg.pool.Pool,
            currencies: List[str]):
    db = await db_pool.acquire()
    for currency in currencies:
        await db.execute("INSERT INTO currencies (name) VALUES ($1)", currency)
    await db.close()


async def get_oldest_rate_date_for(
            db_pool: asyncpg.pool.Pool,
            currency_id: int
        ) -> Optional[datetime]:
    db = await db_pool.acquire()
    # will yield None if no max
    response, = await db.fetch(
        "SELECT max(date) FROM rate WHERE currency_id=$1", currency_id)
    await db.close()
    return response["max"]


async def insert_new_rate(
            db_pool: asyncpg.pool.Pool,
            currency_id: int,
            rate: bitfinex_api.CurrencyRate):
    db = await db_pool.acquire()
    await db.execute(
        """
            INSERT INTO rate (currency_id, date, rate_usd, volume)
            VALUES ($1, $2, $3, $4)
        """,
        currency_id,
        rate.date,
        rate.close,
        rate.volume
    )
    await db.close()


async def update_rate(
            db_pool: asyncpg.pool.Pool,
            currency_id: int,
            rate: bitfinex_api.CurrencyRate):
    db = await db_pool.acquire()
    await db.execute(
        """
            UPDATE rate SET rate_usd=$1, volume=$2
            WHERE date=$3 AND currency_id=$4
        """,
        rate.close,
        rate.volume,
        rate.date,
        currency_id
    )
    await db.close()


async def save_rate_history(
            db_pool: asyncpg.pool.Pool,
            rate_history: List[bitfinex_api.CurrencyRate],
            currency_id: int):
    oldest_rate_date = await get_oldest_rate_date_for(db_pool, currency_id)
    if oldest_rate_date is None:
        await asyncio.gather(*[
            insert_new_rate(db_pool, currency_id, r)
            for r in rate_history
        ])
        return
    def get_rate_handler(rate):
        if rate.date > oldest_rate_date:
            return insert_new_rate(db_pool, currency_id, rate)
        return update_rate(db_pool, currency_id, rate)
    await asyncio.gather(*map(get_rate_handler, rate_history))


async def get_summary_for_period(
            db_pool: asyncpg.pool.Pool,
            currency: str,
            from_date: date,
            to_date: date
        ) -> Tuple[float, float]:
    assert isinstance(from_date, date)
    assert isinstance(to_date, date)
    assert from_date < to_date
    db = await db_pool.acquire()
    history = await db.fetch(
        """
            SELECT rate_usd, volume FROM rate
            LEFT JOIN currencies ON (currency_id = currencies.id)
            WHERE date >= $1 AND date <= $2 AND currencies.name = $3
        """,
        from_date,
        to_date,
        currency
    )
    if not history:
        return None, None
    mean_volume = sum(map(lambda h: h["volume"], history)) / len(history)
    latest_rate = history[-1]["rate_usd"]
    await db.close()
    return mean_volume, latest_rate

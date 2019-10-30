"""
Author: Egor Tyuvaev

Holds the high level functions to work with database
"""

from typing import List, Optional, Tuple
from datetime import datetime, date
import asyncio
import os

import asyncpg

from . import bitfinex_api


async def get_database_pool() -> asyncpg.pool.Pool:
    """
    Creates a database connection pool taking params from environment variables:
    * PG_USERNAME
    * PG_PASSWORD
    * PG_DATABASE, the database name
    * PG_HOST, IP or hostname of postgres instance
    """
    db_config = {
        "user": os.environ.get("PG_USERNAME"),
        "password": os.environ.get("PG_PASSWORD"),
        "database": os.environ.get("PG_DATABASE", "bc_api_data"),
        "host": os.environ.get("PG_HOST", "127.0.0.1")
    }
    db_pool: asyncpg.pool.Pool = await asyncpg.create_pool(**db_config)
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS currencies (
            id SERIAL UNIQUE,
            name varchar(3)
        );
    ''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS rate (
            id SERIAL UNIQUE,
            currency_id SERIAL REFERENCES currencies(id),
            date TIMESTAMP,
            rate_usd REAL,
            volume REAL
        );
    ''')
    return db_pool


async def get_currencies(db_pool: asyncpg.pool.Pool) -> List[str]:
    """
    Gets the currency list stored in database
    Returns the list of three-letter codes
    """
    return await db_pool.fetch("SELECT id, name FROM currencies")


async def get_currencies_paged(
            db_pool: asyncpg.pool.Pool,
            page: int,
            page_size: int
        ) -> List[str]:
    """
    Gets the currency list stored in database
    Supports pagination, pages start from 0
    """
    page = int(page)
    assert page >= 0
    page_size = int(page_size)
    assert page_size > 0
    return await db_pool.fetch(
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
    """
    Inserts new currencies into database
    Takes the list of three-letter codes
    To get the new ids, use `get_currencies`
    """
    await db_pool.executemany(
        "INSERT INTO currencies (name) VALUES ($1)",
        [(c,) for c in currencies]
    )


async def get_oldest_rate_date_for(
            db_pool: asyncpg.pool.Pool,
            currency_id: int
        ) -> Optional[datetime]:
    """
    Gets the date of the latest rate in database for the given currency id
    """
    response, = await db_pool.fetch(
        "SELECT max(date) FROM rate WHERE currency_id=$1",
        currency_id
    )
    return response["max"]


async def insert_new_rate(
            db_pool: asyncpg.pool.Pool,
            currency_id: int,
            rate: bitfinex_api.CurrencyRate):
    """
    Inserts a new rate
    """
    await db_pool.execute(
        """
            INSERT INTO rate (currency_id, date, rate_usd, volume)
            VALUES ($1, $2, $3, $4)
        """,
        currency_id,
        rate.date,
        rate.close,
        rate.volume
    )


async def update_rate(
            db_pool: asyncpg.pool.Pool,
            currency_id: int,
            rate: bitfinex_api.CurrencyRate):
    """
    Updates the existing rate
    """
    await db_pool.execute(
        """
            UPDATE rate SET rate_usd=$1, volume=$2
            WHERE date=$3 AND currency_id=$4
        """,
        rate.close,
        rate.volume,
        rate.date,
        currency_id
    )


async def save_rate_history(
            db_pool: asyncpg.pool.Pool,
            rate_history: List[bitfinex_api.CurrencyRate],
            currency_id: int):
    """
    Inserts the list of currency rates, inserts new values, uopdates old ones
    """
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
    """
    Gets the latest rate and the mean volume for the given date period and
    three-letter currency code
    """
    assert isinstance(from_date, date)
    assert isinstance(to_date, date)
    assert from_date < to_date
    history = await db_pool.fetch(
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
    return mean_volume, latest_rate

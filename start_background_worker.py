"""
Author: Egor Tyuvaev

Starts the background worker that calls the bitfinex API and updates old and
inserts new records to the database

Database configuration is taken from environment variables, see
`bc_api.database.get_database_pool` for details.
"""

import asyncio

import asyncio_throttle
import asyncpg
import aiohttp

from bc_api import bitfinex_api as bfapi
from bc_api import database as db


async def _main(session: aiohttp.ClientSession, db_pool: asyncpg.pool.Pool):
    """
    Updates the list of supported currencies in database,
    updates the currency rates
    """
    db_currencies = await db.get_currencies(db_pool)
    api_currencies = await bfapi.get_currencies(session)
    db_currencies_set = {c["name"] for c in db_currencies}
    new_currencies = set(api_currencies) - db_currencies_set
    if new_currencies:
        print("Found new currencies:", ", ".join(new_currencies))
        await db.insert_currencies(db_pool, new_currencies)
        db_currencies = await db.get_currencies(db_pool)
    cur_name_id_mapping = {
        c["name"]: c["id"]
        for c in db_currencies
    }

    throttler = asyncio_throttle.Throttler(rate_limit=40, period=60)

    async def handle_single_currency(currency: str):
        await throttler.acquire()
        print("Fetching", currency)
        rate_history = await bfapi.get_history_for(session, currency)
        print("Updating", currency)
        await db.save_rate_history(
            db_pool, rate_history, cur_name_id_mapping[currency])

    await asyncio.gather(*map(handle_single_currency, api_currencies))


async def main():
    """
    Wraps _main to close db connection and aiohttp session on unhandled
    exception
    """
    db_pool = await db.get_database_pool()
    session = aiohttp.ClientSession()
    try:
        await _main(session, db_pool)
    finally:
        await db_pool.close()
        await session.close()


asyncio.get_event_loop().run_until_complete(main())

import aiohttp
import asyncpg
import asyncio
import os
import asyncio_throttle

from bc_api import bitfinex_api as bfapi
from bc_api import database as db


async def main():
    db_config = {
        "user": os.environ.get("PG_USERNAME"),
        "password": os.environ.get("PG_PASSWORD"),
        "database": os.environ.get("PG_DATABASE", "bc_api_data"),
        "host": os.environ.get("PG_HOST", "127.0.0.1")
    }

    session = aiohttp.ClientSession()
    db_pool = await db.get_database_pool(db_config)

    db_currencies = await db.get_currencies(db_pool)
    db_currencies_set = set([c["name"] for c in db_currencies])
    api_currencies = await bfapi.get_currencies(session)
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

    await session.close()
    await db_pool.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

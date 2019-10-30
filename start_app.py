from aiohttp import web
import asyncpg
import os

from bc_api import views, database


async def init_app(db_config):
    app = web.Application()
    app["db_pool"] = await database.get_database_pool(db_config)
    app.add_routes([
        web.get("/currencies", views.currencies),
        web.get("/rate/{currency}", views.rate)
    ])
    return app


DB_CONFIG = {
    "user": os.environ.get("PG_USERNAME"),
    "password": os.environ.get("PG_PASSWORD"),
    "database": os.environ.get("PG_DATABASE", "bc_api_data"),
    "host": os.environ.get("PG_HOST", "127.0.0.1")
}


web.run_app(init_app(DB_CONFIG))
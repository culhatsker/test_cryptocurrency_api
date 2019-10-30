"""
Author: Egor Tyuvaev

Starts the API app

Database configuration is taken from environment variables, see
`bc_api.database.get_database_pool` for details.
"""

from aiohttp import web

from bc_api import views, database


async def init_app():
    """
    Setups the app
    """
    app = web.Application()
    app["db_pool"] = await database.get_database_pool()
    app.add_routes([
        web.get("/currencies", views.currencies),
        web.get("/rate/{currency}", views.rate)
    ])
    return app


web.run_app(init_app())

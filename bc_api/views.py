from aiohttp import web
from string import ascii_letters
import json
from datetime import datetime, timedelta

from . import database


CURRENCY_ALPHABET = set(ascii_letters)


def make_json_response(data, status=200):
    resp = web.Response(status=status)
    resp.body = json.dumps(data)
    resp.headers["Content-Type"] = "application/json"
    return resp


def make_json_error(description):
    return make_json_response({"error": description}, status=500)


async def currencies(request: web.Request) -> web.Response:
    """
    Returns http resposne that contains the list of currencies
    query params:
    * page_size, if it's not set, page has no effect
    * page, the page id, starts from 0, 0 by default
    """
    db_pool = request.app["db_pool"]
    page_size = request.query.get("page_size")
    if page_size is None:
        currencies = await database.get_currencies(db_pool)
    else:
        page = request.query.get("page", 0)
        currencies = await database.get_currencies_paged(
            db_pool, page, page_size)
    return make_json_response([
        {"id": c["id"], "name": c["name"]}
        for c in currencies
    ])


async def rate(request: web.Request) -> web.Response:
    db_pool = request.app["db_pool"]
    currency = request.match_info["currency"]
    try:
        assert len(currency) == 3
        assert not(set(currency) - CURRENCY_ALPHABET)
    except AssertionError:
        return make_json_error("Currency should be a three letters string.")
    currency = currency.upper()
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=10)
    mean_vol, last_rate = await database.get_summary_for_period(
        db_pool, currency, from_date, to_date)
    if mean_vol is None:
        return make_json_error("No data for this currency")
    return make_json_response(
        {"mean_volume": mean_vol, "latest_rate": last_rate})
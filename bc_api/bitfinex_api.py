import json
from datetime import datetime
from aiohttp import ClientSession, ClientResponse
from dataclasses import dataclass
import asyncio

CANDLE_URL = "https://api-pub.bitfinex.com/v2/candles/trade:1D:t{currency}USD/hist?limit=10"
TICKERS_URL = "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL"


class APIError(Exception):
    pass


@dataclass
class CurrencyRate:
    date: datetime
    currency: str
    close: float
    volume: float


async def parse_api_response(resp: ClientResponse):
    resp.raise_for_status()
    resp = json.loads(await resp.text())
    if "error" in resp:
        raise APIError(resp["error"])
    return resp


async def get_currencies(session: ClientSession):
    async with session.get(TICKERS_URL) as resp:
        tickers_resp = json.loads(await resp.text())

    def currency_filter(curr_obj):
        name = curr_obj[0]
        return name[0] == "t" and name[4:] == "USD"

    currencies = [
        cur[0][1:4]
        for cur in filter(currency_filter, tickers_resp)
    ]
    return currencies


async def get_history_for(session: ClientSession, currency: str):
    async with session.get(CANDLE_URL.format(currency=currency)) as resp:
        resp = await parse_api_response(resp)
        return [
            CurrencyRate(
                date=datetime.utcfromtimestamp(item[0] / 1000),
                currency=currency,
                close=item[2],
                volume=item[5]
            )
            for item in resp
        ]
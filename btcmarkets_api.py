"""
TODO:
 - Handle leap years?
 - Log dummy market operations
 - Full testing suite
 - Look at order history vs trade history
 - Look at types of orders
 - Move error checking to DummyMarket
 - Set timeouts or number of retries
 - Handle divisors better
 - logger.warning or logging.warning?
 - Set up warnings with warnings module?
 - Automatically use parse_json_string (refactor?)
 - Automatically check instrument and currency types to ensure they're valid
 - Set retry intervals and max retries/timeout
 - Complete BTCMarkets API coverage
 - Create more custom types for api values
 - Better DummyMarket? Should DummyMarket require API keys? Need a market for testing that doesn't require API keys.
 - Properly support optional parameters (e.g. 'since' in Trade requests)
 - Better logging formatting (stdout + file?)
"""

import requests
import time
from collections import OrderedDict, namedtuple
import hmac
import base64
import hashlib
import json
import uuid
import logging
from typing import Dict, Union, List

Balance = namedtuple("Balance", ["balance", "pendingFunds"])
# Candlestick used for price history, Tick used for current updates
Candlestick = namedtuple("Candlestick", ["timestamp", "open", "high", "low", "close", "volume"])
Tick = namedtuple("Tick", ["bestBid", "bestAsk", "lastPrice", "currency", "instrument", "timestamp", "volume24h"])
# Order = namedtuple("Order", [])
OrderBook = namedtuple("Orderbook", ["timestamp", "asks"])

VALID_INSTRUMENTS = ("AUD", "ETH", "BTC", "XRP", "ETC", "LTC", "BCH")

# Seconds since unix epoch that trades opened on BTCMarkets
OPENING = 1344760668851

# Amount to divide timestamps received from BTCMarkets by
DIVISOR_TIME = 1000
# Amount to divide instrument quantities received from BTCMarkets by
DIVISOR_INST = 100000000

# Number of seconds in time constants
HOUR_S = 60*60
HOUR_MS = HOUR_S*1000
DAY_MS = HOUR_MS * 24
WEEK_MS = DAY_MS * 7
YEAR_MS = DAY_MS * 365

ORDER_STATUSES = ("New", "Placed", "Failed", "Error", "Cancelled", "Partially Cancelled", "Fully Matched", "Partially Matched")
CALL_LIMITED_25 = ("/order/open", "/order/cancel", "/order/detail", "/account/balance", "/market/")


logging.basicConfig(filename="btcmarkets_api.log", level=logging.INFO)
logger = logging.getLogger(__name__)


def download_history(instrument, currency="AUD", interval="day") -> List[Candlestick]:
    """
    Download available pair history used on BTCMarkets home page.
    """
    if interval not in ("day", "minute", "hour"):  # TODO: what other options are available?
        raise ValueError("Interval must be one of: \"day\", \"hour\", \"minute\".")

    r = requests.get(
        "https://btcmarkets.net/data/market/BTCMarkets/{}/{}/tickByTime?timeWindow={}&since={}".format(instrument,
                                                                                                       currency,
                                                                                                       interval,
                                                                                                       OPENING))
    ticks = r.json()["ticks"]

    to_return = []
    for tick in ticks:
        to_return.append(Candlestick(*tick))

    return to_return


class DummyMarket(object):
    """
    Base Market object that logs methods and can access the public BTCMarkets api
    but doesn't implement private/account methods.
    Can be inherited from for backtesting.
    """
    def __init__(self, public_key, private_key, base_url="https://api.btcmarkets.net", logging=True):
        self.BASE_URL = base_url

        self.public_key = public_key
        self.private_key = private_key
        if isinstance(self.public_key, str):
            self.public_key = self.public_key.encode()

        if isinstance(self.private_key, str):
            self.private_key = self.private_key.encode()

        self.cached_fee = {}

        self._prev_call = {}

        if not logging:
            logger.disabled = True

    @staticmethod
    def current_timestamp() -> int:
        return int(time.time() * DIVISOR_TIME)

    @staticmethod
    def to_json_string(*args: Union[str, int]) -> str:
        """
        Convert key/value pairs to a json string.
        """
        num_args = len(args)
        if num_args % 2 != 0:
            raise ValueError("to_json_string needs a value for every key.")

        the_dict = OrderedDict()

        i = 0
        while i < num_args:
            the_dict[args[i]] = args[i + 1]

            i += 2

        return json.dumps(the_dict).replace(" ", "")

    @staticmethod
    def parse_json_string(the_string) -> List[Dict[str, Union[str, int]]]:
        """
        Convert json string to list of dictionaries.
        """

        def parse_dict(the_dict):
            pairs = the_dict.split(",")

            new_dict = {}
            for pair in pairs:
                pair = pair.replace(" ", "").replace("{", "").replace("}", "")
                key, value = pair.split(":")

                # There are no floating point values
                if not value.startswith("'"):
                    value = int(value)

                new_dict[key] = value

            return new_dict

        if the_string.startswith("["):
            # Strip the brackets off the ends
            the_string = the_string[1:-1]
            the_string.split("},")

            to_return = []
            for dictionary in the_string:
                to_return.append(parse_dict(dictionary))

            return to_return

        return [parse_dict(the_string)]

    def _limit_call(self, amount_per_10: int) -> None:
        """
        Used for limiting the number of times a certain request can be performed in a 10 second window.
        Causes the request to halt until the query can be performed preventing API lockout.
        """
        the_time = time.time()

        try:
            prev_call = self._prev_call[amount_per_10]

        except KeyError:
            self._prev_call[amount_per_10] = [the_time]

        else:
            # Remove calls that have expired
            self._prev_call[amount_per_10] = [i for i in self._prev_call[amount_per_10] if the_time - i < 10]
            the_list = self._prev_call[amount_per_10]

            if len(the_list) < amount_per_10:
                the_list.append(the_time)
                self._prev_call[amount_per_10] = the_list

            else:
                to_wait = 10 - (the_time - self._prev_call[amount_per_10][0])

                logger.warning("Throttle limit exceeded. Waiting for {} seconds.".format(to_wait))
                time.sleep(to_wait)

    def _create_headers(self, path: str, data="") -> OrderedDict:
        """
        Create the headers needed for a request to the BTCMarkets api.
        """
        logger.debug("Creating headers for path: {} with data: {}".format(path, data))
        if path.startswith("/market/") or path in CALL_LIMITED_25:
            self._limit_call(25)

        else:
            self._limit_call(10)

        the_timestamp = self.current_timestamp()
        body = "{}\n{}\n{}".format(path, the_timestamp, data).encode("utf-8")
        rsig = hmac.new(self.private_key, body, hashlib.sha512)
        bsig = base64.standard_b64encode(rsig.digest())

        return OrderedDict([("Accept", "application/json"),
                            ("Accept-Charset", "UTF-8"),
                            ("Content-Type", "application/json"),
                            ("apikey", self.public_key),
                            ("timestamp", str(the_timestamp)),
                            ("signature", bsig)])

    def post_request(self, path, data):
        # Doesn't include data in logging call as it's hashed
        logger.debug("Sending POST request to path: {}".format(path))

    def get_request(self, path: str) -> requests.Response:
        logger.debug("Sending GET request to path: {}".format(path))

        retry = True
        the_request = None
        while retry:
            try:
                the_request = requests.get("{}{}".format(self.BASE_URL, path),
                                           headers=self._create_headers(path),
                                           verify=True)
                retry = False

            except requests.exceptions.ConnectionError:
                logger.error("Lost internet connection. Retrying...")
                time.sleep(1)

        while the_request.status_code != 200:
            logger.error("GET request with path {} failed with status code: {}".format(path, the_request.status_code))
            logger.error("Retrying...")
            time.sleep(1)
            the_request = requests.get("{}{}".format(self.BASE_URL, path),
                                       headers=self._create_headers(path),
                                       verify=True)

        return the_request

    def create_order(self, volume, price, bid_ask, type="Limit", currency="AUD", instrument="ETH"):
        if bid_ask not in ("Bid", "Ask"):
            raise ValueError("bid_ask must be one of: \"Bid\", \"Ask\".")

        if type not in ("Limit", "Market"):
            raise ValueError("type must be one of: \"Limit\", \"Market\"")

        logger.info("Creating {} order {} for {} {} with {} {}".format(type, "bidding" if price == "Bid" else "asking",
                                                                       volume, instrument, price, currency))

    def cancel_order(self, order_id):
        logger.info("Cancelling order {}".format(order_id))

    def open_orders(self, limit=0):
        logger.info("Getting open orders up to maximum of {}".format(limit))

    def cancel_last_order(self):
        logger.info("Cancelling last order...")

    def get_balances(self):
        logger.info("Getting account balance.")

    def get_history(self, limit=0, status="*", include_cancelled=False):
        logger.info("Getting account history with status {} up to a limit of {} with include_cancelled={}"
                    .format(status, limit, include_cancelled))

    def get_tick(self, instrument, currency="AUD") -> Tick:
        logger.info("Getting current {}:{} tick".format(currency, instrument))

        # Ensure results are ordered
        data = self.get_request("/market/{}/{}/tick".format(instrument, currency)).json()
        the_tick = Tick(bestBid=data["bestBid"], bestAsk=data["bestAsk"], lastPrice=data["lastPrice"],
                        currency=data["currency"], instrument=data["instrument"], timestamp=data["timestamp"],
                        volume24h=data["volume24"])

        return the_tick

    def get_orderbook(self, instrument, currency="AUD") -> OrderBook:
        # TODO: Format the orderbook, custom type + hinting
        logger.info("Getting current {}:{} orderbook".format(currency, instrument))

        data = self.get_request("/market/{}/{}/orderbook".format(instrument, currency)).json()
        the_orderbook = OrderBook(timestamp=data["timestamp"], asks=data["asks"])

        return the_orderbook

    def get_trading_fee(self, instrument: str, currency: str, force_update=True) -> float:
        """
        Get the trading fee for a specified currency pair as a percentage. #TODO: Check this
        If force_update is set to false, the fee will be cached for an hour - useful in the case of a program which
        wants to query the fee every time a trade is performed.
        """
        logger.info("Getting current trading fee for {}:{}...".format(currency, instrument))
        if not force_update:
            # Cache the trading fee and update it only every hour
            try:
                cached = self.cached_fee["{}/{}".format(instrument, currency)]

            except KeyError:
                pass

            else:
                if time.time() - cached[0] < HOUR_S:
                    logger.info("Using cached value.")
                    return cached[1]

        logger.info("Using current value and updating cache.")
        response = self.get_request("/account/{}/{}/tradingfee".format(instrument, currency))
        the_rate = round(int(response.json()["tradingFeeRate"]) / DIVISOR_INST, 4)
        self.cached_fee["{}/{}".format(instrument, currency)] = (time.time(), the_rate)

        return the_rate


class Market(DummyMarket):
    """
    Market object implements post requests for account access and trading operations.
    """
    def __init__(self, public_key, private_key, logging=False):
        super().__init__(public_key, private_key, logging=logging)

    def post_request(self, path: str, data: str) -> requests.Response:
        super().post_request(path, data)

        retry = True
        the_request = None
        while retry:
            try:
                the_request = requests.post("{}{}".format(self.BASE_URL, path),
                                            data=data,
                                            headers=self._create_headers(path, data),
                                            verify=True)

                retry = False

            except requests.exceptions.ConnectionError:
                logger.error("Lost internet connection. Retrying...")
                time.sleep(1)

        while the_request.status_code != 200:
            logger.error("POST request with path {} failed with status code: {}".format(path, the_request.status_code))
            logger.error("Retrying...")
            time.sleep(1)
            the_request = requests.post("{}{}".format(self.BASE_URL, path),
                                        data=data,
                                        headers=self._create_headers(path, data),
                                        verify=True)

        return the_request

    def create_order(self, volume: int, price: int, bid_ask: str, type="Limit", currency="AUD", instrument="ETH"):
        bid_ask = bid_ask.title()
        type = type.title()
        super().create_order(volume, price, bid_ask, type, currency, instrument)

        return self.post_request("/order/create", self.to_json_string("currency", currency, "instrument", instrument,
                                                                    "price", price * DIVISOR_INST,
                                                                    "volume", volume * DIVISOR_INST,
                                                                    "orderSide", bid_ask,
                                                                    "ordertype", type,
                                                                    "clientRequestId", str(uuid.uuid4())))

    def cancel_order(self, order_id): # Check what format the order_id needs to be in
        super().cancel_order(order_id)
        return self.post_request("order/cancel", self.to_json_string("orderIds", order_id))

    def open_orders(self, limit=0):
        super().open_orders(limit)
        return self.post_request("/order/open", self.to_json_string("currency", "AUD",
                                                                  "instrument", "ETH",
                                                                  "limit", limit,
                                                                  "since", 1)).json()["orders"]

    def cancel_last_order(self):
        super().cancel_last_order()
        try:
            the_id = (self.open_orders())[-1]["id"]

        except IndexError:
            logger.warning("Cannot cancel order: no open orders found.")

        else:
            return self.cancel_order(the_id)

    def get_balances(self) -> Dict[str, Balance]:
        super().get_balances()

        the_json = self.get_request("/account/balance").json()

        to_return = {}
        for dictionary in the_json:
            to_return[dictionary["currency"]] = Balance(dictionary["balance"] / DIVISOR_INST,
                                                        dictionary["pendingFunds"] / DIVISOR_INST)

        return to_return

    def get_history(self, limit=0, status="*", include_cancelled=False):
        super().get_history(limit, status, include_cancelled)

        status = status.title()
        assert status in ("*", "New", "Error", "Partially Cancelled", "Placed", "Cancelled", "Fully Matched",
                          "Partially Matched", "Failed")

        the_orders = self.post_request("/order/history", self.to_json_string("currency", "AUD",
                                                                           "instrument", "ETH",
                                                                           "limit", limit,
                                                                           "since", 1)).json()["orders"]

        return [order for order in the_orders if ((True if include_cancelled else order["status"] != "Cancelled")
                       and (status == "*" or order["status"] == status))]

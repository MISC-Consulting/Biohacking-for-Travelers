"""
data_pipeline.py
----------------
Fetches and manages 15-minute OHLCV historical data from cTrader OpenAPI
for the USD Basket: EURUSD, GBPUSD, AUDUSD, USDJPY, XAUUSD

Integrates with your existing Twisted/protobuf api_service.py infrastructure.
Stores bars in MongoDB with incremental updates (only fetches what's missing).
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from twisted.internet import defer, reactor
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

# cTrader proto imports — adjust path to match your project structure
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *

logger = logging.getLogger(__name__)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

# Symbol map: name → cTrader symbolId
# Verify these against your account with ProtoOASymbolsListReq if needed
BASKET_SYMBOLS = {
    "EURUSD": 1,
    "GBPUSD": 2,
    "AUDUSD": 10,
    "USDJPY": 5,
    "XAUUSD": 41,
}

# Which direction each pair maps to USD strength
# 1 = pair moves UP when USD strengthens, -1 = moves DOWN
USD_DIRECTION = {
    "EURUSD": -1,   # EUR/USD falls when USD strong
    "GBPUSD": -1,
    "AUDUSD": -1,
    "USDJPY":  1,   # USD/JPY rises when USD strong
    "XAUUSD": -1,   # Gold falls when USD strong
}

# cTrader TrendbarPeriod enum values
PERIOD_M15 = 4   # ProtoOATrendbarPeriod.M15
PERIOD_MAP = {
    "M1": 1, "M2": 2, "M3": 3, "M4": 3, "M5": 4,
    "M10": 5, "M15": 6, "M30": 7, "H1": 8, "H4": 9,
    "H12": 10, "D1": 11, "W1": 12, "MN1": 13,
}
# Note: Use the actual enum integer from your protobuf — check with:
#   ProtoOATrendbarPeriod.Value('M15')
M15_PERIOD = 6   # ProtoOATrendbarPeriod.M15 in most cTrader proto versions

# Max bars cTrader returns per request (API limit)
MAX_BARS_PER_REQUEST = 5000

# How many bars of history to maintain (2 years @ 15min = ~70,080 bars)
HISTORY_BARS = 70_000

# MongoDB collection names
COLLECTION_PREFIX = "bars_15m"


# ─── MONGODB HELPER ───────────────────────────────────────────────────────────

class BarStore:
    """
    Thin MongoDB wrapper for OHLCV bar storage.
    Each symbol gets its own collection: bars_15m_EURUSD, bars_15m_USDJPY, etc.
    Index on 'timestamp' (UTC epoch ms) ensures fast range queries and dedup.
    """

    def __init__(self, mongo_uri: str, db_name: str = "usd_basket"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self._ensure_indexes()

    def _collection(self, symbol: str):
        return self.db[f"{COLLECTION_PREFIX}_{symbol}"]

    def _ensure_indexes(self):
        for symbol in BASKET_SYMBOLS:
            col = self._collection(symbol)
            col.create_index([("timestamp", ASCENDING)], unique=True)
            logger.info(f"Index ensured for {symbol}")

    def upsert_bars(self, symbol: str, bars: list[dict]) -> int:
        """Insert bars, skip duplicates. Returns count inserted."""
        if not bars:
            return 0
        col = self._collection(symbol)
        ops = [
            {
                "updateOne": {
                    "filter": {"timestamp": b["timestamp"]},
                    "update": {"$set": b},
                    "upsert": True,
                }
            }
            for b in bars
        ]
        try:
            result = col.bulk_write(ops, ordered=False)
            inserted = result.upserted_count + result.modified_count
            return inserted
        except BulkWriteError as e:
            logger.warning(f"BulkWrite partial error for {symbol}: {e.details}")
            return 0

    def get_latest_timestamp(self, symbol: str) -> int | None:
        """Returns epoch ms of the most recent bar, or None if empty."""
        col = self._collection(symbol)
        doc = col.find_one(sort=[("timestamp", DESCENDING)])
        return doc["timestamp"] if doc else None

    def get_bars(self, symbol: str, n: int = 500) -> list[dict]:
        """Returns the N most recent bars, oldest first."""
        col = self._collection(symbol)
        cursor = col.find({}, {"_id": 0}).sort("timestamp", DESCENDING).limit(n)
        return list(reversed(list(cursor)))

    def get_bars_since(self, symbol: str, since_ms: int) -> list[dict]:
        """Returns all bars after since_ms, oldest first."""
        col = self._collection(symbol)
        cursor = col.find(
            {"timestamp": {"$gte": since_ms}}, {"_id": 0}
        ).sort("timestamp", ASCENDING)
        return list(cursor)

    def bar_count(self, symbol: str) -> int:
        return self._collection(symbol).count_documents({})


# ─── DATA PIPELINE ────────────────────────────────────────────────────────────

class DataPipeline:
    """
    Fetches 15-min OHLCV bars from cTrader OpenAPI and stores in MongoDB.

    Usage (within your Twisted event loop):
        pipeline = DataPipeline(api_client, account_id, bar_store)
        d = pipeline.initialize_all()          # Full backfill on startup
        d.addCallback(lambda _: pipeline.start_live_updates())  # Then live
    """

    def __init__(self, client, account_id: int, bar_store: BarStore):
        self.client = client          # Your existing cTrader client
        self.account_id = account_id
        self.store = bar_store
        self._callbacks = []          # Registered listeners for new bars
        self._initialized = set()

    # ── Public API ─────────────────────────────────────────────────────────

    def on_new_bars(self, callback):
        """Register a callback(symbol, bars) called when new bars arrive."""
        self._callbacks.append(callback)

    @defer.inlineCallbacks
    def initialize_all(self):
        """
        Backfill all 5 symbols. Fetches only missing bars (incremental).
        Call once at startup before enabling live updates.
        """
        logger.info("=== Starting data backfill for all basket symbols ===")
        for symbol, symbol_id in BASKET_SYMBOLS.items():
            yield self._backfill_symbol(symbol, symbol_id)
            self._initialized.add(symbol)
        logger.info("=== Backfill complete ===")
        self._log_bar_counts()

    @defer.inlineCallbacks
    def refresh_all(self):
        """
        Incremental update — fetches only bars since last stored bar.
        Call every 15 minutes to keep data current.
        """
        for symbol, symbol_id in BASKET_SYMBOLS.items():
            yield self._incremental_update(symbol, symbol_id)

    def get_latest_bars(self, symbol: str, n: int = 200) -> list[dict]:
        """Synchronous read of N most recent bars for a symbol."""
        return self.store.get_bars(symbol, n)

    def get_all_latest(self, n: int = 200) -> dict[str, list[dict]]:
        """Returns {symbol: [bars]} for all 5 pairs."""
        return {sym: self.get_latest_bars(sym, n) for sym in BASKET_SYMBOLS}

    # ── Internal: Backfill ─────────────────────────────────────────────────

    @defer.inlineCallbacks
    def _backfill_symbol(self, symbol: str, symbol_id: int):
        """
        Fetches up to HISTORY_BARS bars going back 2+ years.
        Paginates backward using cTrader's fromTimestamp parameter.
        """
        existing_count = self.store.bar_count(symbol)
        latest_ts = self.store.get_latest_timestamp(symbol)

        if existing_count >= HISTORY_BARS * 0.95:
            logger.info(f"{symbol}: Already have {existing_count} bars, skipping full backfill")
            yield self._incremental_update(symbol, symbol_id)
            return

        logger.info(f"{symbol}: Starting backfill (have {existing_count} bars, need {HISTORY_BARS})")

        # Start from now and paginate backward
        to_timestamp_ms = int(time.time() * 1000)
        total_inserted = 0

        # If we have some bars already, only fetch what's missing
        if latest_ts:
            target_from = latest_ts - (HISTORY_BARS * 15 * 60 * 1000)
        else:
            target_from = to_timestamp_ms - (HISTORY_BARS * 15 * 60 * 1000)

        batch = 0
        while to_timestamp_ms > target_from:
            batch += 1
            logger.debug(f"{symbol}: Fetching batch {batch}, to={_ms_to_dt(to_timestamp_ms)}")

            bars = yield self._fetch_bars(
                symbol_id=symbol_id,
                from_ts_ms=max(target_from, to_timestamp_ms - MAX_BARS_PER_REQUEST * 15 * 60 * 1000),
                to_ts_ms=to_timestamp_ms,
            )

            if not bars:
                logger.info(f"{symbol}: No more bars available (batch {batch})")
                break

            inserted = self.store.upsert_bars(symbol, bars)
            total_inserted += inserted
            to_timestamp_ms = bars[0]["timestamp"] - 1  # paginate backward

            logger.info(f"{symbol}: Batch {batch} → {len(bars)} bars fetched, {inserted} new")

            # Small pause to avoid hammering the API
            yield _sleep(0.5)

        logger.info(f"{symbol}: Backfill done — {total_inserted} bars inserted, {self.store.bar_count(symbol)} total")

    @defer.inlineCallbacks
    def _incremental_update(self, symbol: str, symbol_id: int):
        """Fetch bars since the last stored timestamp."""
        latest_ts = self.store.get_latest_timestamp(symbol)
        if latest_ts is None:
            logger.warning(f"{symbol}: No bars in DB, run full backfill first")
            return

        from_ts = latest_ts  # inclusive — cTrader will return the bar at this ts too
        to_ts = int(time.time() * 1000)

        bars = yield self._fetch_bars(symbol_id, from_ts, to_ts)
        if bars:
            inserted = self.store.upsert_bars(symbol, bars)
            if inserted > 0:
                new_bars = [b for b in bars if b["timestamp"] > latest_ts]
                logger.info(f"{symbol}: Incremental update — {inserted} new bars")
                self._notify(symbol, new_bars)

    # ── Internal: API Call ─────────────────────────────────────────────────

    @defer.inlineCallbacks
    def _fetch_bars(self, symbol_id: int, from_ts_ms: int, to_ts_ms: int) -> list[dict]:
        """
        Sends ProtoOAGetTrendbarsReq and parses the response into dicts.
        Returns list of bar dicts: {timestamp, open, high, low, close, volume}
        All prices in raw cTrader units (divide by 10^5 for 5-decimal pairs,
        10^3 for JPY pairs — your pip_value() helper handles this).
        """
        request = ProtoOAGetTrendbarsReq()
        request.ctidTraderAccountId = self.account_id
        request.symbolId = symbol_id
        request.period = M15_PERIOD
        request.fromTimestamp = from_ts_ms
        request.toTimestamp = to_ts_ms

        deferred = self.client.send(request)
        response = yield deferred

        bars = []
        for bar in response.trendbar:
            # cTrader gives open + deltas; reconstruct OHLCV
            o = bar.open
            h = bar.open + bar.high   # high is delta from open
            l = bar.open + bar.low    # low is delta (negative)
            c = bar.open + bar.close  # close is delta from open

            bars.append({
                "timestamp": bar.utcTimestampInMinutes * 60 * 1000,  # → epoch ms
                "open":   o,
                "high":   h,
                "low":    l,
                "close":  c,
                "volume": bar.volume,
            })

        # Sort oldest first
        bars.sort(key=lambda b: b["timestamp"])
        return bars

    # ── Internal: Helpers ─────────────────────────────────────────────────

    def _notify(self, symbol: str, bars: list[dict]):
        for cb in self._callbacks:
            try:
                cb(symbol, bars)
            except Exception as e:
                logger.error(f"Callback error for {symbol}: {e}")

    def _log_bar_counts(self):
        logger.info("── Bar counts ──────────────────────")
        for symbol in BASKET_SYMBOLS:
            count = self.store.bar_count(symbol)
            latest = self.store.get_latest_timestamp(symbol)
            dt_str = _ms_to_dt(latest) if latest else "N/A"
            logger.info(f"  {symbol:8s} {count:6d} bars  latest={dt_str}")
        logger.info("────────────────────────────────────")


# ─── LIVE BAR SCHEDULER ───────────────────────────────────────────────────────

class LiveBarScheduler:
    """
    Triggers incremental data updates on the 15-minute bar close.
    Works within the Twisted reactor — no threads needed.
    """

    def __init__(self, pipeline: DataPipeline, lookback_seconds: int = 30):
        self.pipeline = pipeline
        self.lookback = lookback_seconds  # fetch slightly before bar close
        self._running = False

    def start(self):
        self._running = True
        self._schedule_next()
        logger.info("LiveBarScheduler started")

    def stop(self):
        self._running = False

    def _schedule_next(self):
        if not self._running:
            return
        seconds_until_next = self._seconds_to_next_15m()
        logger.debug(f"Next bar refresh in {seconds_until_next:.0f}s")
        reactor.callLater(seconds_until_next, self._on_bar_close)

    def _on_bar_close(self):
        logger.info("15m bar closed — refreshing all symbols")
        d = self.pipeline.refresh_all()
        d.addErrback(lambda f: logger.error(f"Refresh failed: {f}"))
        d.addBoth(lambda _: self._schedule_next())

    @staticmethod
    def _seconds_to_next_15m() -> float:
        now = datetime.now(timezone.utc)
        minutes_past = now.minute % 15
        seconds_past = now.second
        total_seconds_past = minutes_past * 60 + seconds_past + now.microsecond / 1e6
        seconds_remaining = (15 * 60) - total_seconds_past
        # Add a small buffer so the bar is definitely closed
        return seconds_remaining + 5


# ─── PRICE NORMALIZER ─────────────────────────────────────────────────────────

class PriceNormalizer:
    """
    Converts raw cTrader price units to decimal prices.
    cTrader stores prices as integers (digits precision varies by symbol).
    """

    # Decimal places for each symbol (check with ProtoOASymbolRes.digits)
    DIGITS = {
        "EURUSD": 5,
        "GBPUSD": 5,
        "AUDUSD": 5,
        "USDJPY": 3,
        "XAUUSD": 2,
    }

    # Pip size for each symbol (used for SL/TP calculations)
    PIP_SIZE = {
        "EURUSD": 0.0001,
        "GBPUSD": 0.0001,
        "AUDUSD": 0.0001,
        "USDJPY": 0.01,
        "XAUUSD": 0.10,   # 1 pip = $0.10 for gold (10 cents per 0.01)
    }

    @classmethod
    def to_decimal(cls, symbol: str, raw_price: int) -> float:
        digits = cls.DIGITS.get(symbol, 5)
        return raw_price / (10 ** digits)

    @classmethod
    def normalize_bars(cls, symbol: str, bars: list[dict]) -> list[dict]:
        """Convert all OHLC fields from raw int to float prices."""
        digits = cls.DIGITS.get(symbol, 5)
        divisor = 10 ** digits
        return [
            {
                **b,
                "open":  b["open"]  / divisor,
                "high":  b["high"]  / divisor,
                "low":   b["low"]   / divisor,
                "close": b["close"] / divisor,
            }
            for b in bars
        ]

    @classmethod
    def pips_to_price(cls, symbol: str, pips: float) -> float:
        return pips * cls.PIP_SIZE[symbol]

    @classmethod
    def price_to_pips(cls, symbol: str, price_diff: float) -> float:
        return price_diff / cls.PIP_SIZE[symbol]


# ─── UTILITIES ────────────────────────────────────────────────────────────────

def _ms_to_dt(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


@defer.inlineCallbacks
def _sleep(seconds: float):
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, None)
    yield d


# ─── STANDALONE TEST ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Quick test: verify bar fetching works for one symbol.
    Replace with your actual client setup from api_service.py.
    """
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    ACCOUNT_ID = int(os.getenv("CTID_TRADER_ACCOUNT_ID", "0"))

    store = BarStore(MONGO_URI)

    # In real usage, pass your existing connected client from api_service.py
    # pipeline = DataPipeline(client=your_client, account_id=ACCOUNT_ID, bar_store=store)
    # pipeline.initialize_all()

    print("BarStore initialized. Bar counts:")
    for sym in BASKET_SYMBOLS:
        print(f"  {sym}: {store.bar_count(sym)} bars")

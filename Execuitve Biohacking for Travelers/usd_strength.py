"""
usd_strength.py
---------------
USD Strength Index calculator and momentum signal engine.

Combines price action across all 5 basket pairs into a single composite
USD strength score, then detects momentum signals for trade entry.

Architecture:
    USDStrengthCalculator  → computes raw USD index from OHLCV bars
    MomentumDetector       → applies filters to generate BUY/SELL/FLAT signals
    SignalValidator        → validates confluence across all 5 pairs
"""

import logging
import numpy as np
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ─── ENUMS & DATACLASSES ─────────────────────────────────────────────────────

class Signal(Enum):
    STRONG_BUY  = 2   # USD very strong → sell EUR/GBP/AUD/XAU, buy JPY
    BUY         = 1   # USD strong (moderate)
    FLAT        = 0   # No clear USD bias
    SELL        = -1  # USD weak (moderate)
    STRONG_SELL = -2  # USD very weak


@dataclass
class PairAnalysis:
    """Analysis result for a single pair."""
    symbol: str
    current_price: float
    usd_contribution: float    # Normalized contribution to USD index (-1 to +1)
    ema_fast: float
    ema_slow: float
    rsi: float
    atr: float
    momentum_score: float      # -1 (USD weak) to +1 (USD strong)
    is_trending: bool
    trend_strength: float      # ADX proxy, 0-100


@dataclass
class USDBasketSignal:
    """Composite signal across the full basket."""
    timestamp: datetime
    signal: Signal
    strength: float            # 0.0 to 1.0 — confidence/strength of signal
    usd_index: float           # Composite USD index value (normalized)
    usd_momentum: float        # Rate of change of USD index
    pair_analyses: dict[str, PairAnalysis] = field(default_factory=dict)

    # Individual entry recommendations per pair
    entry_directions: dict[str, str] = field(default_factory=dict)  # "BUY" | "SELL" | "SKIP"

    confluence_score: float = 0.0    # How many pairs agree (0-1)
    is_tradeable: bool = False        # Passes all filters

    def describe(self) -> str:
        lines = [
            f"{'='*50}",
            f"USD Signal: {self.signal.name} | Strength: {self.strength:.2f} | Index: {self.usd_index:+.4f}",
            f"Momentum: {self.usd_momentum:+.4f} | Confluence: {self.confluence_score:.1%} | Tradeable: {self.is_tradeable}",
            f"Time: {self.timestamp.strftime('%Y-%m-%d %H:%M UTC')}",
            "Pair breakdown:",
        ]
        for sym, analysis in self.pair_analyses.items():
            direction = self.entry_directions.get(sym, "SKIP")
            lines.append(
                f"  {sym:8s} USD contrib: {analysis.usd_contribution:+.4f}  "
                f"RSI: {analysis.rsi:.1f}  ATR: {analysis.atr:.5f}  "
                f"Signal: {direction}"
            )
        return "\n".join(lines)


# ─── CONFIGURATION ───────────────────────────────────────────────────────────

# Pair weightings in the USD basket (sum = 1.0)
# EURUSD is the most liquid and gets highest weight
PAIR_WEIGHTS = {
    "EURUSD": 0.30,
    "GBPUSD": 0.25,
    "USDJPY": 0.20,
    "AUDUSD": 0.15,
    "XAUUSD": 0.10,
}

# Direction each pair's price moves when USD strengthens
# EURUSD goes DOWN when USD strong (negative contribution)
USD_DIRECTION = {
    "EURUSD": -1,
    "GBPUSD": -1,
    "AUDUSD": -1,
    "USDJPY":  1,   # USDJPY goes UP when USD strong
    "XAUUSD": -1,   # Gold goes DOWN when USD strong
}

# EMA periods (all on 15m bars)
EMA_FAST_PERIOD = 8
EMA_SLOW_PERIOD = 21
EMA_TREND_PERIOD = 50    # Trend filter

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ATR period
ATR_PERIOD = 14

# Momentum (Rate of Change) period
MOMENTUM_PERIOD = 3      # 3 bars back = 45 minutes

# Signal thresholds
SIGNAL_THRESHOLD_WEAK   = 0.15   # USD index must exceed this for BUY/SELL
SIGNAL_THRESHOLD_STRONG = 0.30   # Must exceed this for STRONG_BUY/SELL
MOMENTUM_THRESHOLD      = 0.05   # Minimum rate of change for momentum entry
MIN_CONFLUENCE          = 0.60   # At least 60% of pairs must agree (3/5)
MIN_TREND_STRENGTH      = 20.0   # ADX proxy minimum


# ─── INDICATORS ──────────────────────────────────────────────────────────────

class Indicators:
    """
    Vectorized indicator calculations using numpy.
    All functions accept and return numpy arrays.
    Input: close prices array, oldest first.
    """

    @staticmethod
    def ema(prices: np.ndarray, period: int) -> np.ndarray:
        """Exponential Moving Average."""
        if len(prices) < period:
            return np.full(len(prices), np.nan)
        alpha = 2.0 / (period + 1)
        result = np.full(len(prices), np.nan)
        # Seed with SMA
        result[period - 1] = np.mean(prices[:period])
        for i in range(period, len(prices)):
            result[i] = prices[i] * alpha + result[i - 1] * (1 - alpha)
        return result

    @staticmethod
    def rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Wilder's RSI."""
        if len(closes) < period + 1:
            return np.full(len(closes), 50.0)
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        result = np.full(len(closes), np.nan)
        # First avg
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
            result[i + 1] = 100 - (100 / (1 + rs))

        return result

    @staticmethod
    def atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Average True Range."""
        if len(closes) < 2:
            return np.zeros(len(closes))
        tr = np.maximum(highs[1:] - lows[1:],
             np.maximum(np.abs(highs[1:] - closes[:-1]),
                        np.abs(lows[1:] - closes[:-1])))
        result = np.full(len(closes), np.nan)
        result[period] = np.mean(tr[:period])
        for i in range(period, len(tr)):
            result[i + 1] = (result[i] * (period - 1) + tr[i]) / period
        return result

    @staticmethod
    def adx_proxy(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
        """
        Simplified ADX proxy using directional movement.
        Returns 0-100 trend strength value.
        """
        if len(closes) < period * 2:
            return 0.0
        # Directional movement
        up_moves = np.diff(highs)
        dn_moves = -np.diff(lows)
        # True range: max of (H-L, |H-prevC|, |L-prevC|)
        h = highs[1:]; l = lows[1:]; pc = closes[:-1]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))

        plus_dm = np.where((up_moves > dn_moves) & (up_moves > 0), up_moves, 0)
        minus_dm = np.where((dn_moves > up_moves) & (dn_moves > 0), dn_moves, 0)

        if len(tr) < period or np.sum(tr[-period:]) == 0:
            return 0.0

        atr_sum = np.sum(tr[-period:])
        plus_di = 100 * np.sum(plus_dm[-period:]) / atr_sum
        minus_di = 100 * np.sum(minus_dm[-period:]) / atr_sum
        di_sum = plus_di + minus_di
        if di_sum == 0:
            return 0.0
        dx = 100 * abs(plus_di - minus_di) / di_sum
        return float(dx)

    @staticmethod
    def rate_of_change(values: np.ndarray, period: int = 3) -> float:
        """Percentage rate of change over N periods."""
        if len(values) <= period or values[-1 - period] == 0:
            return 0.0
        return (values[-1] - values[-1 - period]) / abs(values[-1 - period])


# ─── PAIR ANALYZER ───────────────────────────────────────────────────────────

class PairAnalyzer:
    """
    Analyzes a single pair's bars and returns its contribution to USD strength.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.direction = USD_DIRECTION[symbol]   # +1 or -1
        self.weight = PAIR_WEIGHTS[symbol]

    def analyze(self, bars: list[dict]) -> Optional[PairAnalysis]:
        """
        bars: list of OHLCV dicts with decimal prices, oldest first.
        Returns PairAnalysis or None if insufficient data.
        """
        min_bars = max(EMA_TREND_PERIOD, RSI_PERIOD, ATR_PERIOD) + MOMENTUM_PERIOD + 5
        if len(bars) < min_bars:
            logger.warning(f"{self.symbol}: Need {min_bars} bars, have {len(bars)}")
            return None

        closes = np.array([b["close"] for b in bars])
        highs  = np.array([b["high"]  for b in bars])
        lows   = np.array([b["low"]   for b in bars])

        # Compute indicators
        ema_fast_arr  = Indicators.ema(closes, EMA_FAST_PERIOD)
        ema_slow_arr  = Indicators.ema(closes, EMA_SLOW_PERIOD)
        ema_trend_arr = Indicators.ema(closes, EMA_TREND_PERIOD)
        rsi_arr       = Indicators.rsi(closes, RSI_PERIOD)
        atr_arr       = Indicators.atr(highs, lows, closes, ATR_PERIOD)
        trend_str     = Indicators.adx_proxy(highs, lows, closes, ATR_PERIOD)

        ema_fast  = float(ema_fast_arr[-1])
        ema_slow  = float(ema_slow_arr[-1])
        ema_trend = float(ema_trend_arr[-1])
        rsi       = float(rsi_arr[-1]) if not np.isnan(rsi_arr[-1]) else 50.0
        atr       = float(atr_arr[-1]) if not np.isnan(atr_arr[-1]) else 0.0
        close     = closes[-1]

        # ── USD Contribution ──────────────────────────────────────────────
        # Positive contribution = USD strengthening for this pair
        # Based on EMA cross normalized by ATR for comparability

        if atr > 0:
            ema_cross_raw = (ema_fast - ema_slow) / atr
        else:
            ema_cross_raw = 0.0

        # Apply direction: EURUSD falling = USD strong → positive contribution
        usd_contribution_raw = self.direction * ema_cross_raw

        # Normalize to roughly -1..+1 range using tanh squashing
        usd_contribution = float(np.tanh(usd_contribution_raw * 2))

        # ── Momentum Score ────────────────────────────────────────────────
        # How strongly is price moving in the expected USD direction?

        # Price momentum: is close above/below trend EMA?
        price_vs_trend = (close - ema_trend) / atr if atr > 0 else 0.0
        price_momentum = self.direction * float(np.tanh(price_vs_trend))

        # RSI momentum: overbought/oversold in context of direction
        rsi_normalized = (rsi - 50) / 50   # -1 to +1
        rsi_contribution = self.direction * rsi_normalized

        # Rate of change of close prices
        roc = Indicators.rate_of_change(closes, MOMENTUM_PERIOD)
        roc_contribution = self.direction * float(np.tanh(roc * 100))

        # Composite momentum score (weighted average)
        momentum_score = (
            0.40 * usd_contribution +
            0.30 * price_momentum +
            0.20 * roc_contribution +
            0.10 * rsi_contribution
        )

        is_trending = trend_str >= MIN_TREND_STRENGTH

        return PairAnalysis(
            symbol=self.symbol,
            current_price=float(close),
            usd_contribution=usd_contribution,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            rsi=rsi,
            atr=atr,
            momentum_score=momentum_score,
            is_trending=is_trending,
            trend_strength=trend_str,
        )


# ─── USD STRENGTH CALCULATOR ─────────────────────────────────────────────────

class USDStrengthCalculator:
    """
    Aggregates PairAnalysis results into a composite USD Index.

    USD Index ranges:
        +0.30 to +1.00  → Strong USD (consider long USD positions)
        -0.30 to -0.99  → Weak USD (consider short USD positions)
        -0.30 to +0.30  → Noise / no trade
    """

    def __init__(self):
        self.analyzers = {sym: PairAnalyzer(sym) for sym in PAIR_WEIGHTS}
        self._index_history: list[float] = []   # Rolling history for momentum calc

    def calculate(self, all_bars: dict[str, list[dict]]) -> Optional[USDBasketSignal]:
        """
        all_bars: {symbol: [bar_dict]} — normalized decimal prices, oldest first.
        Returns USDBasketSignal or None if insufficient data.
        """
        pair_analyses = {}
        weighted_index = 0.0
        weight_used = 0.0

        for symbol, analyzer in self.analyzers.items():
            bars = all_bars.get(symbol, [])
            analysis = analyzer.analyze(bars)
            if analysis is None:
                logger.warning(f"Skipping {symbol} — insufficient data")
                continue
            pair_analyses[symbol] = analysis
            weighted_index += analysis.usd_contribution * PAIR_WEIGHTS[symbol]
            weight_used += PAIR_WEIGHTS[symbol]

        if weight_used < 0.70:   # Need at least 70% of basket weighted
            logger.error(f"Insufficient basket coverage: {weight_used:.0%}")
            return None

        # Re-normalize if some pairs were skipped
        usd_index = weighted_index / weight_used if weight_used > 0 else 0.0

        # USD Momentum: rate of change of the index itself
        self._index_history.append(usd_index)
        if len(self._index_history) > 100:
            self._index_history.pop(0)

        usd_momentum = 0.0
        if len(self._index_history) >= MOMENTUM_PERIOD + 1:
            idx_arr = np.array(self._index_history)
            usd_momentum = Indicators.rate_of_change(idx_arr, MOMENTUM_PERIOD)

        signal, strength = self._classify_signal(usd_index, usd_momentum, pair_analyses)
        confluence = self._calculate_confluence(usd_index, pair_analyses)
        entry_directions = self._get_entry_directions(signal, pair_analyses)
        is_tradeable = self._validate_trade(signal, strength, confluence, pair_analyses)

        result = USDBasketSignal(
            timestamp=datetime.now(timezone.utc),
            signal=signal,
            strength=strength,
            usd_index=usd_index,
            usd_momentum=usd_momentum,
            pair_analyses=pair_analyses,
            entry_directions=entry_directions,
            confluence_score=confluence,
            is_tradeable=is_tradeable,
        )

        logger.info(f"USD Signal: {signal.name} | Index: {usd_index:+.4f} | "
                    f"Momentum: {usd_momentum:+.4f} | Confluence: {confluence:.0%} | "
                    f"Tradeable: {is_tradeable}")
        return result

    # ── Signal Classification ──────────────────────────────────────────────

    def _classify_signal(
        self,
        usd_index: float,
        usd_momentum: float,
        pair_analyses: dict[str, PairAnalysis],
    ) -> tuple[Signal, float]:
        """
        Converts USD index + momentum into a discrete signal + strength score.
        """
        # Base signal from index level
        if usd_index >= SIGNAL_THRESHOLD_STRONG:
            base_signal = Signal.STRONG_BUY
        elif usd_index >= SIGNAL_THRESHOLD_WEAK:
            base_signal = Signal.BUY
        elif usd_index <= -SIGNAL_THRESHOLD_STRONG:
            base_signal = Signal.STRONG_SELL
        elif usd_index <= -SIGNAL_THRESHOLD_WEAK:
            base_signal = Signal.SELL
        else:
            base_signal = Signal.FLAT

        # Momentum filter: downgrade signal if momentum disagrees
        if base_signal in (Signal.BUY, Signal.STRONG_BUY):
            if usd_momentum < -MOMENTUM_THRESHOLD:
                # USD rising but momentum fading — downgrade
                base_signal = Signal.BUY if base_signal == Signal.STRONG_BUY else Signal.FLAT
            elif usd_momentum > MOMENTUM_THRESHOLD:
                # Momentum confirms — upgrade
                if base_signal == Signal.BUY:
                    base_signal = Signal.STRONG_BUY
        elif base_signal in (Signal.SELL, Signal.STRONG_SELL):
            if usd_momentum > MOMENTUM_THRESHOLD:
                base_signal = Signal.SELL if base_signal == Signal.STRONG_SELL else Signal.FLAT
            elif usd_momentum < -MOMENTUM_THRESHOLD:
                if base_signal == Signal.SELL:
                    base_signal = Signal.STRONG_SELL

        # Strength: 0.0 to 1.0 based on how far index is from threshold
        abs_index = abs(usd_index)
        if abs_index < SIGNAL_THRESHOLD_WEAK:
            strength = 0.0
        else:
            strength = min(1.0, (abs_index - SIGNAL_THRESHOLD_WEAK) /
                              (SIGNAL_THRESHOLD_STRONG - SIGNAL_THRESHOLD_WEAK + 0.2))

        # Boost strength with momentum
        if abs(usd_momentum) > MOMENTUM_THRESHOLD:
            strength = min(1.0, strength * 1.2)

        return base_signal, strength

    def _calculate_confluence(
        self, usd_index: float, pair_analyses: dict[str, PairAnalysis]
    ) -> float:
        """
        Confluence = fraction of pairs whose momentum_score agrees with the signal.
        """
        if not pair_analyses:
            return 0.0
        expected_sign = 1 if usd_index > 0 else -1
        agreeing = sum(
            1 for a in pair_analyses.values()
            if (a.momentum_score * expected_sign) > 0.05   # slight buffer
        )
        return agreeing / len(pair_analyses)

    def _get_entry_directions(
        self, signal: Signal, pair_analyses: dict[str, PairAnalysis]
    ) -> dict[str, str]:
        """
        Maps signal to per-pair entry direction.
        USD strong: SELL EUR/GBP/AUD/XAU, BUY JPY pair (USDJPY)
        USD weak:   BUY  EUR/GBP/AUD/XAU, SELL JPY pair (USDJPY)
        """
        if signal == Signal.FLAT:
            return {sym: "SKIP" for sym in pair_analyses}

        directions = {}
        usd_bullish = signal.value > 0  # BUY or STRONG_BUY

        for sym, analysis in pair_analyses.items():
            pair_dir = USD_DIRECTION[sym]
            if usd_bullish:
                # USD strengthening
                # For pairs where USD is base (USDJPY): BUY
                # For pairs where USD is quote (EURUSD etc.): SELL
                directions[sym] = "BUY" if pair_dir == 1 else "SELL"
            else:
                # USD weakening (reverse)
                directions[sym] = "SELL" if pair_dir == 1 else "BUY"

            # SKIP if pair's own momentum opposes the basket signal
            if abs(analysis.momentum_score) < 0.05:
                directions[sym] = "SKIP"  # Pair is ranging — skip it

        return directions

    # ── Trade Validation ──────────────────────────────────────────────────

    def _validate_trade(
        self,
        signal: Signal,
        strength: float,
        confluence: float,
        pair_analyses: dict[str, PairAnalysis],
    ) -> bool:
        """
        All conditions must pass for the signal to be marked tradeable.
        """
        if signal == Signal.FLAT:
            return False

        checks = {
            "min_strength":    strength >= 0.40,
            "min_confluence":  confluence >= MIN_CONFLUENCE,
            "has_momentum":    any(abs(a.momentum_score) > 0.1 for a in pair_analyses.values()),
            "not_all_ranging": sum(a.is_trending for a in pair_analyses.values()) >= 2,
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            logger.debug(f"Trade validation failed: {failed}")
            return False
        return True


# ─── MOMENTUM DETECTOR ───────────────────────────────────────────────────────

class MomentumDetector:
    """
    Higher-level signal detector that wraps USDStrengthCalculator.
    Adds:
      - Session filter (avoid low-liquidity Asian session for these pairs)
      - Spike filter (ignore sudden ATR spikes = news events)
      - Cooldown period (prevent re-entry within N bars of last signal)
    """

    # Trading sessions (UTC hours) — best liquidity for these pairs
    PREFERRED_SESSIONS = [
        (7, 16),   # London session + London/NY overlap
        (12, 20),  # Full NY session
    ]

    COOLDOWN_BARS = 4   # Don't re-signal within 4 bars (1 hour) of last trade

    def __init__(self):
        self.calculator = USDStrengthCalculator()
        self._last_signal_bar: int = -999
        self._bar_count: int = 0

    def process(
        self,
        all_bars: dict[str, list[dict]],
        enforce_session_filter: bool = True,
    ) -> Optional[USDBasketSignal]:
        """
        Main entry point. Call with fresh bars on each 15m close.
        Returns signal if conditions met, else None.
        """
        self._bar_count += 1

        # Session filter
        if enforce_session_filter and not self._in_preferred_session():
            logger.debug("Outside preferred session — no signal")
            return None

        # Spike filter (news/volatility guard)
        if self._has_volatility_spike(all_bars):
            logger.warning("ATR spike detected — skipping signal (possible news event)")
            return None

        # Cooldown
        if self._bar_count - self._last_signal_bar < self.COOLDOWN_BARS:
            logger.debug(f"In cooldown ({self._bar_count - self._last_signal_bar}/{self.COOLDOWN_BARS} bars)")
            return None

        signal = self.calculator.calculate(all_bars)
        if signal and signal.is_tradeable:
            self._last_signal_bar = self._bar_count
            logger.info(f"✅ TRADEABLE SIGNAL: {signal.signal.name}")

        return signal

    def _in_preferred_session(self) -> bool:
        hour = datetime.now(timezone.utc).hour
        return any(start <= hour < end for start, end in self.PREFERRED_SESSIONS)

    def _has_volatility_spike(self, all_bars: dict[str, list[dict]]) -> bool:
        """
        Returns True if any pair's last bar ATR is >2.5x its 14-bar average.
        Indicates a news spike — avoid trading into it.
        """
        for symbol, bars in all_bars.items():
            if len(bars) < 16:
                continue
            highs  = np.array([b["high"]  for b in bars[-16:]])
            lows   = np.array([b["low"]   for b in bars[-16:]])
            closes = np.array([b["close"] for b in bars[-16:]])

            atr_arr = Indicators.atr(highs, lows, closes, 14)
            recent = atr_arr[-3:]   # Last 3 bars
            historical = atr_arr[-15:-3]

            valid_recent = recent[~np.isnan(recent)]
            valid_hist   = historical[~np.isnan(historical)]

            if len(valid_recent) > 0 and len(valid_hist) > 0:
                avg_atr = np.mean(valid_hist)
                last_atr = valid_recent[-1]
                if avg_atr > 0 and last_atr > avg_atr * 2.5:
                    logger.warning(f"{symbol}: ATR spike {last_atr:.5f} vs avg {avg_atr:.5f}")
                    return True
        return False

    def reset_cooldown(self):
        """Call after a position is closed to allow new signals sooner."""
        self._last_signal_bar = -999


# ─── SIGNAL HISTORY TRACKER ──────────────────────────────────────────────────

class SignalHistory:
    """
    Lightweight in-memory tracker for recent signals.
    Useful for debugging and performance analysis during backtesting.
    """

    def __init__(self, max_history: int = 500):
        self._history: list[USDBasketSignal] = []
        self._max = max_history

    def record(self, signal: USDBasketSignal):
        self._history.append(signal)
        if len(self._history) > self._max:
            self._history.pop(0)

    def get_recent(self, n: int = 10) -> list[USDBasketSignal]:
        return self._history[-n:]

    def get_tradeable(self) -> list[USDBasketSignal]:
        return [s for s in self._history if s.is_tradeable]

    def signal_summary(self) -> dict:
        if not self._history:
            return {}
        tradeable = self.get_tradeable()
        signal_counts = {}
        for s in tradeable:
            signal_counts[s.signal.name] = signal_counts.get(s.signal.name, 0) + 1
        return {
            "total_signals": len(self._history),
            "tradeable": len(tradeable),
            "tradeable_rate": len(tradeable) / len(self._history),
            "signal_breakdown": signal_counts,
        }


# ─── CONVENIENCE FACTORY ─────────────────────────────────────────────────────

def create_signal_engine(enforce_session: bool = True) -> tuple[MomentumDetector, SignalHistory]:
    """
    Factory function — returns a ready-to-use signal engine + history tracker.

    Usage:
        detector, history = create_signal_engine()

        # On each 15m bar close:
        signal = detector.process(pipeline.get_all_latest(n=200))
        if signal:
            history.record(signal)
            if signal.is_tradeable:
                print(signal.describe())
                # → pass to order manager
    """
    detector = MomentumDetector()
    detector._enforce_session = enforce_session
    history = SignalHistory()
    return detector, history


# ─── STANDALONE TEST ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Smoke test with synthetic bar data to verify calculations.
    """
    import random
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def make_bars(n: int, base: float, direction: float = 0.001) -> list[dict]:
        """Generate synthetic trending bars."""
        bars = []
        price = base
        ts = 1_700_000_000_000  # arbitrary start
        for i in range(n):
            noise = random.gauss(0, base * 0.001)
            trend = direction * (i / n)
            close = max(price + trend + noise, 0.0001)
            high  = close + abs(random.gauss(0, base * 0.0005))
            low   = close - abs(random.gauss(0, base * 0.0005))
            bars.append({
                "timestamp": ts + i * 15 * 60 * 1000,
                "open":  price,
                "high":  high,
                "low":   low,
                "close": close,
                "volume": random.randint(100, 1000),
            })
            price = close
        return bars

    # Simulate USD strengthening:
    # EURUSD falls, GBPUSD falls, AUDUSD falls, USDJPY rises, XAUUSD falls
    test_bars = {
        "EURUSD": make_bars(200, base=1.0900, direction=-0.0020),   # falling
        "GBPUSD": make_bars(200, base=1.2700, direction=-0.0025),   # falling
        "AUDUSD": make_bars(200, base=0.6500, direction=-0.0015),   # falling
        "USDJPY": make_bars(200, base=148.00, direction=+0.15),     # rising
        "XAUUSD": make_bars(200, base=2000.0, direction=-2.00),     # falling
    }

    detector, history = create_signal_engine(enforce_session=False)

    signal = detector.process(test_bars)
    if signal:
        history.record(signal)
        print(signal.describe())
        print("\nSummary:", history.signal_summary())
    else:
        print("No signal generated (check thresholds or bar count)")

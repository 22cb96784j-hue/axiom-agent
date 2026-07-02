# ta_engine.py — Quantitative Technical Analysis for sub-hour memecoin setups
# Custom parameters tuned for Solana micro-caps: RSI period=7, MACD 5/13/3

import numpy as np
from dataclasses import dataclass
from data_ingestion import TokenSnapshot


@dataclass
class TAResult:
    # Volume / liquidity
    vl_ratio_5m: float          # Volume/Liquidity ratio (5 min)
    vl_ratio_15m: float
    volume_acceleration: float  # % change 5m→15m
    # Order flow
    order_flow_imbalance: float # -1.0 (seller) → +1.0 (buyer)
    # Momentum oscillators
    rsi_15m: float
    macd_signal: str            # BULL_CROSS | BEAR_CROSS | NEUTRAL
    macd_histogram: float
    # Composite
    momentum_score: float       # 0–100
    setup_type: str             # BREAKOUT | CONSOLIDATION | BLOWOFF_TOP | WEAK
    confidence: float           # 0–1, data completeness score
    # Price levels
    entry_low: float
    entry_high: float
    stop_loss: float
    take_profit_1: float        # Scalp  (~+8%)
    take_profit_2: float        # Swing  (~+20%)
    take_profit_3: float        # Moon   (~+40%)


class TAEngine:

    # ── RSI ───────────────────────────────────────────────────
    @staticmethod
    def rsi(prices: list[float], period: int = 7) -> float:
        """
        period=7 is intentionally short — memecoins move in minutes, not days.
        Standard period=14 is too slow for these timeframes.
        """
        if len(prices) < period + 1:
            return 50.0
        deltas = np.diff(prices)
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_g  = np.mean(gains[-period:])
        avg_l  = np.mean(losses[-period:])
        if avg_l == 0:
            return 100.0
        return round(100.0 - (100.0 / (1.0 + avg_g / avg_l)), 2)

    # ── MACD ──────────────────────────────────────────────────
    @staticmethod
    def macd(prices: list[float]) -> tuple[float, float, str]:
        """
        Fast windows (5/13/3) catch micro-momentum before 12/26/9 even registers.
        Critical for tokens that can 10x within an hour.
        """
        def ema(data: list[float], span: int) -> float:
            w = np.exp(np.linspace(-1.0, 0.0, span))
            w /= w.sum()
            return float(np.convolve(data, w, mode="valid")[-1])

        if len(prices) < 14:
            return 0.0, 0.0, "NEUTRAL"

        line   = ema(prices, 5) - ema(prices, 13)
        signal = line * 0.5   # simplified 3-period EMA; store rolling history in prod
        hist   = line - signal

        if hist > 0 and hist > abs(line) * 0.1:
            cross = "BULL_CROSS"
        elif hist < 0 and abs(hist) > abs(line) * 0.1:
            cross = "BEAR_CROSS"
        else:
            cross = "NEUTRAL"

        return round(line, 6), round(hist, 6), cross

    # ── Order Flow Imbalance ──────────────────────────────────
    @staticmethod
    def ofi(buys: int, sells: int) -> float:
        """
        OFI = (B - S) / (B + S)
        +1.0 = pure buyer pressure  →  strong momentum signal
        -1.0 = pure seller pressure →  distribution / avoid
        """
        total = buys + sells
        return 0.0 if total == 0 else round((buys - sells) / total, 3)

    # ── Volume / Liquidity Ratio ──────────────────────────────
    @staticmethod
    def vl(volume: float, liquidity: float) -> float:
        """
        V/L > 4.0  → extreme activity vs pool depth (risk of blow-off or rug)
        V/L 1–3.0  → healthy accumulation
        V/L < 0.3  → dead / low interest
        """
        return 0.0 if liquidity <= 0 else round(volume / liquidity, 3)

    # ── Price Levels ──────────────────────────────────────────
    @staticmethod
    def compute_levels(price: float, atr_pct: float = 0.08) -> dict:
        """
        ATR approximated as % of price (default 8% — typical for micro-cap volatility).
        Entry: ±2% from current price
        Stop:  -1.5× ATR  (keeps R:R positive)
        TP1:   +1× ATR  (quick scalp — take 50% here)
        TP2:   +2.5× ATR (hold remainder)
        TP3:   +5× ATR   (moon bag — 10–20% of position)
        """
        atr = price * atr_pct
        return {
            "entry_low":     round(price * 0.98, 10),
            "entry_high":    round(price * 1.02, 10),
            "stop_loss":     round(price - 1.5 * atr, 10),
            "take_profit_1": round(price + 1.0 * atr, 10),
            "take_profit_2": round(price + 2.5 * atr, 10),
            "take_profit_3": round(price + 5.0 * atr, 10),
        }

    # ── Setup Classifier ──────────────────────────────────────
    @staticmethod
    def classify(rsi: float, vl5m: float, ofi_val: float, macd_cross: str) -> str:
        """
        Rules are intentionally strict to minimise noise.
        Only BREAKOUT is acted upon; all others are filtered by the orchestrator.
        """
        if rsi > 80 and vl5m > 4.0:
            return "BLOWOFF_TOP"   # Never chase — high rug probability
        if rsi < 45 and ofi_val > 0.3 and macd_cross == "BULL_CROSS":
            return "BREAKOUT"      # Ideal entry — oversold with buyer pressure incoming
        if 45 <= rsi <= 65 and ofi_val > 0.1:
            return "CONSOLIDATION" # Possible entry if catalyst arrives
        return "WEAK"

    # ── Momentum Score ────────────────────────────────────────
    @staticmethod
    def momentum_score(
        vl5m: float, vl15m: float, ofi_val: float, rsi: float,
        sentiment: float, smart_buys: float, holder_count: int,
    ) -> float:
        """
        Composite 0–100 score weighting:
          Volume pressure        35 pts
          Order flow             20 pts
          RSI sweet spot         15 pts
          Twitter sentiment      15 pts
          Smart wallet buys      10 pts
          Holder diversity        5 pts
        """
        score = 0.0
        score += min(vl5m / 5.0, 1.0) * 20      # V/L 5m
        score += min(vl15m / 3.0, 1.0) * 15     # V/L 15m
        score += max(ofi_val, 0.0) * 20          # OFI (buyer pressure)
        score += max(0, 15 - abs(rsi - 55) / 55 * 15)  # RSI 40–70 sweet spot
        score += max(sentiment, 0.0) * 15        # Twitter sentiment
        score += min(smart_buys / 10_000, 1.0) * 10    # Smart wallet accumulation
        score += min(holder_count / 500, 1.0) * 5      # Holder diversity
        return round(min(score, 100.0), 1)

    # ── Confidence Score ──────────────────────────────────────
    @staticmethod
    def confidence(snap: TokenSnapshot, prices: list[float]) -> float:
        """
        How complete and trustworthy is our data?
        Low confidence → reduce position size or skip entirely.
        """
        c = 0.0
        if snap.holder_count > 0:          c += 0.20
        if len(prices) >= 14:              c += 0.20
        if snap.twitter_mentions_10m > 5:  c += 0.20
        if snap.smart_wallet_buys_1h > 0:  c += 0.20
        if not snap.is_honeypot:           c += 0.20   # GoPlus verified clean
        return round(c, 2)

    # ── Main Analyze ──────────────────────────────────────────
    def analyze(self, snap: TokenSnapshot, price_history: list[float]) -> TAResult:
        vl5m   = self.vl(snap.volume_5m, snap.liquidity_usd)
        vl15m  = self.vl(snap.volume_15m, snap.liquidity_usd)
        vol_acc = round(((snap.volume_15m / max(snap.volume_5m, 1)) - 1) * 100, 1)
        ofi_val = self.ofi(snap.buy_txns_15m, snap.sell_txns_15m)
        rsi_val = self.rsi(price_history)
        _, macd_h, macd_sig = self.macd(price_history)
        setup   = self.classify(rsi_val, vl5m, ofi_val, macd_sig)
        score   = self.momentum_score(
            vl5m, vl15m, ofi_val, rsi_val,
            snap.twitter_sentiment_score,
            snap.smart_wallet_buys_1h,
            snap.holder_count,
        )
        conf    = self.confidence(snap, price_history)
        levels  = self.compute_levels(snap.price_usd)

        return TAResult(
            vl_ratio_5m=vl5m,
            vl_ratio_15m=vl15m,
            volume_acceleration=vol_acc,
            order_flow_imbalance=ofi_val,
            rsi_15m=rsi_val,
            macd_signal=macd_sig,
            macd_histogram=macd_h,
            momentum_score=score,
            setup_type=setup,
            confidence=conf,
            **levels,
        )

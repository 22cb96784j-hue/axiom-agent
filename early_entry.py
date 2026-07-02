# early_entry.py — Pre-Pump Pattern Detector
# Detects tokens showing buy pressure BEFORE price moves.
# Key signals: volume spike, wallet accumulation, low market cap, thin sell side.
# Goal: get in 2-5 minutes before the pump, not after.

import time
from dataclasses import dataclass, field
from typing import Optional


# ── Pre-pump signal result ────────────────────────────────────
@dataclass
class EarlyEntrySignal:
    ca: str
    symbol: str
    score: int                    # 0-100, higher = stronger early signal
    triggers: list[str]           # human-readable reasons
    confidence: str               # LOW / MEDIUM / HIGH
    urgency: str                  # WATCH / ALERT / URGENT
    recommended_action: str
    timestamp: float = field(default_factory=time.time)

    @property
    def is_actionable(self) -> bool:
        return self.score >= 45


# ── Early Entry Detector ──────────────────────────────────────
class EarlyEntryDetector:
    """
    Analyses a raw DexScreener token dict for pre-pump signals.
    Runs BEFORE the standard risk/TA pipeline so we catch tokens early.

    Scoring (additive, max 100):
      +25  Volume acceleration: 5m vol > 3x the expected rate from 1h vol
      +20  Low market cap (<$500k) with positive volume = room to run
      +15  Buy/sell ratio > 2.0 in last 5min (strong buy pressure)
      +15  Price up <10% but volume spiking (early, not blown off yet)
      +10  Liquidity thin relative to volume (fast price impact)
      +10  New token (<6h old based on pair created timestamp)
      +5   Volume 5m > $5k (real money moving, not dust)
    """

    # Thresholds
    VOL_ACCEL_RATIO   = 3.0     # 5m vol must be 3x the per-5min 1h average
    MAX_MCAP_EARLY    = 500_000 # Under $500k = early stage
    BUY_SELL_RATIO    = 2.0     # Buys must be 2x sells
    PRICE_PUMP_CAP    = 0.15    # Already pumped >15%? Too late
    MIN_VOL_5M        = 5_000   # Minimum $5k volume in 5min to be real
    MAX_NEW_TOKEN_H   = 6       # Token < 6 hours old gets bonus
    MAX_TOKEN_AGE_H   = 24      # Hard cutoff — never alert on tokens older than 24h

    def analyse(self, raw: dict) -> Optional[EarlyEntrySignal]:
        """
        raw: dict from DexScreener DiscoverPoller.fetch_top_movers()
        Returns EarlyEntrySignal or None if clearly not early.
        """
        ca     = raw.get("contract_address", "")
        symbol = raw.get("symbol", "UNKNOWN")
        if not ca:
            return None

        # Hard age filter — skip tokens older than 24 hours
        created = raw.get("pair_created_at", 0) or 0
        if created:
            age_hours = (time.time() - created / 1000) / 3600
            if age_hours > self.MAX_TOKEN_AGE_H:
                return None  # Too old — not an early entry

        score    = 0
        triggers = []

        vol_5m  = float(raw.get("volume_5m",  0) or 0)
        vol_1h  = float(raw.get("volume_1h",  0) or 0)
        liq     = float(raw.get("liquidity_usd", 0) or 0)
        mcap    = float(raw.get("market_cap", 0) or 0)
        price   = float(raw.get("price_usd",  0) or 0)
        buys    = int(raw.get("buy_txns_15m",  0) or 0)
        sells   = int(raw.get("sell_txns_15m", 0) or 0)
        # created already extracted above for age filter

        # ── Signal 1: Volume Acceleration ────────────────────
        expected_5m = vol_1h / 12 if vol_1h > 0 else 0   # expected vol per 5min
        if expected_5m > 0:
            accel = vol_5m / expected_5m
            if accel >= self.VOL_ACCEL_RATIO:
                pts = min(25, int((accel / self.VOL_ACCEL_RATIO) * 15))
                score += pts
                triggers.append(f"🚀 Volume {accel:.1f}x above average (+{pts}pts)")
        elif vol_5m >= self.MIN_VOL_5M:
            score += 10
            triggers.append(f"📊 Vol spike ${vol_5m:,.0f}/5m with no history (+10pts)")

        # ── Signal 2: Low Market Cap ──────────────────────────
        if 0 < mcap <= self.MAX_MCAP_EARLY:
            pts = 20 if mcap < 100_000 else 12 if mcap < 250_000 else 8
            score += pts
            triggers.append(f"💎 Micro-cap ${mcap:,.0f} MCap — huge upside (+{pts}pts)")

        # ── Signal 3: Buy/Sell Pressure ───────────────────────
        if sells > 0 and buys / sells >= self.BUY_SELL_RATIO:
            ratio = buys / sells
            pts   = min(15, int(ratio * 5))
            score += pts
            triggers.append(f"📈 Buy pressure {ratio:.1f}x buys vs sells (+{pts}pts)")
        elif sells == 0 and buys > 5:
            score += 15
            triggers.append(f"🔥 {buys} buys, ZERO sells (+15pts)")

        # ── Signal 4: Price hasn't pumped yet ─────────────────
        price_change = raw.get("price_change_5m", 0) or 0
        if isinstance(price_change, str):
            try:
                price_change = float(price_change)
            except ValueError:
                price_change = 0
        if -5 <= price_change <= self.PRICE_PUMP_CAP * 100:
            score += 15
            triggers.append(f"⏰ Price only {price_change:+.1f}% — not blown off (+15pts)")
        elif price_change > self.PRICE_PUMP_CAP * 100:
            # Already pumped a lot — penalise
            score -= 10
            triggers.append(f"⚠️ Already pumped {price_change:+.1f}% (-10pts)")

        # ── Signal 5: Thin liquidity = fast price impact ──────
        if liq > 0 and vol_5m > 0:
            vol_to_liq = vol_5m / liq
            if vol_to_liq > 0.1:   # Volume > 10% of liquidity in 5min
                pts = min(10, int(vol_to_liq * 20))
                score += pts
                triggers.append(f"⚡ Vol/Liq {vol_to_liq:.1%} — thin book, fast moves (+{pts}pts)")

        # ── Signal 6: New token bonus ─────────────────────────
        if created:
            age_hours = (time.time() - created / 1000) / 3600
            if 0 < age_hours <= self.MAX_NEW_TOKEN_H:
                score += 10
                triggers.append(f"🆕 Token only {age_hours:.1f}h old (+10pts)")

        # ── Signal 7: Minimum real volume ────────────────────
        if vol_5m >= self.MIN_VOL_5M:
            score += 5
            triggers.append(f"✅ Real volume ${vol_5m:,.0f} in 5min (+5pts)")

        # ── Cap score at 100 ─────────────────────────────────
        score = min(100, max(0, score))

        # ── Confidence / Urgency ──────────────────────────────
        if score >= 70:
            confidence = "HIGH"
            urgency    = "URGENT"
            action     = f"Enter NOW at market. Set stop -15%. Target +50-100%."
        elif score >= 55:
            confidence = "MEDIUM"
            urgency    = "ALERT"
            action     = f"Watch closely. Enter on next 1min candle close above current price."
        elif score >= 40:
            confidence = "LOW"
            urgency    = "WATCH"
            action     = f"Add to watchlist. Wait for volume confirmation."
        else:
            confidence = "NONE"
            urgency    = "SKIP"
            action     = "Does not meet early entry criteria."

        if not triggers:
            triggers = ["No strong early signals detected"]

        return EarlyEntrySignal(
            ca=ca,
            symbol=symbol,
            score=score,
            triggers=triggers,
            confidence=confidence,
            urgency=urgency,
            recommended_action=action,
        )

    def format_alert(self, sig: EarlyEntrySignal, raw: dict) -> str:
        """Format a Telegram-ready alert message for an early entry signal."""
        vol_5m = float(raw.get("volume_5m", 0) or 0)
        mcap   = float(raw.get("market_cap", 0) or 0)
        price  = float(raw.get("price_usd", 0) or 0)
        liq    = float(raw.get("liquidity_usd", 0) or 0)

        urgency_emoji = {"URGENT": "🔴", "ALERT": "🟡", "WATCH": "🟢"}.get(sig.urgency, "⚪")

        trigger_text = "\n".join(f"  • {t}" for t in sig.triggers)

        return (
            f"{urgency_emoji} *EARLY ENTRY SIGNAL* — `${sig.symbol}`\n"
            f"Score: *{sig.score}/100* | Confidence: *{sig.confidence}*\n\n"
            f"*Why now:*\n{trigger_text}\n\n"
            f"| Metric | Value |\n"
            f"|--------|-------|\n"
            f"| Price | ${price:.10f} |\n"
            f"| Market Cap | ${mcap:,.0f} |\n"
            f"| 5m Volume | ${vol_5m:,.0f} |\n"
            f"| Liquidity | ${liq:,.0f} |\n\n"
            f"*Action:* {sig.recommended_action}\n\n"
            f"`CA: {sig.ca}`\n"
            f"_⚠️ Not financial advice. DYOR._"
        )

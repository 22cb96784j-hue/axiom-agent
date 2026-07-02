# vector_memory.py — Trade memory backed by Upstash Redis
#
# Replaces ChromaDB + local /data files with Upstash Redis REST API.
# Memory now persists across ALL Railway deploys — no Volume needed.
#
# Setup (one-time):
#   1. Create free account at upstash.com
#   2. Create a Redis database (free tier)
#   3. Copy UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN
#   4. Add both as Railway environment variables
#
# Three layers of memory (same API as before):
#   1. trade_patterns  — confirmed trade outcomes (WIN/LOSS/RUG)
#   2. pending_alerts  — tokens alerted on, awaiting user outcome feedback
#   3. weights         — learned filter thresholds, updated after each trade

import json
import os
import time
import requests
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Redis keys
WEIGHTS_KEY  = "axiom:weights"
PENDING_KEY  = "axiom:pending"
TRADES_KEY   = "axiom:trades"
MAX_TRADES   = 200   # keep last 200 trades

DEFAULT_WEIGHTS: dict = {
    # Filter thresholds (auto-updated by self-learning)
    "momentum_score_threshold":  40,
    "ofi_threshold":             0.0,
    "min_smart_wallet_usd":      0,
    "min_twitter_sentiment":     -1.0,
    "max_vl_ratio_entry":        20.0,
    "min_confidence":            0.3,
    "min_holder_count":          10,
    # MCap sweet spot (learned from wins vs rugs)
    "min_mcap_alert":            0,
    "max_mcap_alert":            10_000_000,
    # Best entry hours UTC (learned from win timing)
    "preferred_hours_utc":       [],
    # Blocklist
    "blacklisted_influencers":   [],
    "blacklisted_wallets":       [],
    # Trusted providers
    "trusted_lp_lockers":        ["Streamflow", "PinkLock"],
    # Learned patterns (human-readable, for /learn command)
    "learned_patterns":          [],
    # Metadata
    "version":                   1,
    "last_updated":              "",
    "last_win_rate":             0.0,
}


# ── Upstash Redis REST client (sync) ─────────────────────────
class _Redis:
    """
    Thin wrapper around the Upstash REST API.
    Falls back to in-RAM storage if credentials aren't set,
    so the bot still works — just without cross-deploy persistence.
    """

    def __init__(self):
        self.url   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
        self.token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
        self.ok    = bool(self.url and self.token)
        if not self.ok:
            print(
                "[Memory] ⚠️  UPSTASH not configured — memory resets on every deploy.\n"
                "[Memory]    Add UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN to Railway env vars."
            )
        self._ram: dict = {}   # in-RAM fallback

    def _cmd(self, *args):
        """Execute any Redis command via Upstash REST POST."""
        if not self.ok:
            return None
        try:
            r = requests.post(
                self.url,
                headers={"Authorization": f"Bearer {self.token}"},
                json=list(args),
                timeout=5,
            )
            return r.json().get("result")
        except Exception as exc:
            print(f"[Memory/Redis] command error: {exc}")
            return None

    def get_json(self, key: str, default=None):
        if self.ok:
            raw = self._cmd("GET", key)
            if raw is not None:
                try:
                    return json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    return default
            return default
        return self._ram.get(key, default)

    def set_json(self, key: str, value):
        serialized = json.dumps(value)
        if self.ok:
            self._cmd("SET", key, serialized)
        self._ram[key] = value

    def push_item(self, key: str, value):
        """Push an item to the head of a Redis list, trimmed to MAX_TRADES."""
        serialized = json.dumps(value)
        if self.ok:
            self._cmd("LPUSH", key, serialized)
            self._cmd("LTRIM", key, 0, MAX_TRADES - 1)
        else:
            lst = self._ram.get(key, [])
            lst.insert(0, value)
            self._ram[key] = lst[:MAX_TRADES]

    def get_list(self, key: str) -> list:
        if self.ok:
            raw = self._cmd("LRANGE", key, 0, -1) or []
            out = []
            for item in raw:
                try:
                    out.append(json.loads(item) if isinstance(item, str) else item)
                except Exception:
                    pass
            return out
        return self._ram.get(key, [])


# Module-level singleton
_redis = _Redis()


# ── Trade record ──────────────────────────────────────────────
@dataclass
class TradeRecord:
    ca:               str
    symbol:           str
    entry_price:      float
    exit_price:       float
    hold_hours:       float
    outcome:          str       # WIN | LOSS | RUG | BREAKEVEN
    pnl_pct:          float
    # Signals at entry
    momentum_score:   float
    vl_ratio_5m:      float
    ofi:              float
    rsi_15m:          float
    sentiment:        float
    smart_wallet_buys: float
    top10_pct:        float
    holder_count:     int
    lp_locked:        bool
    confidence:       float
    setup_type:       str
    # Context
    market_cap:       float     = 0.0
    volume_1h:        float     = 0.0
    hour_utc:         int       = 0
    day_of_week:      int       = 0
    is_graduation:    bool      = False
    source:           str       = "scan"
    catalyst_tags:    list[str] = field(default_factory=list)
    lessons:          str       = ""


# ── Pending alert (awaiting user outcome) ─────────────────────
@dataclass
class PendingAlert:
    ca:            str
    symbol:        str
    alerted_at:    float
    entry_price:   float
    momentum_score: float
    confidence:    float
    setup_type:    str
    market_cap:    float
    volume_1h:     float
    is_graduation: bool
    source:        str


# ── TradingMemory ─────────────────────────────────────────────
class TradingMemory:
    """
    Redis-backed episodic trade memory.
    Same public API as the previous ChromaDB version —
    no changes needed in orchestrator.py or telegram_interactive.py.
    """

    # kept for compatibility — no longer used as a file path
    PENDING_FILE = None

    def __init__(self):
        raw = _redis.get_json(PENDING_KEY, {})
        self._pending: dict[str, dict] = raw if isinstance(raw, dict) else {}
        trade_count = len(_redis.get_list(TRADES_KEY))
        print(f"[Memory] {trade_count} trades | {len(self._pending)} pending alerts loaded from Redis")

    # ── Pending alert tracking ────────────────────────────────
    def _save_pending(self):
        _redis.set_json(PENDING_KEY, self._pending)

    def store_pending_alert(self, alert: PendingAlert):
        self._pending[alert.ca] = asdict(alert)
        self._save_pending()
        print(f"[Memory] Pending: ${alert.symbol} ({alert.ca[:8]}...)")

    def resolve_pending(self, symbol_or_ca: str, outcome: str, pnl_pct: float) -> Optional[TradeRecord]:
        """Called when user reports a trade result via Telegram."""
        key = None
        sym = symbol_or_ca.upper().lstrip("$")
        for ca, data in self._pending.items():
            if ca == symbol_or_ca or data.get("symbol", "").upper() == sym:
                key = ca
                break

        if not key:
            return None

        data = self._pending.pop(key)
        self._save_pending()

        now = time.time()
        record = TradeRecord(
            ca=data["ca"],
            symbol=data["symbol"],
            entry_price=data.get("entry_price", 0),
            exit_price=data.get("entry_price", 0) * (1 + pnl_pct / 100),
            hold_hours=(now - data.get("alerted_at", now)) / 3600,
            outcome=outcome,
            pnl_pct=round(pnl_pct, 2),
            momentum_score=data.get("momentum_score", 0),
            vl_ratio_5m=0, ofi=0, rsi_15m=0, sentiment=0,
            smart_wallet_buys=0, top10_pct=0, holder_count=0,
            lp_locked=False,
            confidence=data.get("confidence", 0),
            setup_type=data.get("setup_type", "?"),
            market_cap=data.get("market_cap", 0),
            volume_1h=data.get("volume_1h", 0),
            hour_utc=datetime.utcfromtimestamp(data.get("alerted_at", now)).hour,
            day_of_week=datetime.utcfromtimestamp(data.get("alerted_at", now)).weekday(),
            is_graduation=data.get("is_graduation", False),
            source=data.get("source", "scan"),
        )
        self.store(record)
        return record

    def pending_count(self) -> int:
        return len(self._pending)

    def pending_list(self) -> list[dict]:
        return list(self._pending.values())

    # ── Store confirmed trade ─────────────────────────────────
    def store(self, record: TradeRecord):
        _redis.push_item(TRADES_KEY, asdict(record))
        total = len(_redis.get_list(TRADES_KEY))
        print(
            f"[Memory] Stored ${record.symbol} → {record.outcome} "
            f"({record.pnl_pct:+.1f}%) | MCap ${record.market_cap:,.0f} "
            f"| total: {total}"
        )

    def _all_trades(self) -> list[dict]:
        return _redis.get_list(TRADES_KEY)

    # ── Similarity search (replaces ChromaDB vector search) ───
    def find_similar(
        self,
        symbol: str,
        momentum: float,
        confidence: float,
        setup_type: str,
        market_cap: float = 0,
        volume_1h: float = 0,
        is_graduation: bool = False,
        n: int = 5,
    ) -> list[dict]:
        """
        Returns past trades most similar to the current setup.
        Uses attribute scoring instead of vector embeddings —
        accurate enough for <200 trades, much simpler than ChromaDB.
        """
        all_trades = self._all_trades()
        if not all_trades:
            return []

        def mcap_bucket(m):
            if m < 50_000:    return "micro"
            if m < 200_000:   return "small"
            if m < 1_000_000: return "mid"
            return "large"

        my_bucket = mcap_bucket(market_cap)

        def similarity(t: dict) -> float:
            s = 0.0
            if t.get("setup_type") == setup_type:                         s += 3.0
            if mcap_bucket(t.get("market_cap", 0)) == my_bucket:          s += 2.0
            if abs(t.get("momentum_score", 0) - momentum) < 20:           s += 1.0
            if t.get("is_graduation") == is_graduation:                   s += 1.0
            if abs(t.get("confidence", 0) - confidence) < 0.2:            s += 0.5
            return s

        ranked = sorted(all_trades, key=similarity, reverse=True)
        return [
            {
                "document": (
                    f"${t.get('symbol')} {t.get('outcome')} "
                    f"{t.get('pnl_pct', 0):+.1f}%"
                ),
                "metadata": t,
                "distance": 1.0 - similarity(t) / 7.5,
            }
            for t in ranked[:n]
        ]

    # ── Win rate from similar trades ──────────────────────────
    def estimate_win_rate(self, similar: list[dict]) -> float:
        if not similar:
            return 0.5
        wins = sum(1 for s in similar if s["metadata"].get("outcome") == "WIN")
        return round(wins / len(similar), 2)

    # ── Pattern match summary for trust layer ─────────────────
    def find_similar_patterns(
        self,
        symbol: str,
        momentum: float,
        confidence: float,
        setup_type: str,
        market_cap: int,
        volume_1h: float = 0,
        is_graduation: bool = False,
        n: int = 5,
    ) -> dict:
        """
        Returns a structured dict for the trust/explanation layer:
        {
            "win_count": 2,
            "loss_count": 1,
            "rug_count": 0,
            "total": 3,
            "win_rate_pct": 67,
            "has_data": True,
            "conviction": "HIGH" | "MEDIUM" | "LOW" | "NONE",
            "examples": [
                {"symbol": "$TOGI", "outcome": "WIN", "pnl_pct": +112, "setup": "flash_gem"},
                ...
            ],
            "pattern_line": "📊 2W/1L on 3 similar setups (67% win rate)",
            "example_line": "✅ $TOGI +112% | ❌ $QUANT -89%",
        }
        """
        similar = self.find_similar(
            symbol=symbol,
            momentum=momentum,
            confidence=confidence,
            setup_type=setup_type,
            market_cap=market_cap,
            volume_1h=volume_1h,
            is_graduation=is_graduation,
            n=n,
        )

        if not similar:
            return {
                "win_count": 0, "loss_count": 0, "rug_count": 0,
                "total": 0, "win_rate_pct": 0, "has_data": False,
                "conviction": "NONE",
                "examples": [],
                "pattern_line": "📊 No historical data yet — first setup like this",
                "example_line": "",
            }

        wins   = [s for s in similar if s["metadata"].get("outcome") == "WIN"]
        losses = [s for s in similar if s["metadata"].get("outcome") == "LOSS"]
        rugs   = [s for s in similar if s["metadata"].get("outcome") == "RUG"]
        total  = len(similar)
        win_rate = round(len(wins) / total * 100)

        # Conviction tier
        if total >= 3 and win_rate >= 65:
            conviction = "HIGH"
        elif total >= 2 and win_rate >= 50:
            conviction = "MEDIUM"
        elif win_rate < 40 and total >= 2:
            conviction = "LOW"
        else:
            conviction = "MEDIUM"

        # Build examples list (wins first, then losses/rugs)
        examples = []
        for s in wins[:2]:
            m = s["metadata"]
            examples.append({
                "symbol": f"${m.get('symbol','?')}",
                "outcome": "WIN",
                "pnl_pct": round(float(m.get("pnl_pct", 0)), 1),
                "setup": m.get("setup_type", "?"),
            })
        for s in (losses + rugs)[:2]:
            m = s["metadata"]
            examples.append({
                "symbol": f"${m.get('symbol','?')}",
                "outcome": m.get("outcome", "LOSS"),
                "pnl_pct": round(float(m.get("pnl_pct", 0)), 1),
                "setup": m.get("setup_type", "?"),
            })

        # ── Hold duration prediction from winning trades ──────────
        win_holds = [
            float(s["metadata"].get("hold_hours", 0))
            for s in wins
            if float(s["metadata"].get("hold_hours", 0) or 0) > 0
        ]
        avg_hold = round(sum(win_holds) / len(win_holds), 1) if win_holds else 0

        if avg_hold == 0:
            hold_type  = "❓ Unknown hold — log more trades"
            hold_emoji = "❓"
        elif avg_hold < 0.5:
            hold_type  = f"⚡ QUICK SCALP — avg {int(avg_hold*60)}min on winners. Take profit fast"
            hold_emoji = "⚡"
        elif avg_hold < 3:
            hold_type  = f"🏃 SWING TRADE — avg {avg_hold:.1f}h on winners. TP1 at 30min, hold rest"
            hold_emoji = "🏃"
        elif avg_hold < 12:
            hold_type  = f"💤 HOLD TRADE — avg {avg_hold:.1f}h on winners. Be patient"
            hold_emoji = "💤"
        else:
            hold_type  = f"📅 MULTI-DAY — avg {avg_hold:.1f}h on winners. Narrative-driven hold"
            hold_emoji = "📅"

        # Format readable lines
        wr_emoji = "🟢" if win_rate >= 60 else ("🟡" if win_rate >= 40 else "🔴")
        pattern_line = (
            f"📊 {wr_emoji} {len(wins)}W/{len(losses)+len(rugs)}L on {total} "
            f"similar setups ({win_rate}% win rate)"
        )
        ex_parts = []
        for e in examples[:3]:
            icon = "✅" if e["outcome"] == "WIN" else ("💀" if e["outcome"] == "RUG" else "❌")
            ex_parts.append(f"{icon} {e['symbol']} {e['pnl_pct']:+.0f}%")
        example_line = " | ".join(ex_parts)

        return {
            "win_count":    len(wins),
            "loss_count":   len(losses),
            "rug_count":    len(rugs),
            "total":        total,
            "win_rate_pct": win_rate,
            "has_data":     True,
            "conviction":   conviction,
            "examples":     examples,
            "pattern_line": pattern_line,
            "example_line": example_line,
            # Hold duration prediction
            "avg_hold_hours":  avg_hold,
            "hold_type":       hold_type,
            "hold_emoji":      hold_emoji,
        }

    # ── Historical win rate stats ─────────────────────────────
    def win_rate_stats(self) -> dict:
        records = self._all_trades()
        if not records:
            return {"total_trades": 0, "message": "No trades stored yet"}

        total  = len(records)
        wins   = [r for r in records if r.get("outcome") == "WIN"]
        rugs   = [r for r in records if r.get("outcome") == "RUG"]
        losses = [r for r in records if r.get("outcome") == "LOSS"]

        def avg(lst, key):
            vals = [float(r[key]) for r in lst if key in r and r[key] is not None]
            return round(sum(vals) / len(vals), 3) if vals else 0.0

        win_mcaps  = [r.get("market_cap", 0) for r in wins   if r.get("market_cap", 0) > 0]
        loss_mcaps = [r.get("market_cap", 0) for r in losses if r.get("market_cap", 0) > 0]

        return {
            "total_trades":         total,
            "wins":                 len(wins),
            "losses":               len(losses),
            "rugs":                 len(rugs),
            "win_rate_pct":         round(len(wins) / total * 100, 1),
            "rug_rate_pct":         round(len(rugs) / total * 100, 1),
            "avg_pnl_pct":          round(sum(r.get("pnl_pct", 0) for r in records) / total, 1),
            "avg_momentum_wins":    avg(wins,   "momentum_score"),
            "avg_momentum_losses":  avg(losses, "momentum_score"),
            "avg_confidence_wins":  avg(wins,   "confidence"),
            "avg_hold_hours_wins":  avg(wins,   "hold_hours"),
            "avg_mcap_wins":        round(sum(win_mcaps)  / len(win_mcaps),  0) if win_mcaps  else 0,
            "avg_mcap_losses":      round(sum(loss_mcaps) / len(loss_mcaps), 0) if loss_mcaps else 0,
            "pending_feedback":     self.pending_count(),
        }

    # ── Natural-language summary of a trade (for LLM prompts) ─
    @staticmethod
    def _to_text(r) -> str:
        """Works with both TradeRecord dataclass and plain dict."""
        if isinstance(r, dict):
            symbol       = r.get("symbol", "?")
            ca           = r.get("ca", "")[:8]
            source       = "graduation" if r.get("is_graduation") else r.get("source", "scan")
            market_cap   = r.get("market_cap", 0)
            volume_1h    = r.get("volume_1h", 0)
            momentum     = r.get("momentum_score", 0)
            confidence   = r.get("confidence", 0)
            setup_type   = r.get("setup_type", "?")
            lp_locked    = r.get("lp_locked", False)
            hour_utc     = r.get("hour_utc", 0)
            dow          = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][r.get("day_of_week", 0) % 7]
            outcome      = r.get("outcome", "?")
            pnl_pct      = r.get("pnl_pct", 0)
            hold_hours   = r.get("hold_hours", 0)
            lessons      = r.get("lessons", "")
        else:
            symbol       = r.symbol
            ca           = r.ca[:8]
            source       = "graduation" if r.is_graduation else r.source
            market_cap   = r.market_cap
            volume_1h    = r.volume_1h
            momentum     = r.momentum_score
            confidence   = r.confidence
            setup_type   = r.setup_type
            lp_locked    = r.lp_locked
            hour_utc     = r.hour_utc
            dow          = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][r.day_of_week % 7]
            outcome      = r.outcome
            pnl_pct      = r.pnl_pct
            hold_hours   = r.hold_hours
            lessons      = r.lessons

        return (
            f"Token ${symbol} ({ca}...). "
            f"Source={source}. MCap=${market_cap:,.0f}. Vol1h=${volume_1h:,.0f}. "
            f"Momentum={momentum:.0f}/100. Confidence={confidence:.2f}. "
            f"Setup={setup_type}. LPLocked={lp_locked}. "
            f"EnteredAt={hour_utc:02d}:00 UTC {dow}. "
            f"Outcome={outcome} PnL={pnl_pct:+.1f}% hold={hold_hours:.1f}h. "
            f"Lessons: {lessons}"
        )

    # ── Pattern analysis (for /learn + retrospective) ─────────
    def pattern_analysis(self) -> str:
        records = self._all_trades()
        if len(records) < 3:
            return (
                f"📚 Only {len(records)} trade(s) in memory — "
                f"need at least 3 to spot patterns.\n"
                f"Report outcomes: \"$SYMBOL +80%\" or \"$SYMBOL got rugged\""
            )

        total  = len(records)
        wins   = [r for r in records if r.get("outcome") == "WIN"]
        rugs   = [r for r in records if r.get("outcome") == "RUG"]
        losses = [r for r in records if r.get("outcome") == "LOSS"]

        def avg(lst, key, default=0):
            vals = [float(r[key]) for r in lst if key in r and r[key] is not None]
            return round(sum(vals) / len(vals), 1) if vals else default

        def mcap_bucket(m):
            if m < 50_000:    return "micro"
            if m < 200_000:   return "small"
            if m < 1_000_000: return "mid"
            return "large"

        from collections import Counter
        win_buckets = Counter(mcap_bucket(r.get("market_cap", 0)) for r in wins)
        rug_buckets = Counter(mcap_bucket(r.get("market_cap", 0)) for r in rugs)
        win_setups  = Counter(r.get("setup_type", "?") for r in wins)
        rug_setups  = Counter(r.get("setup_type", "?") for r in rugs)
        win_hours   = Counter(int(r.get("hour_utc", 0)) for r in wins)
        grad_wins   = sum(1 for r in wins if r.get("is_graduation"))
        grad_total  = sum(1 for r in records if r.get("is_graduation"))

        lines = [
            f"🧠 *What the Agent Has Learned* ({total} trades)\n",
            f"📊 Win rate: *{len(wins)/total*100:.0f}%* | Rug rate: *{len(rugs)/total*100:.0f}%*",
            "",
            "📈 *Signal averages (Wins vs Losses)*",
            f"  Momentum:   Wins *{avg(wins,'momentum_score')}* vs Losses *{avg(losses,'momentum_score')}*/100",
            f"  Confidence: Wins *{avg(wins,'confidence'):.0%}* vs Losses *{avg(losses,'confidence'):.0%}*",
            f"  Avg hold:   *{avg(wins,'hold_hours'):.1f}h* on wins",
            "",
            "💰 *MCap ranges (Win vs Rug counts)*",
        ]
        for bucket in ["micro", "small", "mid", "large"]:
            w = win_buckets.get(bucket, 0)
            r = rug_buckets.get(bucket, 0)
            if w + r > 0:
                lines.append(f"  {bucket}: {w} wins / {r} rugs")

        lines += ["", "🎯 *Best setups (top 3)*"]
        for setup, count in win_setups.most_common(3):
            lines.append(f"  {setup}: {count} wins")

        lines += ["", "⚠️ *Riskiest setups*"]
        for setup, count in rug_setups.most_common(3):
            lines.append(f"  {setup}: {count} rugs")

        if win_hours:
            best_hour = win_hours.most_common(1)[0][0]
            lines.append(f"\n⏰ *Best entry hour:* {best_hour:02d}:00 UTC")

        if grad_total > 0:
            lines.append(
                f"\n🎓 *Graduations:* {grad_wins}/{grad_total} wins "
                f"({grad_wins/grad_total*100:.0f}%)"
            )

        if self.pending_count() > 0:
            lines.append(
                f"\n📬 *{self.pending_count()} alerts awaiting your outcome report*\n"
                f"Tell me: \"$SYMBOL +80%\" or \"$SYMBOL got rugged\""
            )

        return "\n".join(lines)


# ── SelfLearningLoop ─────────────────────────────────────────
class SelfLearningLoop:
    """
    Three learning triggers:
      1. run_retrospective()    — nightly, full Claude analysis
      2. update_from_feedback() — immediate, when user reports a trade
      3. mini_study()           — every 6h, lightweight pattern extraction
    """

    def __init__(self, memory: TradingMemory):
        self.memory  = memory
        import anthropic
        self.claude  = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.weights = self._load()

    def _load(self) -> dict:
        w = _redis.get_json(WEIGHTS_KEY)
        if not isinstance(w, dict):
            w = DEFAULT_WEIGHTS.copy()
        else:
            # Backfill any new keys
            for k, v in DEFAULT_WEIGHTS.items():
                if k not in w:
                    w[k] = v
        print(f"[SelfLearning] Weights v{w.get('version', 1)} loaded from Redis")
        return w

    def _save(self, w: dict):
        w["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        w["version"]      = w.get("version", 1) + 1
        _redis.set_json(WEIGHTS_KEY, w)
        self.weights = w
        print(f"[SelfLearning] Weights v{w['version']} saved to Redis")

    # ── Immediate feedback update ─────────────────────────────
    async def update_from_feedback(self, record: TradeRecord) -> str:
        stats = self.memory.win_rate_stats()
        prompt = (
            f"A Solana memecoin trade just closed:\n"
            f"${record.symbol} | {record.outcome} | PnL: {record.pnl_pct:+.1f}% "
            f"| MCap at entry: ${record.market_cap:,.0f} "
            f"| Momentum: {record.momentum_score:.0f}/100 "
            f"| Confidence: {record.confidence:.0%} "
            f"| Source: {record.source} "
            f"| Setup: {record.setup_type}\n\n"
            f"Overall stats: {stats.get('win_rate_pct', 0)}% win rate over "
            f"{stats.get('total_trades', 0)} trades.\n\n"
            f"In one sentence, what is the key lesson from this trade? "
            f"Then on a new line, if this outcome strongly suggests adjusting "
            f"a threshold (momentum_score_threshold, min_confidence, min_mcap_alert), "
            f"output ONLY: ADJUST key=value (e.g. ADJUST momentum_score_threshold=65). "
            f"Otherwise output: NO_CHANGE"
        )
        try:
            r = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            text   = r.content[0].text.strip()
            lines  = text.split("\n")
            # Skip any markdown headers / blank lines Claude prefixes before the real lesson
            lesson = ""
            for l in lines:
                stripped = l.strip()
                if stripped and not stripped.startswith("#"):
                    lesson = stripped
                    break
            if not lesson:
                lesson = "No lesson extracted."

            for line in lines:
                if line.startswith("ADJUST "):
                    try:
                        kv = line[7:].split("=", 1)
                        key, val = kv[0].strip(), kv[1].strip()
                        if key in self.weights:
                            old = self.weights[key]
                            self.weights[key] = type(old)(val)
                            print(f"[SelfLearning] {key}: {old} → {self.weights[key]}")
                    except Exception:
                        pass

            patterns = self.weights.get("learned_patterns", [])
            patterns.append({
                "symbol":    record.symbol,
                "outcome":   record.outcome,
                "pnl_pct":   record.pnl_pct,
                "lesson":    lesson,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
            self.weights["learned_patterns"] = patterns[-30:]
            self.weights["last_win_rate"]    = stats.get("win_rate_pct", 0.0)
            self._save(self.weights)
            return lesson
        except Exception as exc:
            print(f"[SelfLearning] feedback update error: {exc}")
            return ""

    # ── Mini study (every 6h) ─────────────────────────────────
    async def mini_study(self):
        stats = self.memory.win_rate_stats()
        if stats.get("total_trades", 0) < 3:
            print("[SelfLearning] mini_study: not enough trades yet")
            return

        avg_win_mcap  = stats.get("avg_mcap_wins",  0)
        avg_loss_mcap = stats.get("avg_mcap_losses", 0)
        if avg_win_mcap > 0 and avg_loss_mcap > 0:
            new_min = max(10_000, avg_win_mcap * 0.3)
            if abs(new_min - self.weights.get("min_mcap_alert", 0)) > 5_000:
                self.weights["min_mcap_alert"] = int(new_min)
                print(f"[SelfLearning] min_mcap_alert adjusted to ${new_min:,.0f}")

        self.weights["last_win_rate"] = stats.get("win_rate_pct", 0.0)
        self._save(self.weights)
        print(f"[SelfLearning] mini_study complete — {stats.get('total_trades', 0)} trades")

    # ── Nightly retrospective ─────────────────────────────────
    async def run_retrospective(self, day_trades: list) -> dict:
        """
        Full nightly retrospective — Claude analyses today's trades + all-time stats
        and proposes new filter thresholds.
        day_trades: list of TradeRecord or dict
        """
        if not day_trades:
            print("[SelfLearning] No trades today — skipping retrospective.")
            return self.weights

        stats     = self.memory.win_rate_stats()
        summaries = [TradingMemory._to_text(t) for t in day_trades[:15]]
        analysis  = self.memory.pattern_analysis()

        prompt = f"""You are a quantitative trading AI performing a nightly retrospective
for a Solana memecoin scanner.

PERFORMANCE STATS (all-time):
{json.dumps(stats, indent=2)}

PATTERN ANALYSIS:
{analysis}

TODAY'S TRADES ({len(day_trades)} confirmed):
{chr(10).join(f"- {s}" for s in summaries)}

CURRENT WEIGHTS (v{self.weights.get('version', 1)}):
{json.dumps({k: v for k, v in self.weights.items() if k != 'learned_patterns'}, indent=2)}

YOUR TASK:
1. Identify which signals (momentum_score, confidence, MCap range, setup type,
   graduation vs launch) best predicted WINS vs LOSSES vs RUGS.
2. Propose concrete numeric threshold changes. Be conservative — small adjustments (5-10%).
3. Set min_mcap_alert and max_mcap_alert to the MCap range where wins cluster.
4. If rug patterns correlate with specific setups, note in learned_patterns.
5. Update last_win_rate.
6. Return ONLY valid JSON matching the weights schema. No prose, no markdown fences."""

        response = self.claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()

        try:
            new_weights = json.loads(raw)
            for k, v in new_weights.items():
                self.weights[k] = v
            self._save(self.weights)
        except json.JSONDecodeError as exc:
            print(f"[SelfLearning] JSON parse error: {exc}")
            print(f"[SelfLearning] Raw: {raw[:300]}")

        return self.weights

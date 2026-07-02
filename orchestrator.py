# orchestrator.py — Master agent loop
# Wires all modules together. Runs continuously via asyncio.
# APScheduler fires reports at 9 UTC (10am Nigeria) and 18 UTC (7pm Nigeria).
# Usage: python orchestrator.py

import asyncio
import json
import os
import time
from datetime import datetime

import anthropic
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from data_ingestion import DataAggregator, DiscoverPoller, BirdeyeScanner
from ta_engine import TAEngine
from risk_shield import AntiRugShield
from vector_memory import TradingMemory, SelfLearningLoop, TradeRecord, PendingAlert, _redis as _mem_redis
from telegram_bot import TelegramAlerter

# ── Optional new modules (loaded safely) ─────────────────────
try:
    from early_entry import EarlyEntryDetector
    _EARLY_OK = True
except Exception as _e:
    print(f"[WARN] early_entry not loaded: {_e}")
    _EARLY_OK = False

try:
    from telegram_interactive import InteractiveBot
    _INTERACTIVE_OK = True
except Exception as _e:
    print(f"[WARN] telegram_interactive not loaded: {_e}")
    _INTERACTIVE_OK = False

try:
    from smart_wallet import SmartWalletRegistry, SolanaWalletMonitor, WalletAutoDiscovery
    _WALLET_OK = True
except Exception as _e:
    print(f"[WARN] smart_wallet not loaded: {_e}")
    _WALLET_OK = False

try:
    from axiom_pulse import AxiomPulseFeed, format_launch_alert, format_graduation_alert
    _PULSE_OK = True
except Exception as _e:
    print(f"[WARN] axiom_pulse not loaded: {_e}")
    _PULSE_OK = False

try:
    from my_wallet_tracker import MyWalletTracker
    _MY_WALLET_OK = True
except Exception as _e:
    print(f"[WARN] my_wallet_tracker not loaded: {_e}")
    _MY_WALLET_OK = False

load_dotenv()

SCAN_INTERVAL_S  = int(os.getenv("SCAN_INTERVAL_S", "30"))
POSITIONS_KEY    = "axiom:positions"
GHOST_KEY        = "axiom:ghost_watch"


class AxiomAgent:

    def __init__(self):
        smart_wallets        = json.loads(os.getenv("SMART_WALLETS", "[]"))
        self.aggregator      = DataAggregator(smart_wallets)
        self.ta              = TAEngine()
        self.shield          = AntiRugShield()
        self.memory          = TradingMemory()
        self.learner         = SelfLearningLoop(self.memory)
        self.claude          = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.telegram        = TelegramAlerter()
        self._candidates         = []
        self._day_trades         = []
        self._early_signals      = []
        self._recent_graduations = []   # last 20 pump.fun → Raydium migrations
        self._best_scanned       = []   # top tokens by volume ratio (report fallback)
        self._recent_alerts      = []   # last 20 alerted tokens with full context + timestamp
        self._ghost_watch        = {}   # {ca: {symbol, score, first_price, tracked_at}} ghost-tracked tokens
        # ── Persist open positions across restarts ────────────────
        self._open_positions: dict = _mem_redis.get_json(POSITIONS_KEY, {}) or {}
        print(f"[Agent] Restored {len(self._open_positions)} open position(s) from Redis")
        # Alert cooldown: {ca: last_alert_timestamp} — persisted in Redis
        self._COOLDOWN_KEY   = "axiom:cooldown"
        self._alert_cooldown = _mem_redis.get_json(self._COOLDOWN_KEY, {}) or {}
        self._COOLDOWN_SECS  = int(os.getenv("ALERT_COOLDOWN_HOURS", "4")) * 3600
        self._HIGH_CONF_RESEND = float(os.getenv("HIGH_CONF_RESEND", "90"))

        # ── Session stats (daily, persisted to Redis) ──────────────────
        self._SESSION_STATS_KEY = "axiom:session_stats"
        _saved = _mem_redis.get_json(self._SESSION_STATS_KEY, {}) or {}
        _today = datetime.utcnow().strftime("%Y-%m-%d")
        self._session_stats: dict = _saved if _saved.get("date") == _today else {}
        if not self._session_stats:
            self._session_stats = {
                "date": _today, "alerts_total": 0, "hc_alerts": 0, "normal_alerts": 0,
                "closed_wins": 0, "closed_losses": 0,
                "narrative_perf": {}, "hourly_alerts": {},
                "last_gem_time": 0.0, "last_gem_symbol": "",
                "market_regime": "MIXED", "pumping": 0, "declining": 0, "total_scanned": 0,
            }
        print(f"[Agent] Session stats loaded: {self._session_stats.get('alerts_total',0)} alerts today")

        # Optional modules
        self.early_detector  = EarlyEntryDetector() if _EARLY_OK else None
        self.interactive_bot = InteractiveBot(agent_ref=self) if _INTERACTIVE_OK else None
        self.pulse_feed      = (
            AxiomPulseFeed(
                on_new_launch=self._on_new_launch,
                on_graduation=self._on_graduation,
            ) if _PULSE_OK else None
        )

        if _WALLET_OK:
            self.wallet_registry  = SmartWalletRegistry()
            self.wallet_monitor   = SolanaWalletMonitor(
                self.wallet_registry, self._on_smart_wallet_buy
            )
            self.wallet_discovery = WalletAutoDiscovery(self.wallet_registry)
        else:
            self.wallet_registry  = None
            self.wallet_monitor   = None
            self.wallet_discovery = None

        # My-wallet auto-tracker (user's own trades → auto enter/exit)
        self.my_wallet_tracker = (
            MyWalletTracker(_mem_redis, self._on_my_buy, self._on_my_sell)
            if _MY_WALLET_OK else None
        )
        # Load saved wallet NOW (in __init__) so startup message shows correct status.
        # Without this, load() only runs inside run() — after the message is already sent.
        if self.my_wallet_tracker:
            self.my_wallet_tracker.load()

        # Scheduler
        self.scheduler = AsyncIOScheduler()
        self.scheduler.add_job(self._daily_report,    "cron", hour=9,  minute=0,  id="morning")
        self.scheduler.add_job(self._daily_report,    "cron", hour=18, minute=0,  id="evening")
        self.scheduler.add_job(self._retrospective,   "cron", hour=23, minute=55)
        self.scheduler.add_job(self._position_monitor,"interval", minutes=5)
        self.scheduler.add_job(self._ghost_check,     "interval", minutes=30)
        self.scheduler.add_job(self._mini_study,      "interval", hours=6)
        if _WALLET_OK and self.wallet_discovery:
            self.scheduler.add_job(self._auto_discover_wallets, "interval", hours=6)

    # ── Alert cooldown persistence (Redis) ───────────────────────
    def _save_cooldown(self):
        # Prune entries older than 24h before saving
        cutoff = time.time() - 86400
        pruned = {k: v for k, v in self._alert_cooldown.items() if v > cutoff}
        self._alert_cooldown = pruned
        _mem_redis.set_json(self._COOLDOWN_KEY, pruned)

    def _save_session_stats(self):
        """Persist session stats to Redis."""
        _mem_redis.set_json(self._SESSION_STATS_KEY, self._session_stats)

    # ── Alert deduplication ────────────────────────────────────
    def _should_alert(self, ca: str, confidence_score: float = 0.0) -> bool:
        """
        Returns True if this token should fire an alert.
        - First time seen: always alert
        - Repeat within cooldown window: only alert if score >= HIGH_CONF_RESEND (default 90)
        - After cooldown expires: alert again normally
        """
        now  = time.time()
        last = self._alert_cooldown.get(ca, 0)
        elapsed = now - last

        if elapsed > self._COOLDOWN_SECS:
            self._alert_cooldown[ca] = now
            self._save_cooldown()
            return True
        if confidence_score >= self._HIGH_CONF_RESEND:
            self._alert_cooldown[ca] = now
            self._save_cooldown()
            return True
        print(f"[Cooldown] {ca[:8]} suppressed (last alert {elapsed/3600:.1f}h ago, score {confidence_score:.0f})")
        return False

    # ── Smart wallet callback ──────────────────────────────────
    async def _on_smart_wallet_buy(self, event: dict):
        wallet   = event.get("wallet", "")
        ca       = event.get("ca", "")
        sym      = event.get("symbol", "?")
        amount   = event.get("amount", 0)
        currency = event.get("currency", "SOL")
        win_rate = event.get("win_rate", 50.0)
        sig      = event.get("sig", "")

        amt_str = f"{amount:.2f} {currency}" if amount else "unknown amount"
        await self.telegram.send(
            f"🐋 *SMART WALLET BUY — ${sym}*\n"
            f"Wallet: `{wallet[:8]}...{wallet[-4:]}` (win rate: *{win_rate:.0f}%*)\n"
            f"Spent: *{amt_str}*\n"
            f"CA: `{ca}`\n"
            f"[DexScreener](https://dexscreener.com/solana/{ca})\n"
            f"_Your buy will be auto-tracked on-chain_"
        )

    async def _auto_discover_wallets(self):
        if self.wallet_discovery:
            async with aiohttp.ClientSession() as session:
                await self.wallet_discovery.discover(session)

    # ── Token processing ───────────────────────────────────────
    async def _process_token(self, raw: dict):
        ca = raw.get("contract_address", "")

        # Early entry detection
        if self.early_detector:
            try:
                early = self.early_detector.analyse(raw)
                if early and early.is_actionable:
                    self._early_signals.append(early)
                    if len(self._early_signals) > 20:
                        self._early_signals = self._early_signals[-20:]
                    if early.urgency == "URGENT":
                        # Only alert if not in cooldown (re-alert if score ≥ HIGH_CONF_RESEND)
                        if self._should_alert(ca, confidence_score=early.score):
                            msg = self.early_detector.format_alert(early, raw)
                            await self.telegram.send(msg)
                            self._add_recent_alert(early.symbol, ca, {
                                "score":      early.score,
                                "urgency":    early.urgency,
                                "market_cap": raw.get("market_cap", 0),
                                "volume_5m":  raw.get("volume_5m", 0),
                                "liquidity":  raw.get("liquidity", 0),
                                "price_usd":  raw.get("price_usd", 0),
                                "source":     "early_entry",
                            })
            except Exception as exc:
                print(f"[EarlyEntry] error: {exc}")

        snap = await self.aggregator.build_snapshot(raw)
        if snap is None:
            return

        # STAGE 1: Anti-rug shield
        risk = self.shield.evaluate(snap)
        if not risk.passed:
            return

        # STAGE 2: Technical analysis
        prices = raw.get("price_history_15m", [snap.price_usd] * 20)
        ta     = self.ta.analyze(snap, prices)

        # STAGE 3: Combined signal gate — require multiple conviction signals
        w = self.learner.weights
        # Hard kills first
        if ta.setup_type == "BLOWOFF_TOP": return
        if ta.momentum_score < w.get("momentum_score_threshold", 40): return
        if ta.confidence     < w.get("min_confidence", 0.30):         return

        # MCap sweet spot: $20k–$5M (too small = instant rug; too large = no room to 5x)
        mcap = snap.market_cap
        if mcap > 0 and (mcap < 15_000 or mcap > 5_000_000):
            return

        # Volume sanity: must have real activity (>$3k/h estimated)
        vol_1h_est = snap.volume_5m * 12
        if vol_1h_est < 3_000:
            return

        # Count strong signals — need at least 2 of 4 for a quality setup
        strong = 0
        if ta.momentum_score >= 60:                         strong += 1
        if vol_1h_est >= 10_000:                            strong += 1
        if snap.smart_wallet_buys_1h >= 500:                strong += 1
        if snap.holder_count >= 30 or snap.lp_locked:       strong += 1
        if strong < 2:
            return  # Weak signal — skip, don't waste an alert

        # Optional: twitter/OFI filters when data exists
        if snap.twitter_mentions_10m > 5:
            if snap.twitter_sentiment_score < w.get("min_twitter_sentiment", -1.0): return
        if ta.order_flow_imbalance != 0:
            if ta.order_flow_imbalance < w.get("ofi_threshold", -0.5): return

        # STAGE 4: Vector memory — pattern match + trust layer
        pattern  = self.memory.find_similar_patterns(
            symbol       = snap.symbol,
            momentum     = ta.momentum_score,
            confidence   = ta.confidence,
            setup_type   = ta.setup_type,
            market_cap   = snap.market_cap,
            volume_1h    = snap.volume_5m * 12,
            is_graduation= raw.get("_is_graduation", False),
            n            = 5,
        )
        win_rate = pattern["win_rate_pct"] / 100 if pattern["has_data"] else 0.5

        # If similar past trades were mostly rugs — skip
        rug_rate = pattern["rug_count"] / max(pattern["total"], 1)
        if rug_rate >= 0.6 and pattern["total"] >= 3:
            print(f"[Scan] ${snap.symbol} skipped — {rug_rate:.0%} rug rate in similar trades")
            return

        # HIGH CONVICTION — relative to bot's current baseline win rate
        # Pattern must beat the bot's average by 15+ points AND have ≥2 samples
        stats = self.memory.win_rate_stats()
        overall_wr = stats.get("win_rate_pct", 50.0)   # e.g. 29%
        pattern_wr = pattern["win_rate_pct"]             # e.g. 60%
        edge       = pattern_wr - overall_wr             # e.g. +31 points
        pattern["overall_wr"] = round(overall_wr, 1)
        pattern["edge"]       = round(edge, 1)

        signals_strong = (
            ta.momentum_score >= 60
            and vol_1h_est >= 10_000
            and (snap.smart_wallet_buys_1h >= 500 or snap.holder_count >= 30 or snap.lp_locked)
        )
        pattern_edge_confirmed = (
            pattern["has_data"]
            and pattern["total"] >= 2
            and edge >= 15          # ≥15 points above bot's own average
        )
        conviction_tier = "HIGH" if (signals_strong and pattern_edge_confirmed) else "NORMAL"
        if conviction_tier == "HIGH":
            print(f"[Scan] 🔥 HIGH CONVICTION ${snap.symbol} — pattern {pattern_wr:.0f}% vs avg {overall_wr:.0f}% (+{edge:.0f}pts)")

        self._candidates.append({
            "snap": snap, "ta": ta, "risk": risk,
            "win_rate": win_rate, "pattern": pattern, "conviction": conviction_tier,
        })

        # ── Ghost tracking: silently watch tokens that score well but don't alert ──
        # These build training data even when you don't trade
        ghost_score = (ta.momentum_score * ta.confidence * 100)
        if ghost_score >= 55 and snap.ca not in self._ghost_watch:
            self._ghost_watch[snap.ca] = {
                "symbol":      snap.symbol,
                "score":       round(ghost_score, 1),
                "first_price": snap.price_usd,
                "tracked_at":  time.time(),
                "market_cap":  snap.market_cap,
                "momentum":    ta.momentum_score,
                "confidence":  ta.confidence,
                "setup_type":  ta.setup_type,
            }
            print(f"[Ghost] Tracking ${snap.symbol} score={ghost_score:.0f} price=${snap.price_usd}")

        # STAGE 5: Alert threshold — score-based (momentum × confidence ≥ 65)
        alert_score = ta.momentum_score * ta.confidence
        if (
            alert_score >= float(os.getenv("ALERT_SCORE_THRESHOLD", "65"))
            and self._should_alert(snap.ca, confidence_score=alert_score)
        ):
            catalyst  = await self._catalyst(snap, ta)
            narrative = await self._narrative_score(
                symbol  = snap.symbol,
                name    = raw.get("name", snap.symbol),
                socials = raw.get("socials", []),
                boosts  = int(raw.get("boosts", 0) or 0),
                chg1h   = float(raw.get("chg1h", 0) or 0),
            )
            # Re-evaluate conviction now that we have narrative
            # HIGH CONVICTION requires pattern edge AND narrative substance
            narrative_ok = (
                narrative["narrative_strength"] >= 6
                and (narrative["has_twitter"] or narrative["has_telegram"])
            ) or narrative["trending"]
            if conviction_tier == "HIGH" and not narrative_ok:
                conviction_tier = "NORMAL"
                print(f"[Scan] ${snap.symbol} downgraded to NORMAL — weak narrative ({narrative['narrative_strength']}/10, no socials)")
            msg = self._format_block(snap, ta, risk,
                                     catalyst=catalyst, win_rate=win_rate,
                                     pattern=pattern, conviction=conviction_tier,
                                     narrative=narrative, alert=True)
            await self.telegram.send(msg)
            # Store pending alert so user can report outcome and train memory
            self._store_pending_alert(snap, ta, raw)
            # Also store in recent alerts so chat can reference it for 2h
            self._add_recent_alert(snap.symbol, snap.ca, {
                "score":      ta.momentum_score,
                "urgency":    "URGENT",
                "market_cap": snap.market_cap,
                "volume_5m":  snap.volume_5m,
                "liquidity":  snap.liquidity_usd,
                "price_usd":  snap.price_usd,
                "setup_type": ta.setup_type,
                "confidence": ta.confidence,
                "win_rate":   win_rate,
                "source":     "scan",
            })

    # ── Recent alerts memory (for chat context) ───────────────
    def _add_recent_alert(self, symbol: str, ca: str, alert_data: dict):
        """Store a snapshot of any alert sent, so chat can reference it up to 2h later."""
        self._recent_alerts.append({
            "symbol":         symbol,
            "ca":             ca,
            "alerted_at":     time.time(),
            **alert_data,
        })
        if len(self._recent_alerts) > 20:
            self._recent_alerts = self._recent_alerts[-20:]

    # ── Pending alert context storage ────────────────────────
    def _store_pending_alert(self, snap, ta, raw: dict):
        """Store full context when we send an alert, so user can report outcome."""
        try:
            alert = PendingAlert(
                ca=snap.ca,
                symbol=snap.symbol,
                alerted_at=time.time(),
                entry_price=snap.price_usd,
                momentum_score=ta.momentum_score,
                confidence=ta.confidence,
                setup_type=ta.setup_type,
                market_cap=snap.market_cap,
                volume_1h=snap.volume_5m * 12,
                is_graduation=raw.get("_is_graduation", False),
                source=raw.get("_source", "scan"),
            )
            self.memory.store_pending_alert(alert)
        except Exception as exc:
            print(f"[Agent] store_pending_alert error: {exc}")

    # ── Mini self-study (every 6h) ────────────────────────────
    async def _mini_study(self):
        """Lightweight pattern study — runs every 6h, no trades needed."""
        try:
            await self.learner.mini_study()
        except Exception as exc:
            print(f"[Agent] mini_study error: {exc}")

    # ── Axiom Pulse callbacks ─────────────────────────────────
    async def _on_new_launch(self, token):
        """Called when pump.fun creates a new token.
        Only alert if MCap crosses $50k — filters out 95% of dead launches."""
        try:
            # Silent processing — score it but don't alert yet
            await self._process_token(token.to_dict())

            # Alert only when there's real early buying ($50k+ mcap)
            MIN_MCAP = float(os.getenv("LAUNCH_ALERT_MCAP", "50000"))
            if token.market_cap >= MIN_MCAP:
                msg = format_launch_alert(token)
                await self.telegram.send(msg)
                print(f"[Pulse] 🔔 Launch alert sent: ${token.symbol} MCap ${token.market_cap:,.0f}")
            else:
                print(f"[Pulse] silent: ${token.symbol} MCap ${token.market_cap:,.0f} < ${MIN_MCAP:,.0f}")
        except Exception as exc:
            print(f"[Pulse] launch callback error: {exc}")

    async def _on_graduation(self, token):
        """Called instantly when a token bonds to Raydium — prime entry window."""
        try:
            # Store for /scan command (keep last 20)
            self._recent_graduations.append({
                "ca":         token.ca,
                "symbol":     token.symbol,
                "name":       token.name,
                "market_cap": token.market_cap,
                "liquidity":  token.liquidity,
                "volume_1h":  token.volume_1h,
                "price_usd":  token.price_usd,
                "timestamp":  time.time(),
            })
            if len(self._recent_graduations) > 20:
                self._recent_graduations = self._recent_graduations[-20:]

            msg = format_graduation_alert(token)
            await self.telegram.send(msg)
            # Store pending alert — graduation = highest quality signal
            pending = PendingAlert(
                ca=token.ca,
                symbol=token.symbol,
                alerted_at=time.time(),
                entry_price=token.price_usd,
                momentum_score=80,  # graduation is inherently high momentum
                confidence=0.75,
                setup_type="GRADUATION",
                market_cap=token.market_cap,
                volume_1h=token.volume_1h,
                is_graduation=True,
                source="graduation",
            )
            self.memory.store_pending_alert(pending)
            # Process through full pipeline with higher priority
            raw = token.to_dict()
            raw["lp_locked"] = True
            await self._process_token(raw)
        except Exception as exc:
            print(f"[Pulse] graduation callback error: {exc}")

    # ── Flash Gain Scanner ────────────────────────────────────
    async def _flash_gain_loop(self):
        """
        Dedicated 30-second loop that hunts tokens pumping >50% in 1h.
        Two tiers:
          Tier 1 (micro): MCap $0–$500k   — earliest entry, highest risk/reward
          Tier 2 (small): MCap $500k–$5M  — slightly larger, better liquidity
        $GOKHSHTEIN ($660k MCap, +112% 1h) was missed because old cap was $500k.
        """
        FLASH_INTERVAL = int(os.getenv("FLASH_SCAN_INTERVAL_S", "30"))  # was 60
        async with aiohttp.ClientSession() as session:
            birdeye = BirdeyeScanner(session)
            while True:
                try:
                    await self._flash_gain_scan(birdeye)
                except Exception as exc:
                    print(f"[FlashScan] error: {exc}")
                await asyncio.sleep(FLASH_INTERVAL)

    async def _flash_gain_scan(self, birdeye: BirdeyeScanner):
        """Check Birdeye high-gainers across two MCap tiers."""
        min_chg = float(os.getenv("FLASH_MIN_CHG1H", "50"))   # >50% 1h gain
        max_age = float(os.getenv("FLASH_MAX_AGE_H", "6"))    # <6h old
        # Raised from $500k to $5M — catches gems like $GOKHSHTEIN ($660k MCap)
        max_mc  = float(os.getenv("FLASH_MAX_MCAP",  "5000000"))

        gems = await birdeye.fetch_high_gainers(
            min_chg1h=min_chg, max_age_h=max_age, max_mcap=max_mc
        )

        for t in gems:
            ca  = t.get("contract_address", "")
            sym = t.get("symbol", "?")
            if not ca:
                continue

            # Use a 1h cooldown for flash alerts (shorter than main 4h cooldown)
            last = self._alert_cooldown.get(f"flash:{ca}", 0)
            if time.time() - last < 3600:
                continue

            chg1h  = float(t.get("chg1h",  0) or 0)
            chg5m  = float(t.get("chg5m",  0) or 0)
            mcap   = float(t.get("market_cap", 0) or 0)
            vol1h  = float(t.get("volume_1h",  0) or 0)
            liq    = float(t.get("liquidity_usd", 0) or 0)
            age_h  = float(t.get("age_hours", 0) or 0)
            price  = float(t.get("price_usd", 0) or 0)

            # ── Anti-rug filters (learned from $QUANT -89% loss) ──────────
            # Liquidity must be substantial — $6.65K got rugged in 2 minutes
            if liq < 15_000:
                continue

            # Must be at least 3 minutes old — first-minute pumps are setup dumps
            if age_h < (3 / 60):
                continue

            # Reject if 5m change is deeply negative — pump already over
            # (token showed +50% 1h but -30% 5m = you're entering the dump)
            if chg5m < -20:
                continue

            # Volume must be buy-side positive — skip if sellers dominating
            vol5m  = float(t.get("volume_5m", 0) or 0)
            buys5m = int(t.get("buy_txns_15m", 0) or 0)
            sells5m = int(t.get("sell_txns_15m", 0) or 0)
            if sells5m > 0 and buys5m / max(sells5m, 1) < 0.8:
                # Fewer than 80% buy-side transactions — sellers in control
                continue

            # MCap tier label (display only — sizing set after conviction is known)
            if mcap < 100_000:
                tier = "🔬 MICRO"
            elif mcap < 500_000:
                tier = "💊 SMALL"
            elif mcap < 2_000_000:
                tier = "💊 MID-SMALL"
            else:
                tier = "📦 MID"

            # Rug risk label based on liquidity depth
            if liq < 25_000:
                rug_label = "🔴 HIGH RUG RISK — liq thin"
            elif liq < 60_000:
                rug_label = "🟡 MODERATE RISK"
            else:
                rug_label = "🟢 LOWER RISK"

            # Buy/sell ratio label
            buy_pct = int(buys5m / max(buys5m + sells5m, 1) * 100)

            # Pattern match — relative to bot's baseline, show edge
            pattern = self.memory.find_similar_patterns(
                symbol     = sym,
                momentum   = min(100, int(chg1h)),
                confidence = min(1.0, chg1h / 100),
                setup_type = "flash_gem",
                market_cap = int(mcap),
                volume_1h  = vol1h,
            )
            stats      = self.memory.win_rate_stats()
            overall_wr = stats.get("win_rate_pct", 50.0)
            pattern_wr = pattern["win_rate_pct"]
            edge       = pattern_wr - overall_wr
            pattern["overall_wr"] = round(overall_wr, 1)
            pattern["edge"]       = round(edge, 1)

            # Edge label
            if pattern["has_data"] and edge >= 15:
                edge_str = f" ⬆️ +{edge:.0f}pts above your avg ({overall_wr:.0f}%) — EDGE CONFIRMED"
            elif pattern["has_data"] and edge > 0:
                edge_str = f" (+{edge:.0f}pts above avg)"
            elif pattern["has_data"] and edge < 0:
                edge_str = f" (⚠️ {edge:.0f}pts below avg)"
            else:
                edge_str = ""

            pattern_lines = pattern["pattern_line"] + edge_str
            if pattern.get("example_line"):
                pattern_lines += f"\n  {pattern['example_line']}"

            # Narrative scoring — what meme is this riding?
            narr = await self._narrative_score(
                symbol  = sym,
                name    = t.get("name", sym),
                socials = t.get("socials", []),
                boosts  = int(t.get("boosts", 0) or 0),
                chg1h   = chg1h,
            )

            # HIGH CONVICTION flash requires ALL three pillars:
            # 1. Pattern edge: beats bot avg by ≥15pts with ≥2 samples
            # 2. Still pumping: 5m positive
            # 3. Narrative gate: meme coins MUST have social presence + decent narrative
            #    (weak narrative = no community = pump dies fast)
            narrative_ok = (
                narr["narrative_strength"] >= 6          # narrative has real substance
                and (narr["has_twitter"] or narr["has_telegram"])  # at least one social
            ) or narr["trending"]                        # OR it's a confirmed trending narrative
            pattern_edge_ok = (
                pattern["has_data"]
                and pattern["total"] >= 2
                and edge >= 15
                and chg5m > 0
            )
            high_conv_flash = pattern_edge_ok and narrative_ok
            conviction_header = "🔥 *HIGH CONVICTION FLASH*" if high_conv_flash else "⚡ *FLASH GEM*"

            # ── Single-line trade plan: one size, one TP, one SL ───────
            hold_type_str = pattern.get("hold_type", "") if pattern.get("has_data") else ""
            thin_liq = liq < 25_000

            # SOL size — one number, not a range
            if thin_liq:
                sol_size = 0.05
            elif high_conv_flash:
                sol_size = 0.10 if mcap < 100_000 else (0.20 if mcap < 500_000 else (0.30 if mcap < 2_000_000 else 0.25))
            else:
                sol_size = 0.03 if mcap < 100_000 else (0.08 if mcap < 500_000 else (0.15 if mcap < 2_000_000 else 0.12))

            # TP % — single target based on conviction + what past winners held for
            if thin_liq:
                tp_pct, sl_pct = 60, 25
            elif high_conv_flash:
                sl_pct = 20
                tp_pct = (80  if "SCALP"     in hold_type_str else
                          150 if "SWING"     in hold_type_str else
                          250 if "HOLD"      in hold_type_str else
                          350 if "MULTI-DAY" in hold_type_str else 100)
            else:
                sl_pct = 25
                tp_pct = (40  if "SCALP"     in hold_type_str else
                          80  if "SWING"     in hold_type_str else
                          120 if "HOLD"      in hold_type_str else
                          200 if "MULTI-DAY" in hold_type_str else 60)

            hc_tag    = " 🔥" if high_conv_flash else ""
            sell_pct  = 70 if high_conv_flash else 100   # HC: keep 30% runner; NORMAL: full exit
            size_tip  = f"{sol_size:.2f} SOL{hc_tag}"
            tp_plan   = f"🎯 TP: +{tp_pct}% (sell {sell_pct}%) | 🛑 SL: -{sl_pct}% (exit all)"

            # Hold type from pattern history
            hold_line = ""
            if pattern.get("avg_hold_hours", 0) > 0:
                hold_line = f"⏳ {pattern['hold_type']}\n"
            elif narr.get("hold_signal") == "NARRATIVE":
                hold_line = "⏳ Narrative-driven — hold longer if trend stays hot\n"

            # ── Bear market suppression — NORMAL alerts muted in BEAR ─────
            # Exception: strong moves (≥80% 1h) cut through BEAR suppression —
            # they represent real momentum even in a down market.
            strong_move = chg1h >= 80
            if (not high_conv_flash and not strong_move
                    and self._session_stats.get("market_regime") == "BEAR"):
                print(f"[FlashScan] BEAR regime — suppressing NORMAL ${sym} +{chg1h:.0f}%")
                continue

            msg = (
                f"{conviction_header} {tier} — `${sym}`\n"
                f"🚀 *+{chg1h:.0f}% in 1h* | 5m: {chg5m:+.1f}%\n"
                f"💰 MCap: ${mcap:,.0f} | Liq: ${liq:,.0f}\n"
                f"📊 Vol 1h: ${vol1h:,.0f} | Buys: {buy_pct}%\n"
                f"⏱ Age: {age_h:.1f}h | Price: ${price:.10f}\n"
                f"{rug_label}\n"
                f"{narr['narrative_line']}\n"
                f"{hold_line}"
                f"{pattern_lines}\n"
                f"CA: `{ca}`\n"
                f"🔗 [DexScreener](https://dexscreener.com/solana/{ca})\n"
                f"💡 Size: {size_tip}\n"
                f"{tp_plan}\n"
                f"_Buy on Axiom — bot auto-tracks your entry on-chain_"
            )
            await self.telegram.send(msg)

            # Cooldown + recent alerts memory
            self._alert_cooldown[f"flash:{ca}"] = time.time()
            self._save_cooldown()
            self._add_recent_alert(sym, ca, {
                "score":      min(100, int(chg1h)),
                "urgency":    "URGENT",
                "market_cap": mcap,
                "volume_1h":  vol1h,
                "liquidity":  liq,
                "price_usd":  price,
                "source":     "flash_gain",
            })
            print(f"[FlashScan] 🔥 Alert sent: ${sym} +{chg1h:.0f}% | MCap ${mcap:,.0f}")

            # ── Session stats update ──────────────────────────────────────
            hour_key = datetime.utcnow().strftime("%H")
            self._session_stats["alerts_total"] = self._session_stats.get("alerts_total", 0) + 1
            if high_conv_flash:
                self._session_stats["hc_alerts"]     = self._session_stats.get("hc_alerts", 0) + 1
            else:
                self._session_stats["normal_alerts"] = self._session_stats.get("normal_alerts", 0) + 1
            self._session_stats["last_gem_time"]   = time.time()
            self._session_stats["last_gem_symbol"] = sym
            hourly_a = self._session_stats.setdefault("hourly_alerts", {})
            hourly_a[hour_key] = hourly_a.get(hour_key, 0) + 1
            narr_type = narr.get("narrative_type", "Unknown")
            narr_perf = self._session_stats.setdefault("narrative_perf", {})
            if narr_type not in narr_perf:
                narr_perf[narr_type] = {"total": 0, "wins": 0}
            narr_perf[narr_type]["total"] += 1
            self._save_session_stats()

    # ── Scan loop ──────────────────────────────────────────────
    async def scan_loop(self):
        async with aiohttp.ClientSession() as session:
            birdeye = BirdeyeScanner(session)
            poller  = DiscoverPoller(session)
            while True:
                try:
                    # Primary: Birdeye API (real-time, reliable)
                    tokens = await birdeye.fetch_movers(limit=50)
                    if not tokens:
                        print("[Scan] Birdeye empty, falling back to DexScreener")
                        tokens = await poller.fetch_top_movers(
                            min_volume_5m=100, min_liquidity=2_000
                        )
                    print(f"[Scan] processing {len(tokens)} tokens")

                    # Track best tokens by volume/MCap ratio (report fallback)
                    # Catches active tokens even when full TA pipeline lacks social data
                    # Extra CA-level dedup here as final safety net
                    if tokens:
                        scored = []
                        seen_ca: set[str] = set()
                        now_ts = time.time()
                        for t in tokens:
                            ca_t = t.get("contract_address", "")
                            if not ca_t or ca_t in seen_ca:
                                continue
                            seen_ca.add(ca_t)
                            # Extract metrics first, then filter
                            vol1h    = float(t.get("volume_1h", 0) or 0)
                            vol24h   = float(t.get("volume_24h", 0) or vol1h * 24 or 0)
                            mcap     = float(t.get("market_cap", 1) or 1)
                            pair_age = t.get("pair_created_at", 0) or 0
                            age_h    = (now_ts - pair_age / 1000) / 3600 if pair_age else 0
                            # Skip dead/old tokens
                            if vol24h < 1_000:          # less than $1k volume in 24h — dead token
                                continue
                            if vol1h < 500:             # less than $500 in last hour — not active NOW
                                continue
                            if age_h > 48 and age_h > 0:  # older than 48 hours — not fresh
                                continue
                            vol_ratio = vol1h / mcap if mcap > 0 else 0
                            scored.append({**t, "_vol_ratio": vol_ratio})
                        self._best_scanned = sorted(
                            scored, key=lambda x: x["_vol_ratio"], reverse=True
                        )[:10]

                        # ── Market regime: count pumping vs declining ──────────
                        pumping_count = sum(
                            1 for t in tokens
                            if float(t.get("chg1h", 0) or t.get("price_change_1h", 0) or 0) > 0
                        )
                        total_t  = max(len(tokens), 1)
                        pump_pct = pumping_count / total_t
                        # Require ≥10 tokens scanned before declaring BEAR
                        # (small samples like 7/7 dumping are noise, not regime)
                        # BEAR threshold raised to <20% pumping (was 35%) —
                        # need 80%+ declining before suppressing normal alerts.
                        regime   = ("BULL" if pump_pct >= 0.60 else
                                    "BEAR" if (pump_pct < 0.20 and total_t >= 10) else "MIXED")
                        self._session_stats["pumping"]       = pumping_count
                        self._session_stats["declining"]     = total_t - pumping_count
                        self._session_stats["total_scanned"] = total_t
                        self._session_stats["market_regime"] = regime
                        self._save_session_stats()

                    await asyncio.gather(
                        *[self._process_token(t) for t in tokens],
                        return_exceptions=True,
                    )
                except Exception as exc:
                    print(f"[Scan] error: {exc}")
                await asyncio.sleep(SCAN_INTERVAL_S)

    # ── Position persistence ──────────────────────────────────
    def _save_positions(self):
        """Persist open positions to Redis so they survive restarts."""
        _mem_redis.set_json(POSITIONS_KEY, self._open_positions)

    # ── Position monitor ───────────────────────────────────────
    async def _position_monitor(self):
        """Checks live price for every open position, fires TP/SL alerts, auto-logs extremes."""
        if not self._open_positions:
            return

        TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"
        HEADERS   = {"User-Agent": "AxiomAIAgent/2.0"}
        changed   = False

        async with aiohttp.ClientSession() as session:
            for ca, pos in list(self._open_positions.items()):
                try:
                    async with session.get(
                        TOKEN_URL.format(ca), headers=HEADERS,
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        data = await r.json(content_type=None)
                    pairs = data.get("pairs", [])
                    if not pairs:
                        continue
                    current_price = float(pairs[0].get("priceUsd", 0) or 0)
                    if current_price == 0:
                        continue

                    entry   = pos["entry_price"]
                    pnl     = (current_price - entry) / entry * 100
                    sym     = pos["symbol"]
                    sol_in  = pos.get("sol_amount", 0)
                    sol_pnl = sol_in * (pnl / 100) if sol_in else 0

                    # ── Auto-close: catastrophic loss >-70% (rug) ──────────
                    if pnl <= -70 and not pos.get("auto_closed"):
                        pos["auto_closed"] = True
                        await self.telegram.send(
                            f"☠️ *RUG DETECTED — ${sym}*\n"
                            f"Price crashed *{pnl:+.1f}%* — auto-logged as RUG\n"
                            f"Loss: *{sol_pnl:+.3f} SOL*\n"
                            f"Memory updated to avoid similar setups.\n"
                            f"CA: `{ca}`"
                        )
                        await self._close_position(ca, current_price, "auto_rug")
                        self._session_stats["closed_losses"] = self._session_stats.get("closed_losses", 0) + 1
                        self._save_session_stats()
                        changed = True
                        continue

                    # ── Auto-close: massive win >500% ──────────────────────
                    if pnl >= 500 and not pos.get("auto_closed"):
                        pos["auto_closed"] = True
                        await self.telegram.send(
                            f"🌕 *MEGA WIN — ${sym} +{pnl:.0f}%!*\n"
                            f"Auto-logged to memory as a WIN\n"
                            f"Est. profit: *+{sol_pnl:.3f} SOL*\n"
                            f"CA: `{ca}`"
                        )
                        await self._close_position(ca, current_price, "auto_5x")
                        self._session_stats["closed_wins"] = self._session_stats.get("closed_wins", 0) + 1
                        self._save_session_stats()
                        changed = True
                        continue

                    # ── Stop loss alert: -15% ──────────────────────────────
                    if pnl <= -15 and not pos.get("alerted_sl"):
                        pos["alerted_sl"] = True
                        changed = True
                        await self.telegram.send(
                            f"🛑 *STOP LOSS — ${sym}*\n"
                            f"Down *{pnl:+.1f}%* from entry\n"
                            f"Est. loss: *{sol_pnl:+.3f} SOL*\n"
                            f"➡️ Sell on Axiom now — bot will auto-detect\n"
                            f"CA: `{ca}`"
                        )

                    # ── TP1: +50% ──────────────────────────────────────────
                    elif pnl >= 50 and not pos.get("alerted_tp1"):
                        pos["alerted_tp1"] = True
                        changed = True
                        await self.telegram.send(
                            f"🎯 *TP1 HIT — ${sym} +{pnl:.0f}%*\n"
                            f"Est. profit: *+{sol_pnl:.3f} SOL*\n"
                            f"💡 Sell 50% on Axiom, let the rest ride\n"
                            f"_Bot auto-detects your sell_\n"
                            f"CA: `{ca}`"
                        )

                    # ── TP2: +100% ─────────────────────────────────────────
                    elif pnl >= 100 and not pos.get("alerted_tp2"):
                        pos["alerted_tp2"] = True
                        changed = True
                        await self.telegram.send(
                            f"🚀 *TP2 HIT — ${sym} +{pnl:.0f}%* (2x!)\n"
                            f"Est. profit: *+{sol_pnl:.3f} SOL*\n"
                            f"💡 Pull your initial out — rest is house money\n"
                            f"_Sell on Axiom — bot auto-tracks_\n"
                            f"CA: `{ca}`"
                        )

                    # ── TP3: +200% ─────────────────────────────────────────
                    elif pnl >= 200 and not pos.get("alerted_tp3"):
                        pos["alerted_tp3"] = True
                        changed = True
                        await self.telegram.send(
                            f"💎 *TP3 HIT — ${sym} +{pnl:.0f}%* (3x!)\n"
                            f"Est. profit: *+{sol_pnl:.3f} SOL*\n"
                            f"🏆 Lock it in — sell on Axiom NOW\n"
                            f"_Bot auto-tracks your exit_\n"
                            f"CA: `{ca}`"
                        )

                    # ── Momentum Fade Detector (independent of TP/SL chain) ─
                    # Fires once when position is in profit but momentum reverses.
                    # Catches tops that fixed % targets miss entirely.
                    if pnl >= 20 and not pos.get("alerted_fade") and not pos.get("auto_closed"):
                        pair_d      = pairs[0]
                        txns_5m     = (pair_d.get("txns") or {}).get("m5", {}) or {}
                        buys_now    = int(txns_5m.get("buys",  0) or 0)
                        sells_now   = int(txns_5m.get("sells", 0) or 0)
                        vol_now     = float((pair_d.get("volume") or {}).get("m5", 0) or 0)

                        # Track rolling peak volume for this position
                        if vol_now > pos.get("peak_vol_5m", 0):
                            pos["peak_vol_5m"] = vol_now
                            changed = True

                        peak_vol     = pos.get("peak_vol_5m", vol_now) or vol_now
                        total_txns   = buys_now + sells_now
                        buy_pct_now  = (buys_now / max(total_txns, 1) * 100
                                        if total_txns > 0 else 50.0)

                        buy_flipped  = buy_pct_now < 40          # sellers taking over
                        vol_collapsed = (peak_vol > 0 and
                                         vol_now  < peak_vol * 0.45)  # volume dried up >55%

                        if buy_flipped or vol_collapsed:
                            pos["alerted_fade"] = True
                            changed = True
                            fade_lines = []
                            if buy_flipped:
                                fade_lines.append(
                                    f"📉 Buy pressure: *{buy_pct_now:.0f}%* (sellers dominant)")
                            if vol_collapsed and peak_vol > 0:
                                vol_drop = (1 - vol_now / peak_vol) * 100
                                fade_lines.append(
                                    f"📉 Volume: *-{vol_drop:.0f}%* from peak")
                            await self.telegram.send(
                                f"🚨 *TAKE PROFITS NOW — ${sym}*\n"
                                f"You're up *{pnl:+.1f}%* (+{sol_pnl:.3f} SOL) — "
                                f"*momentum is reversing*\n"
                                + "\n".join(fade_lines) + "\n\n"
                                f"⚡ This move is likely over. Sell on Axiom NOW.\n"
                                f"_Bot auto-detects your exit_\n"
                                f"CA: `{ca}`"
                            )
                            pos["fade_warned_at"] = time.time()
                            print(f"[MomentumFade] ${sym} fading — "
                                  f"buy%={buy_pct_now:.0f} vol_drop={'yes' if vol_collapsed else 'no'}")

                    # ── Escalation: still holding 10min after fade warning ──
                    # If they ignored the first alert and pnl is now dropping, fire once more.
                    fade_warned_at = pos.get("fade_warned_at", 0)
                    if (fade_warned_at > 0
                            and not pos.get("alerted_fade_escalation")
                            and not pos.get("auto_closed")
                            and time.time() - fade_warned_at >= 600   # 10 min later
                            and pnl < pos.get("pnl_at_fade_warn", pnl)):  # pnl dropped
                        pos["alerted_fade_escalation"] = True
                        changed = True
                        await self.telegram.send(
                            f"🔴 *STILL HOLDING ${sym}? GET OUT.*\n"
                            f"You were warned 10 min ago. Now at *{pnl:+.1f}%*\n"
                            f"Every minute you wait, gains disappear.\n"
                            f"SELL ON AXIOM NOW — bot auto-tracks\n"
                            f"CA: `{ca}`"
                        )
                    # Record pnl snapshot when fade first warned (for escalation comparison)
                    if fade_warned_at > 0 and "pnl_at_fade_warn" not in pos:
                        pos["pnl_at_fade_warn"] = pnl
                        changed = True

                except Exception as exc:
                    print(f"[PositionMonitor] error on {ca[:8]}: {exc}")

        if changed:
            self._save_positions()

    # ── Ghost tracker ──────────────────────────────────────────
    async def _ghost_check(self):
        """
        Every 30 min: check ghost-watched tokens (scored 55+ but didn't alert).
        - 3x+ gain   → log as MISSED_WIN (teaches bot what a real gem looks like)
        - -70% dump  → log as DODGED_LOSS (confirms the filter was right)
        - After 6h   → expire regardless
        Builds training data without requiring real trades.
        """
        if not self._ghost_watch:
            return

        TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"
        HEADERS   = {"User-Agent": "AxiomAIAgent/2.0"}
        now       = time.time()
        expired   = []

        async with aiohttp.ClientSession() as session:
            for ca, g in list(self._ghost_watch.items()):
                age_h = (now - g["tracked_at"]) / 3600
                if age_h > 6:
                    expired.append(ca)
                    continue
                try:
                    async with session.get(
                        TOKEN_URL.format(ca), headers=HEADERS,
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as r:
                        data = await r.json(content_type=None)
                    pairs = data.get("pairs", [])
                    if not pairs:
                        continue
                    cur_price = float(pairs[0].get("priceUsd", 0) or 0)
                    if cur_price == 0 or g["first_price"] == 0:
                        continue

                    pnl = (cur_price - g["first_price"]) / g["first_price"] * 100
                    sym = g["symbol"]

                    if pnl >= 200:   # 3x+ — missed gem
                        outcome = "WIN"
                        tag     = "MISSED_WIN"
                        expired.append(ca)
                        print(f"[Ghost] 💰 MISSED WIN: ${sym} +{pnl:.0f}% — logging to memory")
                        await self.telegram.send(
                            f"👻 *GHOST TRACKER — MISSED GEM*\n"
                            f"${sym} pumped *+{pnl:.0f}%* since we saw it\n"
                            f"Score at detection: *{g['score']:.0f}*\n"
                            f"_Bot is logging this as a learning signal._"
                        )
                    elif pnl <= -70:  # rug — good that we didn't enter
                        outcome = "RUG"
                        tag     = "DODGED_LOSS"
                        expired.append(ca)
                        print(f"[Ghost] ☠️ DODGED RUG: ${sym} {pnl:.0f}%")
                    else:
                        continue  # Still developing — keep watching

                    from vector_memory import TradeRecord
                    from datetime import datetime
                    record = TradeRecord(
                        ca=ca, symbol=sym,
                        entry_price=g["first_price"], exit_price=cur_price,
                        hold_hours=age_h,
                        outcome=outcome,
                        pnl_pct=round(pnl, 2),
                        momentum_score=int(g.get("momentum", 0)),
                        vl_ratio_5m=0, ofi=0, rsi_15m=0, sentiment=0,
                        smart_wallet_buys=0, top10_pct=0, holder_count=0,
                        lp_locked=False,
                        confidence=g.get("confidence", 0),
                        setup_type=g.get("setup_type", "GHOST"),
                        market_cap=g.get("market_cap", 0),
                        catalyst_tags=[tag],
                    )
                    self.memory.store(record)

                except Exception as exc:
                    print(f"[Ghost] error on {ca[:8]}: {exc}")

        for ca in expired:
            self._ghost_watch.pop(ca, None)

    # ── My-wallet auto-tracking callbacks ─────────────────────
    async def _on_my_buy(self, wallet: str, ca: str, sol_spent: float):
        """Called when on-chain buy is detected in the user's own wallet."""
        if ca in self._open_positions:
            pos = self._open_positions[ca]
            # Dedup: if position was entered in the last 30s with same amount, it's a
            # duplicate signal (race condition on WS reconnect / bot restart) — silently skip
            age = time.time() - pos.get("entry_time", 0)
            if age < 30 and abs(pos.get("sol_amount", 0) - sol_spent) < 0.0001:
                print(f"[MyWallet] Duplicate BUY signal for {ca[:8]} within {age:.0f}s — skipping")
                return
            # Genuine add-to-position (averaging in)
            pos["sol_amount"] = round(pos.get("sol_amount", 0) + sol_spent, 5)
            self._save_positions()
            sym = pos.get("symbol", ca[:8])
            await self.telegram.send(
                f"🔗 *Added to ${sym}*\n"
                f"+{sol_spent:.4f} SOL → total: {pos['sol_amount']:.4f} SOL tracked"
            )
            return

        # Resolve token info from DexScreener
        symbol = "UNKNOWN"
        price  = 0.0
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    raw = await r.text()
                    if raw and raw.strip():
                        d     = json.loads(raw)
                        pairs = d.get("pairs", [])
                        if pairs:
                            p      = pairs[0]
                            symbol = (p.get("baseToken", {}).get("symbol") or "UNKNOWN").upper()
                            # Always store USD price — /positions also reads priceUsd via
                            # _fetch_price_for_ca, so entry and live prices must match units.
                            price  = float(p.get("priceUsd", 0) or 0)
        except Exception:
            pass

        self._open_positions[ca] = {
            "symbol":       symbol,
            "entry_price":  price,
            "sol_amount":   sol_spent,
            "entry_time":   time.time(),
            "auto_entry":   True,
        }
        self._save_positions()
        print(f"[MyWallet] Auto-entered ${symbol} | {sol_spent:.4f} SOL | @ {price:.8f}")
        await self.telegram.send(
            f"🔗 *Auto-entered: ${symbol}*\n\n"
            f"Detected on-chain buy ✓\n"
            f"SOL in: *{sol_spent:.4f}*\n"
            f"Entry price: `{price:.8f}`\n\n"
            f"_Monitoring — alerts at -15% / +50% / +100% / +200%_"
        )

    async def _on_my_sell(self, wallet: str, ca: str, sol_received: float, pct_sold: float = 1.0):
        """Called when on-chain sell is detected. pct_sold=fraction of position sold (0-1)."""
        pos = self._open_positions.get(ca)
        if not pos:
            await self.telegram.send(
                f"🔗 *On-chain sell detected*\n\n"
                f"CA: `{ca[:20]}...`\n"
                f"SOL received: {sol_received:.4f}\n\n"
                f"_(Position wasn't tracked — use `/log $SYMBOL <pnl%> <sol>` to record it)_"
            )
            return

        sym     = pos.get("symbol", "?")
        sol_in  = pos.get("sol_amount", 0)
        entry_p = pos.get("entry_price", 0)
        hold_h  = (time.time() - pos.get("entry_time", time.time())) / 3600
        hold_s  = f"{int(hold_h*60)}min" if hold_h < 1 else f"{hold_h:.1f}h"

        # ── Partial sell (< 90% of position closed) ───────────
        is_full = pct_sold >= 0.90
        if not is_full:
            sol_chunk    = sol_in * pct_sold           # portion of SOL originally put in
            partial_pnl  = sol_received - sol_chunk
            partial_pct  = (partial_pnl / sol_chunk * 100) if sol_chunk > 0 else 0
            new_sol      = round(sol_in * (1 - pct_sold), 5)
            pnl_emoji    = "✅" if partial_pct > 0 else "❌"

            # Reduce position size, keep it open
            self._open_positions[ca]["sol_amount"] = new_sol
            self._save_positions()

            print(f"[MyWallet] ⚡ PARTIAL EXIT ${sym} | {pct_sold:.0%} sold | {partial_pct:+.1f}%")
            await self.telegram.send(
                f"⚡ *Partial exit detected — ${sym}*\n\n"
                f"Sold: *{pct_sold:.0%}* of position\n"
                f"SOL received: *{sol_received:.4f}*\n"
                f"{pnl_emoji} Partial P&L: *{partial_pct:+.1f}%* ({partial_pnl:+.4f} SOL)\n\n"
                f"Remaining: *{new_sol:.4f} SOL* still open\n"
                f"_Bot updated your position size automatically_"
            )
            return

        # ── Full exit (≥ 90% of position sold) ───────────────
        sol_pnl = sol_received - sol_in
        pnl_pct = (sol_pnl / sol_in * 100) if sol_in > 0 else 0
        outcome = "WIN" if pnl_pct > 2 else ("RUG" if pnl_pct < -50 else "LOSS") if pnl_pct < -2 else "BREAKEVEN"
        emoji   = "✅" if outcome == "WIN" else ("💀" if outcome == "RUG" else "❌")

        # Fetch exit price for memory record
        exit_p = 0.0
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    raw = await r.text()
                    if raw and raw.strip():
                        d = json.loads(raw)
                        p = (d.get("pairs") or [{}])[0]
                        exit_p = float(p.get("priceUsd", 0) or 0)
        except Exception:
            pass
        exit_p = exit_p or (entry_p * (1 + pnl_pct / 100) if entry_p > 0 else 0)

        record = TradeRecord(
            ca=ca, symbol=sym,
            entry_price=entry_p, exit_price=exit_p,
            hold_hours=round(hold_h, 3),
            outcome=outcome, pnl_pct=round(pnl_pct, 2),
            momentum_score=pos.get("momentum_score", 0),
            vl_ratio_5m=0, ofi=0, rsi_15m=0, sentiment=0,
            smart_wallet_buys=0, top10_pct=0, holder_count=0,
            lp_locked=False, confidence=0,
            setup_type=pos.get("setup_type", "auto_tracked"),
            source="auto_wallet",
            catalyst_tags=["auto_exit"],
        )
        self.memory.store(record)
        self._day_trades.append(record)

        # Update session stats
        stats = self._session_stats
        if outcome == "WIN":
            stats["closed_wins"] = stats.get("closed_wins", 0) + 1
        elif outcome in ("LOSS", "RUG"):
            stats["closed_losses"] = stats.get("closed_losses", 0) + 1
        self._save_session_stats()

        del self._open_positions[ca]
        self._save_positions()

        print(f"[MyWallet] 🔴 Full exit ${sym} | {pnl_pct:+.1f}% | {sol_pnl:+.4f} SOL")
        await self.telegram.send(
            f"{emoji} *Auto-exit: ${sym}*\n\n"
            f"SOL in:  {sol_in:.4f}\n"
            f"SOL out: {sol_received:.4f}\n"
            f"P&L: *{pnl_pct:+.1f}%* ({sol_pnl:+.4f} SOL)\n"
            f"Held: {hold_s}\n\n"
            f"📚 Trade logged to memory."
        )
        try:
            if self.learner:
                await self.learner.update_from_feedback(record)
        except Exception:
            pass

    async def _close_position(self, ca: str, exit_price: float, reason: str):
        pos    = self._open_positions.pop(ca, {})
        pnl    = (exit_price - pos["entry_price"]) / pos["entry_price"] * 100
        record = TradeRecord(
            ca=ca, symbol=pos.get("symbol", "?"),
            entry_price=pos["entry_price"], exit_price=exit_price,
            hold_hours=(time.time() - pos["entry_time"]) / 3600,
            outcome="WIN" if pnl > 2 else "LOSS" if pnl < -2 else "BREAKEVEN",
            pnl_pct=round(pnl, 2),
            momentum_score=pos.get("momentum_score", 0),
            vl_ratio_5m=pos.get("vl_ratio_5m", 0),
            ofi=pos.get("ofi", 0), rsi_15m=pos.get("rsi_15m", 0),
            sentiment=pos.get("sentiment", 0), smart_wallet_buys=pos.get("smart_buys", 0),
            top10_pct=pos.get("top10_pct", 0), holder_count=pos.get("holder_count", 0),
            lp_locked=pos.get("lp_locked", False), confidence=pos.get("confidence", 0),
            setup_type=pos.get("setup_type", "?"), catalyst_tags=[reason],
        )
        self.memory.store(record)
        self._day_trades.append(record)
        self._save_positions()  # persist removal to Redis

    # ── Daily performance digest ───────────────────────────────
    async def _performance_digest(self) -> str:
        """Build a short performance summary for the daily report."""
        stats    = self.memory.win_rate_stats()
        total    = stats.get("total_trades", 0)
        win_rate = stats.get("win_rate_pct", 0)
        avg_pnl  = stats.get("avg_pnl_pct", 0)
        open_ct  = len(self._open_positions)
        ghost_ct = len(self._ghost_watch)

        lines = [
            f"📊 *Performance Digest*",
            f"Trades logged: *{total}* | Win rate: *{win_rate:.0f}%* | Avg PnL: *{avg_pnl:+.1f}%*",
        ]

        if open_ct:
            syms = ", ".join(f"${p['symbol']}" for p in self._open_positions.values())
            lines.append(f"📂 Open positions ({open_ct}): {syms}")

        if ghost_ct:
            lines.append(f"👻 Ghost-watching {ghost_ct} token(s) for training data")

        # Show what's working / not working
        if total >= 5:
            all_trades = self.memory._all_trades()
            wins  = [t for t in all_trades if t.get("outcome") == "WIN"]
            rugs  = [t for t in all_trades if t.get("outcome") == "RUG"]

            if wins:
                avg_mom_win = sum(t.get("momentum_score", 0) for t in wins) / len(wins)
                lines.append(f"✅ Winning setups avg momentum: *{avg_mom_win:.0f}/100*")

                # Hold duration breakdown for wins
                hold_hrs = [float(t.get("hold_hours", 0) or 0) for t in wins if t.get("hold_hours", 0)]
                if hold_hrs:
                    avg_h = sum(hold_hrs) / len(hold_hrs)
                    if avg_h < 1:
                        hold_label = f"{int(avg_h*60)}min avg hold"
                    else:
                        hold_label = f"{avg_h:.1f}h avg hold"
                    lines.append(f"⏳ Winning trades held: *{hold_label}* — {'quick scalp style' if avg_h < 1 else ('swing style' if avg_h < 6 else 'narrative/hold style')}")

                # Setup type breakdown
                from collections import Counter
                setup_counts = Counter(t.get("setup_type", "?") for t in wins)
                top_setups   = setup_counts.most_common(3)
                if top_setups:
                    setup_str = " · ".join(f"{s}({c})" for s, c in top_setups)
                    lines.append(f"🏆 Best setups: *{setup_str}*")

            if rugs:
                avg_mom_rug = sum(t.get("momentum_score", 0) for t in rugs) / len(rugs)
                lines.append(f"☠️ Rugged setups avg momentum: *{avg_mom_rug:.0f}/100*")

            # Source breakdown — where are wins coming from?
            if total >= 8:
                from collections import Counter as _Counter
                src_wins = _Counter(t.get("source", "scan") for t in wins)
                src_all  = _Counter(t.get("source", "scan") for t in all_trades)
                src_lines = []
                for src, cnt in src_all.most_common(3):
                    w = src_wins.get(src, 0)
                    wr = round(w / cnt * 100) if cnt else 0
                    src_lines.append(f"{src}: {wr}% WR ({w}W/{cnt-w}L)")
                if src_lines:
                    lines.append("📡 *Win rate by source:* " + " | ".join(src_lines))

        lines.append(f"\n_To train bot: send plain text like `$TOKEN +80%` or `$TOKEN -59% loss`_")
        return "\n".join(lines)

    # ── Daily report ───────────────────────────────────────────
    async def _daily_report(self):
        stats  = self.memory.win_rate_stats()
        digest = await self._performance_digest()

        if not self._candidates:
            # Use volume-based fallback — always show something real
            best = self._best_scanned[:5]
            if best:
                lines = [
                    f"📊 *Axiom Alpha Report* — {datetime.utcnow().strftime('%H:%M UTC')}\n",
                    digest + "\n",
                    "⚡ *Top Active Tokens* (by volume activity)\n",
                ]
                for i, t in enumerate(best, 1):
                    sym   = t.get("symbol", "?")
                    ca    = t.get("contract_address", "")
                    mcap  = float(t.get("market_cap", 0) or 0)
                    liq   = float(t.get("liquidity_usd", 0) or 0)
                    vol1h = float(t.get("volume_1h", 0) or 0)
                    price = float(t.get("price_usd", 0) or 0)
                    stop  = round(price * 0.85, 10)
                    tp1   = round(price * 1.50, 10)
                    lines.append(
                        f"{i}. *${sym}*\n"
                        f"   MCap: ${mcap:,.0f} | Liq: ${liq:,.0f} | Vol1h: ${vol1h:,.0f}\n"
                        f"   Entry: ${price:.8f} | Stop: -15% | TP: +50%\n"
                        f"   CA: `{ca}`\n"
                    )
                lines.append("\n🎓 _Graduation alerts are live 24/7 — best signal_\n")
                lines.append("_Not financial advice. DYOR._")
                await self.telegram.send("\n".join(lines))
            else:
                await self.telegram.send(
                    "📊 *Axiom Alpha Report*\n\n"
                    "Market quiet — no active tokens found this window.\n"
                    "Graduation alerts still live 24/7. 🎓"
                )
            self._candidates = []
            return

        ranked = sorted(
            self._candidates,
            key=lambda x: x["ta"].momentum_score * x["win_rate"],
            reverse=True,
        )[:5]

        blocks = []
        for item in ranked:
            catalyst = await self._catalyst(item["snap"], item["ta"])
            blocks.append(self._format_block(
                item["snap"], item["ta"], item["risk"],
                catalyst=catalyst, win_rate=item["win_rate"],
            ))

        pattern_summary = self.memory.pattern_analysis()
        pending_note = (
            f"\n\n📬 *{stats.get('pending_feedback', 0)} alerts awaiting your outcome.*\n"
            f"_Reply: \"$SYMBOL +80%\" or \"$SYMBOL got rugged\" to train the agent._"
            if stats.get("pending_feedback", 0) > 0 else ""
        )
        report = (
            "# 🔥 AXIOM ALPHA REPORT\n"
            f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"**Setups scanned:** {len(self._candidates)}\n"
            f"**DB win rate:** {stats.get('win_rate_pct', 'N/A')}% "
            f"over {stats.get('total_trades', 0)} trades\n\n---\n"
            + "".join(blocks)
            + f"\n---\n## 🧠 Agent Pattern Analysis\n{pattern_summary}"
            + pending_note
            + "\n\n---\n*Not financial advice.*"
        )

        fname = f"alpha_report_{datetime.utcnow().date()}.md"
        with open(fname, "w") as f:
            f.write(report)

        await self.telegram.send_file(fname, caption="📄 Alpha Report")
        self._candidates = []

    # ── Retrospective ──────────────────────────────────────────
    async def _retrospective(self):
        if self._day_trades:
            await self.learner.run_retrospective(self._day_trades)
            self._day_trades = []

    # ── Catalyst ───────────────────────────────────────────────
    async def _catalyst(self, snap, ta) -> str:
        try:
            r = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                messages=[{"role": "user", "content": (
                    f"2-sentence trading catalyst for ${snap.symbol}. "
                    f"Setup: {ta.setup_type}. Momentum: {ta.momentum_score}/100. "
                    f"Confidence: {ta.confidence:.0%}. Holders: {snap.holder_count}. "
                    f"Be factual. No hype."
                )}],
            )
            return r.content[0].text.strip()
        except Exception:
            return f"Setup: {ta.setup_type} | Momentum: {ta.momentum_score}/100"

    # ── Narrative & Social Scoring ────────────────────────────
    async def _narrative_score(
        self,
        symbol: str,
        name: str,
        socials: list | None = None,  # [{"type": "twitter", "url": "..."}]
        boosts: int = 0,
        chg1h: float = 0,
    ) -> dict:
        """
        Scores the meme/narrative strength of a token.
        Uses Claude Haiku to classify narrative type and trend alignment.
        Checks social presence from DexScreener data.
        Returns:
        {
            "narrative_type": "AI Agent" | "Political" | "Animal Meme" | etc,
            "narrative_strength": 1-10,
            "trending": True/False,
            "social_score": 0-10,
            "has_twitter": bool,
            "has_telegram": bool,
            "boost_count": int,
            "narrative_line": "🧬 AI Agent narrative | Strength 8/10 🔥 | Twitter ✅ Telegram ✅",
            "hold_signal": "NARRATIVE" | "MOMENTUM" | "UNKNOWN",
        }
        """
        # ── Social presence from DexScreener info ─────────────
        socials = socials or []
        has_twitter  = any(s.get("type") == "twitter"  for s in socials)
        has_telegram = any(s.get("type") == "telegram" for s in socials)
        has_discord  = any(s.get("type") == "discord"  for s in socials)

        social_score = 0
        if has_twitter:  social_score += 3
        if has_telegram: social_score += 3
        if has_discord:  social_score += 1
        if boosts > 0:   social_score += min(3, boosts)   # each boost = +1, max +3

        # ── Narrative classification via Haiku ─────────────────
        try:
            r = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                messages=[{"role": "user", "content": (
                    f"Classify this Solana meme coin's narrative. "
                    f"Token: ${symbol} ({name}). "
                    f"Reply with exactly this JSON (no markdown): "
                    f'{{ "type": "<category>", "strength": <1-10>, "trending": <true/false>, "reason": "<15 words max>" }} '
                    f"Categories: AI Agent, Political, Animal Meme, Gaming, DeFi, Celebrity, Evergreen Meme, Seasonal, Unknown. "
                    f"trending=true if this narrative is hot RIGHT NOW in crypto (mid-2026)."
                )}],
            )
            import json as _json
            raw = r.content[0].text.strip()
            # Strip markdown fences if present
            raw = raw.strip("`").strip()
            if raw.startswith("json"): raw = raw[4:].strip()
            data = _json.loads(raw)
            narrative_type     = data.get("type", "Unknown")
            narrative_strength = int(data.get("strength", 5))
            trending           = bool(data.get("trending", False))
            reason             = data.get("reason", "")
        except Exception:
            narrative_type     = "Unknown"
            narrative_strength = 5
            trending           = False
            reason             = ""

        # ── Hold signal: narrative-driven tokens hold longer ──
        narrative_hold_types = {"AI Agent", "Political", "Celebrity", "Evergreen Meme", "Seasonal"}
        hold_signal = "NARRATIVE" if narrative_type in narrative_hold_types else "MOMENTUM"

        # ── Format the narrative line ──────────────────────────
        trend_badge  = " 🔥 *TRENDING*" if trending else ""
        strength_bar = "🔥" if narrative_strength >= 8 else ("⚡" if narrative_strength >= 6 else ("🌱" if narrative_strength >= 4 else "❄️"))
        social_parts = []
        if has_twitter:  social_parts.append("Twitter ✅")
        if has_telegram: social_parts.append("TG ✅")
        if boosts > 0:   social_parts.append(f"Boosts: {boosts}")
        social_str = " | ".join(social_parts) if social_parts else "No socials found"

        narrative_line = (
            f"🧬 *{narrative_type}* narrative{trend_badge} | "
            f"Strength {narrative_strength}/10 {strength_bar}\n"
            f"  📣 {social_str}"
        )
        if reason:
            narrative_line += f"\n  💬 _{reason}_"

        return {
            "narrative_type":     narrative_type,
            "narrative_strength": narrative_strength,
            "trending":           trending,
            "social_score":       social_score,
            "has_twitter":        has_twitter,
            "has_telegram":       has_telegram,
            "boost_count":        boosts,
            "narrative_line":     narrative_line,
            "hold_signal":        hold_signal,
        }

    # ── Format block ───────────────────────────────────────────
    @staticmethod
    def _format_block(snap, ta, risk, catalyst="", win_rate=0.5,
                      pattern: dict | None = None, conviction: str = "NORMAL",
                      narrative: dict | None = None,
                      alert=False) -> str:
        # ── Header ────────────────────────────────────────────────
        if conviction == "HIGH" and alert:
            tag = "🔥 HIGH CONVICTION ALERT"
        elif alert:
            tag = "🚨 URGENT ALERT"
        else:
            tag = "📌 SETUP"

        warns = "\n" + "\n".join(f"  - {w}" for w in risk.warnings) if risk.warnings else ""

        # ── Why confident: list the signals that fired ────────────
        signals = []
        if ta.momentum_score >= 70:  signals.append(f"momentum {ta.momentum_score}/100 🔥")
        elif ta.momentum_score >= 55: signals.append(f"momentum {ta.momentum_score}/100")
        if ta.confidence >= 0.70:     signals.append(f"confidence {ta.confidence:.0%} ✅")
        elif ta.confidence >= 0.55:   signals.append(f"confidence {ta.confidence:.0%}")
        if snap.smart_wallet_buys_1h >= 500: signals.append("smart wallet buys 💸")
        if snap.lp_locked:            signals.append("LP locked 🔒")
        if snap.holder_count >= 50:   signals.append(f"{snap.holder_count} holders 👥")
        why_line = "🧠 *Why confident:* " + " · ".join(signals) if signals else ""

        # ── Pattern history block ─────────────────────────────────
        if pattern and pattern.get("has_data"):
            edge        = pattern.get("edge", 0)
            overall_wr  = pattern.get("overall_wr", 0)
            edge_str    = ""
            if edge >= 15:
                edge_str = f" ⬆️ +{edge:.0f}pts above your avg ({overall_wr:.0f}%) — EDGE CONFIRMED"
            elif edge > 0:
                edge_str = f" (+{edge:.0f}pts above avg)"
            elif edge < 0:
                edge_str = f" (⚠️ {edge:.0f}pts below avg)"
            pattern_block = (
                f"\n{pattern['pattern_line']}{edge_str}\n"
                + (f"  {pattern['example_line']}\n" if pattern.get("example_line") else "")
            )
        elif pattern:
            pattern_block = f"\n{pattern['pattern_line']}\n"
        else:
            pattern_block = ""

        # ── Narrative block ───────────────────────────────────────
        narrative_block = ""
        hold_line       = ""
        if narrative:
            narrative_block = f"\n{narrative['narrative_line']}\n"

        # ── Hold type from pattern history ────────────────────────
        if pattern and pattern.get("avg_hold_hours", 0) > 0:
            hold_line = f"⏳ *Hold style:* {pattern['hold_type']}\n"
        elif narrative and narrative.get("hold_signal") == "NARRATIVE":
            hold_line = "⏳ *Hold style:* 📅 Narrative-driven — consider holding longer if trend stays hot\n"

        # ── Single-line trade plan: one size, one TP, one SL ────────
        hold_type_str = pattern.get("hold_type", "") if pattern and pattern.get("has_data") else ""

        # Size — one SOL amount based on conviction + risk model
        base_size = risk.position_size_usd / 200  # rough SOL estimate (assume ~$200/SOL)
        if conviction == "HIGH":
            sol_size = round(min(base_size * 1.5, 0.5), 2)  # scale up, cap at 0.5 SOL
            hc_tag = " 🔥"
        else:
            sol_size = round(min(base_size, 0.3), 2)
            hc_tag = ""
        sol_size = max(sol_size, 0.02)  # floor
        size_note = f"💡 Size: {sol_size:.2f} SOL{hc_tag}\n"

        # TP % — one target, SL — based on conviction + hold pattern
        if conviction == "HIGH":
            sl_pct = 20
            tp_pct = (80  if "SCALP"     in hold_type_str else
                      150 if "SWING"     in hold_type_str else
                      250 if "HOLD"      in hold_type_str else
                      350 if "MULTI-DAY" in hold_type_str else 100)
        else:
            sl_pct = 25
            tp_pct = (40  if "SCALP"     in hold_type_str else
                      80  if "SWING"     in hold_type_str else
                      120 if "HOLD"      in hold_type_str else
                      200 if "MULTI-DAY" in hold_type_str else 60)
        sell_pct = 70 if conviction == "HIGH" else 100   # HC: keep 30% runner; NORMAL: full exit
        tp_plan  = f"🎯 TP: +{tp_pct}% (sell {sell_pct}%) | 🛑 SL: -{sl_pct}% (exit all)\n"

        return (
            f"\n## {tag} — ${snap.symbol}\n"
            f"**CA:** `{snap.ca}`\n\n"
            f"**Catalyst:** {catalyst}\n"
            f"{narrative_block}"
            f"{hold_line}"
            f"{pattern_block}"
            f"{why_line}\n\n"
            f"{tp_plan}\n"
            f"{size_note}"
            f"| Metric | Value |\n|---|---|\n"
            f"| Entry | ${ta.entry_low:.10f} – ${ta.entry_high:.10f} |\n"
            f"| Stop Loss | ${ta.stop_loss:.10f} |\n"
            f"| TP1 | ${ta.take_profit_1:.10f} |\n"
            f"| TP2 | ${ta.take_profit_2:.10f} |\n"
            f"| TP3 | ${ta.take_profit_3:.10f} |\n"
            f"| Momentum | {ta.momentum_score}/100 |\n"
            f"| Confidence | {ta.confidence:.0%} |\n"
            f"| Win Rate | {win_rate * 100:.0f}% |\n"
            f"| Risk | {risk.risk_level} ({risk.risk_score}/100) |\n"
            f"{warns}\n\n---\n"
        )

    # ── Run ────────────────────────────────────────────────────
    async def run(self):
        print("🤖 Axiom AI Agent v2 starting…")
        self.scheduler.start()

        my_w = self.my_wallet_tracker
        my_w_status = (
            "✅ Auto-wallet tracking active"
            if (my_w and my_w.get_wallet()) else
            "🔗 Auto-wallet: use /mywallet <address> to enable"
            if _MY_WALLET_OK else
            "⚠️ Auto-wallet tracker (disabled)"
        )

        features = [
            "✅ Market scanning (Birdeye)",
            "✅ Axiom Pulse — pump.fun launches + graduations" if _PULSE_OK else "⚠️ Axiom Pulse (disabled)",
            "✅ Early entry detection" if _EARLY_OK else "⚠️ Early entry (disabled)",
            "✅ Interactive bot — type /help" if _INTERACTIVE_OK else "⚠️ Interactive bot (disabled)",
            "✅ Smart wallet tracking" if _WALLET_OK else "⚠️ Smart wallets (disabled)",
            my_w_status,
        ]
        await self.telegram.send("🤖 *Axiom AI Agent v2 online!*\n\n" + "\n".join(features))

        loops = [self.scan_loop(), self._flash_gain_loop()]
        if self.pulse_feed:
            loops.append(self.pulse_feed.start())
        if self.wallet_monitor:
            loops.append(self.wallet_monitor.monitor_loop())
        if self.interactive_bot:
            loops.append(self.interactive_bot.poll_loop())
        if self.my_wallet_tracker:
            loops.append(self.my_wallet_tracker.run())

        await asyncio.gather(*loops)


if __name__ == "__main__":
    asyncio.run(AxiomAgent().run())

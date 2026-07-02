# telegram_interactive.py — Two-way interactive Telegram bot
# Polls for incoming messages and responds to commands + natural language.
# Commands: /report /status /winrate /wallets /early /help
# Natural language: powered by Claude — ask anything about the market or your trades.

import asyncio
import aiohttp
import json
import os
import time
from typing import Optional, Callable
import anthropic
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
BASE_URL     = f"https://api.telegram.org/bot{BOT_TOKEN}"
BIRDEYE_KEY  = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_BASE = "https://public-api.birdeye.so"


class InteractiveBot:
    """
    Polls Telegram for new messages every 2 seconds and dispatches
    commands or natural language queries to the appropriate handler.
    """

    def __init__(self, agent_ref=None):
        self.agent        = agent_ref   # reference to AxiomAgent for live data
        self.claude       = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self._offset      = 0
        self._running     = False
        self._chat_history: list[dict] = []   # rolling conversation context

        # Command registry
        self._commands = {
            "/report":     self._cmd_report,
            "/status":     self._cmd_status,
            "/winrate":    self._cmd_winrate,
            "/wallets":    self._cmd_wallets,
            "/addwallet":  self._cmd_addwallet,
            "/rmwallet":   self._cmd_rmwallet,
            "/log":        self._cmd_log,
            "/track":      self._cmd_track,
            "/clearpos":   self._cmd_clearpos,
            "/mywallet":   self._cmd_mywallet,
            "/sync":       self._cmd_sync,
            "/balance":    self._cmd_balance,
            "/positions":  self._cmd_positions,
            "/check":      self._cmd_check,
            "/early":      self._cmd_early,
            "/scan":       self._cmd_scan,
            "/learn":      self._cmd_learn,
            "/forget":     self._cmd_forget,
            "/help":       self._cmd_help,
            "/start":      self._cmd_help,
        }

    # ── Polling loop ──────────────────────────────────────────
    async def poll_loop(self):
        """Main loop: polls Telegram every 2 seconds for new messages."""
        self._running = True
        print("[InteractiveBot] Polling for messages...")
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    updates = await self._get_updates(session)
                    for update in updates:
                        await self._handle_update(session, update)
                except Exception as exc:
                    print(f"[InteractiveBot] poll error: {exc}")
                await asyncio.sleep(2)

    async def _get_updates(self, session: aiohttp.ClientSession) -> list[dict]:
        url = f"{BASE_URL}/getUpdates"
        params = {"offset": self._offset, "timeout": 1, "allowed_updates": ["message"]}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data    = await r.json()
            updates = data.get("result", [])
            if updates:
                self._offset = updates[-1]["update_id"] + 1
            return updates

    async def _handle_update(self, session: aiohttp.ClientSession, update: dict):
        msg  = update.get("message", {})
        text = msg.get("text", "").strip()
        chat = str(msg.get("chat", {}).get("id", ""))

        # Only respond to the authorised chat
        if chat != CHAT_ID or not text:
            return

        print(f"[InteractiveBot] Received: {text[:80]}")

        import re
        cmd = text.split()[0].lower()

        # 1. Known commands get dispatched FIRST — no ambiguity
        if cmd in self._commands:
            response = await self._commands[cmd](text)
            await self._send(session, response)
            return

        # 2. Unknown slash commands — don't try to interpret, just help
        if text.startswith("/"):
            await self._send(session,
                f"⚠️ Unknown command `{cmd}`\n\nType /help to see all commands.")
            return

        # 3. Trade outcome detection — only for plain-text messages (no slash)
        outcome_resp = await self._detect_trade_outcome(text)
        if outcome_resp:
            await self._send(session, outcome_resp)
            return

        # 4. Auto-detect Solana CA in plain-text message and run /check
        ca_match = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', text)
        if ca_match:
            ca = ca_match.group(0)
            response = await self._cmd_check(f"/check {ca}")
            await self._send(session, response)
            return

        # 5. Natural language fallback
        response = await self._natural_language(text)
        await self._send(session, response)

    # ── Trade outcome detection ───────────────────────────────
    async def _detect_trade_outcome(self, text: str) -> Optional[str]:
        """
        Detects when user reports a trade result and logs it to memory.
        Patterns recognised:
          "$BONK +80%"  "$MOON got rugged"  "took profit on WIF +45%"
          "BONK loss -30%"  "rugged on SAMO"  "WIF hit 3x"
        Returns a confirmation message or None if not a trade report.
        """
        import re
        t = text.strip()

        # ── Guard: strip Solana CA addresses from analysis text ──────────────
        # CA addresses are base58 strings of 32-44 chars. Remove them so
        # embedded characters like "6x" inside a CA can't trigger outcome regex.
        SOLANA_CA_RE = re.compile(r'[1-9A-HJ-NP-Za-km-z]{32,44}')
        t_clean = SOLANA_CA_RE.sub('', t)   # use cleaned text for analysis

        # Extract symbol — prefer $-prefixed (search original text) then ALL-CAPS words
        # Both patterns use group(1) as the capture
        sym_match = re.search(r'\$([A-Za-z]{2,10})', t) or \
                    re.search(r'(?<!\w)([A-Z]{2,10})(?!\w)', t_clean)
        if not sym_match:
            return None

        symbol = sym_match.group(1).upper()

        # Detect outcome type
        outcome = None
        pnl_pct = 0.0

        # Explicit PnL: "+80%" "−30%" "x3" "3x"
        # Use cleaned text (CA stripped) so regex can't fire on CA contents
        pnl_match = re.search(r'([+\-−]?\d+(?:\.\d+)?)\s*%', t_clean)
        # x_match requires the number to be standalone (word boundary on both sides)
        # "3x", "10x", "2.5x" are valid; "6xSipx" or "4xFk" inside a CA are NOT
        x_match   = re.search(r'(?<![A-Za-z0-9])(\d+(?:\.\d+)?)\s*[xX](?![A-Za-z0-9])', t_clean)

        rug_keywords  = ["rug", "rugged", "exit scam", "honeypot", "dumped", "dead"]
        win_keywords  = ["profit", "win", "won", "pumped", "mooned", "hit tp", "took profit", "sold"]
        loss_keywords = ["loss", "lost", "stopped out", "sl hit", "stop loss"]

        # Use CA-stripped text for keyword matching to avoid "pump" at end of CA addresses
        tl = t_clean.lower()

        if any(k in tl for k in rug_keywords):
            outcome = "RUG"
            pnl_match2 = re.search(r'([+\-−]?\d+(?:\.\d+)?)\s*%', t)
            pnl_pct = float((pnl_match2.group(1) or "-90").replace("−", "-")) if pnl_match2 else -90.0
        elif pnl_match:
            pnl_pct = float(pnl_match.group(1).replace("−", "-"))
            outcome = "WIN" if pnl_pct > 2 else "LOSS" if pnl_pct < -2 else "BREAKEVEN"
        elif x_match:
            multiplier = float(x_match.group(1))
            pnl_pct    = (multiplier - 1) * 100
            outcome    = "WIN" if pnl_pct > 0 else "LOSS"
        elif any(k in tl for k in win_keywords):
            outcome = "WIN"
            pnl_pct = 50.0   # unknown PnL — assume moderate win
        elif any(k in tl for k in loss_keywords):
            outcome = "LOSS"
            pnl_pct = -20.0

        if not outcome:
            return None

        # Must have agent + memory to log
        if not self.agent or not hasattr(self.agent, "memory"):
            return (
                f"✅ Got it — ${symbol} *{outcome}* ({pnl_pct:+.1f}%)\n"
                f"⚠️ Memory not available to store this right now."
            )

        # Try to resolve pending alert first (has full entry context)
        record = self.agent.memory.resolve_pending(symbol, outcome, pnl_pct)

        if record is None:
            # No pending alert — create a minimal record from what we know
            from vector_memory import TradeRecord
            from datetime import datetime
            now = datetime.utcnow()

            # Try to recover momentum_score + confidence from bot's recent data
            # so "Winning setups avg momentum" stat is accurate, not stuck at 0/100
            recovered_momentum = 0
            recovered_confidence = 0.0
            recovered_mcap = 0
            if self.agent:
                sym_up = symbol.upper()
                # Check recent alerts first (highest fidelity)
                for item in reversed(getattr(self.agent, "_recent_alerts", [])):
                    if item.get("symbol", "").upper() == sym_up:
                        recovered_momentum   = item.get("momentum_score", 0) or item.get("score", 0)
                        recovered_confidence = item.get("confidence", 0.0)
                        recovered_mcap       = item.get("market_cap", 0)
                        break
                # Fall back to early signals list
                if not recovered_momentum:
                    for sig in getattr(self.agent, "_early_signals", []):
                        if sig.symbol.upper() == sym_up:
                            recovered_momentum   = getattr(sig, "momentum_score", 0)
                            recovered_confidence = getattr(sig, "confidence", 0.0)
                            recovered_mcap       = getattr(sig, "market_cap", 0)
                            break
                # Fall back to best scanned cache
                if not recovered_momentum:
                    for t in getattr(self.agent, "_best_scanned", []):
                        if t.get("symbol", "").upper() == sym_up:
                            recovered_momentum   = t.get("momentum_score", 0) or t.get("score", 0)
                            recovered_confidence = t.get("confidence", 0.0)
                            recovered_mcap       = t.get("market_cap", 0)
                            break

            record = TradeRecord(
                ca=symbol,  # use symbol as CA placeholder
                symbol=symbol,
                entry_price=0, exit_price=0,
                hold_hours=0,
                outcome=outcome,
                pnl_pct=round(pnl_pct, 2),
                momentum_score=recovered_momentum,
                vl_ratio_5m=0, ofi=0,
                rsi_15m=0, sentiment=0, smart_wallet_buys=0,
                top10_pct=0, holder_count=0,
                lp_locked=False,
                confidence=recovered_confidence,
                market_cap=recovered_mcap,
                setup_type="USER_REPORT",
                hour_utc=now.hour,
                day_of_week=now.weekday(),
                source="user_report",
            )
            self.agent.memory.store(record)
            self.agent._day_trades.append(record)

        # Trigger immediate learning
        lesson = ""
        if hasattr(self.agent, "learner"):
            try:
                lesson = await self.agent.learner.update_from_feedback(record)
            except Exception as exc:
                print(f"[InteractiveBot] learner feedback error: {exc}")

        emoji = "🏆" if outcome == "WIN" else "💀" if outcome == "RUG" else "📉"
        stats = self.agent.memory.win_rate_stats()

        reply = (
            f"{emoji} *Logged: ${symbol} → {outcome}* ({pnl_pct:+.1f}%)\n\n"
        )
        if lesson:
            reply += f"💡 *Lesson:* {lesson}\n\n"
        reply += (
            f"📊 *Updated stats:* {stats.get('win_rate_pct', 0):.0f}% win rate "
            f"over {stats.get('total_trades', 0)} trades\n"
            f"_Agent weights updated — scanning smarter now._"
        )
        return reply

    # ── Send helper ───────────────────────────────────────────
    async def _send(self, session: aiohttp.ClientSession, text: str):
        url    = f"{BASE_URL}/sendMessage"
        chunks = [text[i:i+4096] for i in range(0, len(text), 4096)]
        for chunk in chunks:
            payload = {
                "chat_id":                  CHAT_ID,
                "text":                     chunk,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            }
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                resp = await r.json()
                if not resp.get("ok"):
                    print(f"[InteractiveBot] send error: {resp.get('description')}")

    # ── Commands ──────────────────────────────────────────────
    async def _cmd_scan(self, text: str) -> str:
        """
        MARKET RADAR — real-time session intelligence.
        Replaces the old token-list scan (Flash Gem scanner does that 24/7).
        Gives you: market conditions, session stats, hot narrative, strategy advice.
        """
        from datetime import datetime as _dt
        stats  = getattr(self.agent, "_session_stats", {}) if self.agent else {}
        now    = time.time()
        utc_t  = _dt.utcnow().strftime("%H:%M UTC")

        # ── Market regime ─────────────────────────────────────────────────
        regime   = stats.get("market_regime", "MIXED")
        pumping  = stats.get("pumping", 0)
        declining = stats.get("declining", 0)
        scanned  = stats.get("total_scanned", 0)
        if regime == "BULL":
            regime_emoji = "🟢"
        elif regime == "BEAR":
            regime_emoji = "🔴"
        else:
            regime_emoji = "🟡"

        if scanned > 0:
            regime_line = f"{regime_emoji} *{regime}* — {pumping} pump / {declining} dump of {scanned} scanned"
        else:
            regime_line = f"{regime_emoji} *{regime}* — scan running…"

        # ── Session alert counts ──────────────────────────────────────────
        alerts  = stats.get("alerts_total", 0)
        hc_a    = stats.get("hc_alerts", 0)
        norm_a  = stats.get("normal_alerts", 0)
        wins    = stats.get("closed_wins", 0)
        losses  = stats.get("closed_losses", 0)
        total_c = wins + losses

        gem_line = f"⚡ Flash gems today: *{alerts}*"
        if alerts > 0:
            gem_line += f"  ({hc_a} 🔥HC / {norm_a} NORMAL)"
        if total_c > 0:
            wr = int(wins / total_c * 100)
            gem_line += f"\n📊 Closed: {wins}W / {losses}L | WR: *{wr}%*"

        # ── Hot narrative ─────────────────────────────────────────────────
        narr_perf = stats.get("narrative_perf", {})
        hot_narr_line = ""
        if narr_perf:
            # Rank by total alerts, surface the most active narrative
            top = max(narr_perf.items(), key=lambda x: x[1].get("total", 0))
            narr_name, narr_data = top
            narr_total = narr_data.get("total", 0)
            narr_wins  = narr_data.get("wins", 0)
            if narr_total > 0:
                hot_narr_line = f"🔥 Hot narrative: *{narr_name}*  ({narr_wins}W / {narr_total} alerts)"

        # ── Last gem ─────────────────────────────────────────────────────
        last_t  = stats.get("last_gem_time", 0)
        last_s  = stats.get("last_gem_symbol", "")
        if last_t and last_s:
            mins_ago = int((now - last_t) / 60)
            if mins_ago < 60:
                last_line = f"🕐 Last gem: *${last_s}* — {mins_ago}m ago"
            else:
                last_line = f"🕐 Last gem: *${last_s}* — {mins_ago // 60}h ago"
        elif alerts == 0:
            last_line = "🕐 Last gem: none yet this session"
        else:
            last_line = ""

        # ── Bear mode notice ──────────────────────────────────────────────
        bear_line = ""
        if regime == "BEAR":
            bear_line = "⚠️ *BEAR MODE — NORMAL alerts auto-suppressed. HC only.*"

        # ── Strategy advice ───────────────────────────────────────────────
        if regime == "BEAR":
            strategy = "🔴 *Strategy:* BEAR market — stay small, HC signals only"
        elif regime == "BULL":
            strategy = "🟢 *Strategy:* BULL market — conditions favourable, follow alerts"
        elif total_c > 0 and wins / total_c < 0.4:
            strategy = "🟡 *Strategy:* Low WR today — HC alerts only, smaller size"
        else:
            strategy = "🟡 *Strategy:* Mixed conditions — HIGH CONVICTION signals only"

        # ── Assemble ─────────────────────────────────────────────────────
        lines = [
            f"🎯 *MARKET RADAR* — {utc_t}",
            "─────────────────────────",
            regime_line,
            gem_line,
        ]
        if hot_narr_line:
            lines.append(hot_narr_line)
        if last_line:
            lines.append(last_line)
        if bear_line:
            lines.append("")
            lines.append(bear_line)
        lines += ["", strategy, "", "_Flash scanner active 24/7 — gems alert automatically._"]
        return "\n".join(lines)

    async def _fetch_raydium_new_pairs(self) -> str:
        """Fetch recently listed Raydium pairs from DexScreener as fallback."""
        try:
            async with aiohttp.ClientSession() as session:
                # Search DexScreener for newest Raydium/Solana pairs
                async with session.get(
                    "https://api.dexscreener.com/latest/dex/search",
                    params={"q": "raydium"},
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json(content_type=None)

            pairs = data.get("pairs", []) if isinstance(data, dict) else []
            now_ts = time.time()
            sol_pairs = []
            for p in pairs:
                if p.get("chainId") != "solana" or p.get("dexId") != "raydium":
                    continue
                created = p.get("pairCreatedAt", 0) or 0
                age_h   = (now_ts - created / 1000) / 3600 if created else 9999
                # Max 6 hours old — if it's older it's no longer an early entry
                if age_h > 6:
                    continue
                liq    = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                vol1h  = float((p.get("volume") or {}).get("h1", 0) or 0)
                vol5m  = float((p.get("volume") or {}).get("m5", 0) or 0)
                mcap   = float(p.get("marketCap", 0) or 0)
                chg1h  = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                txns5  = p.get("txns", {}).get("m5", {})
                buys5  = int(txns5.get("buys", 0))
                # Quality gates: real liquidity, volume, POSITIVE momentum, active buys
                if liq   < 10_000:  continue   # need real liquidity
                if vol1h < 5_000:   continue   # need active trading
                if buys5 < 3:       continue   # at least 3 buys in last 5m
                if chg1h < 0:       continue   # MUST be going UP, not down
                if mcap  > 5_000_000: continue # max $5M — room to grow
                vol_ratio = vol1h / mcap if mcap > 0 else 0
                sol_pairs.append({**p, "_vol_ratio": vol_ratio})

            # Sort by volume/MCap ratio — highest momentum relative to size first
            sol_pairs.sort(key=lambda p: p["_vol_ratio"], reverse=True)

            if not sol_pairs:
                return (
                    "📭 *No qualifying gems right now*\n\n"
                    "Filters: <6h old · positive 1h momentum · liq >$10k · vol >$5k/h\n\n"
                    "Market is quiet or everything is declining.\n"
                    "Graduation alerts fire automatically 24/7 — best signal.\n"
                    "_Try again in 10–15 minutes._"
                )

            lines = ["⚡ *Fresh Gems* (< 6h old, positive momentum)\n"]
            for i, p in enumerate(sol_pairs[:5], 1):
                base    = p.get("baseToken", {})
                sym     = base.get("symbol", "?")
                ca      = base.get("address", "")
                mcap    = float(p.get("marketCap", 0) or 0)
                liq     = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                vol1h   = float((p.get("volume") or {}).get("h1", 0) or 0)
                vol5m   = float((p.get("volume") or {}).get("m5", 0) or 0)
                chg1h   = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                chg5m   = float((p.get("priceChange") or {}).get("m5", 0) or 0)
                created = p.get("pairCreatedAt", 0)
                age_min = int((now_ts - created / 1000) / 60) if created else 0
                age_str = f"{age_min}m" if age_min < 60 else f"{age_min//60}h {age_min%60}m"
                lines.append(
                    f"{i}. *${sym}* | ⏱ {age_str} old\n"
                    f"   📈 1h: {chg1h:+.1f}% | 5m: {chg5m:+.1f}%\n"
                    f"   MCap: ${mcap:,.0f} | Liq: ${liq:,.0f} | Vol1h: ${vol1h:,.0f}\n"
                    f"   CA: `{ca}`"
                )
            lines.append("\n_Positive momentum, <6h old only. Graduation alerts are live 24/7._")
            return "\n".join(lines)
        except Exception as exc:
            return f"⚠️ Scan error: {exc}"

    # ── Birdeye live scan (primary data source) ────────────────
    async def _fetch_birdeye_movers(self) -> str:
        """
        Birdeye API: real-time Solana tokens sorted by volume acceleration.
        This is the most reliable live data source for Solana memecoins.
        """
        if not BIRDEYE_KEY:
            return "⚠️ BIRDEYE_API_KEY not set — run: railway variable set BIRDEYE_API_KEY=your_key"

        headers = {
            "X-API-KEY": BIRDEYE_KEY,
            "x-chain":   "solana",
            "accept":    "application/json",
        }
        try:
            async with aiohttp.ClientSession() as session:
                # Try trending first (free tier), then tokenlist fallback
                tokens = []
                for endpoint, params in [
                    (f"{BIRDEYE_BASE}/defi/trending_tokens", {"chain": "solana"}),
                    (f"{BIRDEYE_BASE}/defi/tokenlist", {"sort_by": "v24hChangePercent",
                     "sort_type": "desc", "offset": 0, "limit": 30}),
                ]:
                    try:
                        async with session.get(endpoint, headers=headers, params=params,
                                               timeout=aiohttp.ClientTimeout(total=10)) as r:
                            data = await r.json(content_type=None)
                            t = (data.get("data", {}) or {}).get("tokens", []) if isinstance(data, dict) else []
                            if t:
                                tokens = t
                                break
                    except Exception:
                        continue

            if not tokens:
                return "⚠️ Birdeye returned no tokens."

            lines = ["📡 *LIVE Birdeye Scan* — Solana movers right now\n"]
            for i, t in enumerate(tokens[:12], 1):
                ca     = t.get("address", "")
                sym    = t.get("symbol", "?")
                price  = float(t.get("price", 0) or 0)
                mcap   = float(t.get("mc", 0) or 0)
                liq    = float(t.get("liquidity", 0) or 0)
                vol1h  = float(t.get("v1hUSD", 0) or 0)
                chg1h  = float(t.get("v1hChangePercent", 0) or 0)
                chg24h = float(t.get("v24hChangePercent", 0) or 0)
                lines.append(
                    f"{i}. *${sym}*\n"
                    f"   Price: ${price:.8f} | MCap: ${mcap:,.0f}\n"
                    f"   Liq: ${liq:,.0f} | Vol1h: ${vol1h:,.0f}\n"
                    f"   Change: 1h {chg1h:+.1f}% | 24h {chg24h:+.1f}%\n"
                    f"   CA: `{ca}`\n"
                )
            return "\n".join(lines)
        except Exception as exc:
            return f"⚠️ Birdeye error: {exc}"

    # ── pump.fun live trades (secondary source) ────────────────
    async def _fetch_pumpfun(self) -> str:
        """Latest pump.fun launches via DexScreener (pump.fun API often blocks bots)."""
        # pump.fun frontend API blocks non-browser requests → use DexScreener
        try:
            async with aiohttp.ClientSession() as session:
                # Get latest boosted Solana tokens (many are pump.fun)
                async with session.get(
                    "https://api.dexscreener.com/token-profiles/latest/v1",
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    text = await r.text()
                    if not text.strip():
                        return "⚠️ pump.fun data unavailable (DexScreener empty)."
                    items = json.loads(text)

                if not isinstance(items, list) or not items:
                    return "⚠️ No pump.fun tokens found via DexScreener."

                # Filter to Solana only
                sol_items = [
                    x for x in items
                    if x.get("chainId") == "solana" and x.get("tokenAddress")
                ][:8]

                if not sol_items:
                    return "⚠️ No Solana tokens in latest DexScreener profiles."

                lines = ["🔴 *Latest Solana Token Profiles*\n"]
                for i, x in enumerate(sol_items, 1):
                    ca   = x.get("tokenAddress", "")
                    url  = x.get("url", "")
                    desc = (x.get("description") or "")[:60]
                    lines.append(
                        f"{i}. `{ca[:16]}...`\n"
                        f"   {desc}\n"
                        f"   [View]({url})\n"
                    )
                return "\n".join(lines)

        except Exception as exc:
            return f"⚠️ pump.fun error: {exc}"

    async def _send_typing(self):
        pass

    async def _cmd_learn(self, text: str) -> str:
        """Shows what the agent has learned from past trades."""
        if not self.agent or not hasattr(self.agent, "memory"):
            return "⚠️ Memory not connected."
        analysis = self.agent.memory.pattern_analysis()
        # Also show recent lessons from weights
        patterns = []
        if hasattr(self.agent, "learner"):
            patterns = self.agent.learner.weights.get("learned_patterns", [])[-5:]
        if patterns:
            analysis += "\n\n📝 *Recent lessons:*"
            for p in reversed(patterns):
                emoji = "🏆" if p.get("outcome") == "WIN" else "💀" if p.get("outcome") == "RUG" else "📉"
                analysis += (
                    f"\n{emoji} ${p.get('symbol','?')} ({p.get('pnl_pct',0):+.0f}%): "
                    f"_{p.get('lesson','')[:80]}_"
                )
        return analysis

    async def _cmd_forget(self, text: str) -> str:
        """
        /forget $SYMBOL [WIN|LOSS|RUG]
        Removes bad/hallucinated trade records for a symbol from Redis memory.
        Usage:
          /forget $SOLISM        — removes ALL trades logged for SOLISM
          /forget $SOLISM WIN    — removes only fake WIN entries for SOLISM
        """
        import re
        from vector_memory import _redis, TRADES_KEY
        parts = text.split()
        if len(parts) < 2:
            return "Usage: `/forget $SYMBOL [WIN|LOSS|RUG]`"

        sym_raw = parts[1].lstrip("$").upper()
        filter_outcome = parts[2].upper() if len(parts) >= 3 else None

        try:
            raw = _redis._cmd("LRANGE", TRADES_KEY, 0, -1) or []
            import json
            before = len(raw)
            kept = []
            removed = 0
            for item in raw:
                try:
                    rec = json.loads(item) if isinstance(item, str) else item
                except Exception:
                    kept.append(item)
                    continue
                sym = (rec.get("symbol") or rec.get("ca") or "").upper()
                outcome = (rec.get("outcome") or "").upper()
                if sym == sym_raw:
                    if filter_outcome is None or outcome == filter_outcome:
                        removed += 1
                        continue   # drop this record
                kept.append(item)

            if removed == 0:
                return f"🔍 No records found for `${sym_raw}`" + (f" with outcome `{filter_outcome}`" if filter_outcome else "") + "."

            # Rewrite the list
            _redis._cmd("DEL", TRADES_KEY)
            for item in kept:
                _redis._cmd("RPUSH", TRADES_KEY, item if isinstance(item, str) else json.dumps(item))

            return (
                f"🗑️ Removed *{removed}* trade record(s) for `${sym_raw}`"
                + (f" (outcome: `{filter_outcome}`)" if filter_outcome else "")
                + f"\n📚 Memory now has {len(kept)} trades."
            )
        except Exception as exc:
            return f"⚠️ Error clearing memory: {exc}"

    async def _cmd_help(self, text: str) -> str:
        return (
            "🤖 *Axiom AI Agent — Commands*\n\n"
            "/scan — 📡 Live Solana market scan RIGHT NOW\n"
            "/report — Run the alpha report right now\n"
            "/status — Agent health + scan stats\n"
            "/winrate — Your historical win rate\n\n"
            "💰 *Position Tracking*\n"
            "/mywallet <address> — Auto-detect every swap within 2-5s (set this once)\n"
            "/sync — Read wallet token balances NOW and import all current holdings\n"
            "/positions — Live P&L on all open trades\n"
            "/log $SYMBOL <pnl%> <sol> — Manually log a completed trade (rugs, dead tokens)\n\n"
            "👛 *Wallet Tracking*\n"
            "/wallets — Show tracked smart wallets\n"
            "/addwallet <address> — Track a wallet (alerts on every buy)\n"
            "/rmwallet <address> — Remove a tracked wallet\n\n"
            "/check <CA> — 🔍 Safety check any token by contract address\n"
            "/early — Latest early entry signals\n"
            "/learn — 🧠 What the agent has learned from past trades\n"
            "/forget $SYMBOL — 🗑️ Erase bad/hallucinated trade records\n"
            "/help — Show this menu\n\n"
            "💬 Or just *ask me anything* in plain English:\n"
            "_\"What's pumping on Solana right now?\"\n"
            "\"Was the $BONK call a win?\"\n"
            "\"That last trade was a rug, -80%\"_\n\n"
            "📊 *Report trade results (plain text, no slash commands):*\n"
            "_\"$WIF +120%\"  \"$BONK got rugged\"  \"$SOLISM -59% loss\"_\n\n"
            "⚠️ *Set /mywallet once — bot auto-tracks all your swaps*\n"
            "_I remember everything and get smarter with each trade._"
        )

    async def _cmd_status(self, text: str) -> str:
        if self.agent is None:
            return "⚠️ Agent reference not set."
        candidates = len(getattr(self.agent, "_candidates", []))
        positions  = len(getattr(self.agent, "_open_positions", {}))
        weights    = getattr(self.agent, "learner", None)
        version    = weights.weights.get("version", 1) if weights else "?"
        win_rate   = weights.weights.get("last_win_rate", 0.0) if weights else 0.0
        return (
            "📡 *Agent Status*\n\n"
            f"✅ Online and scanning\n"
            f"🔍 Candidates today: *{candidates}*\n"
            f"📊 Open positions: *{positions}*\n"
            f"🧠 Brain version: *v{version}*\n"
            f"🏆 Learned win rate: *{win_rate:.1f}%*\n"
            f"⏰ Next report: see schedule"
        )

    async def _cmd_report(self, text: str) -> str:
        """
        INTELLIGENCE BRIEF — smart session analysis.
        Replaces the old daily_report() call (that fires automatically at 9 UTC + 18 UTC).
        Shows: alert breakdown, setup performance, narrative WR, peak hours, open positions, recommendation.
        """
        if self.agent is None:
            return "⚠️ Agent not available."

        from datetime import datetime as _dt
        stats   = getattr(self.agent, "_session_stats", {})
        open_pos = getattr(self.agent, "_open_positions", {})
        now_ts  = time.time()

        alerts     = stats.get("alerts_total", 0)
        hc_alerts  = stats.get("hc_alerts", 0)
        norm_alerts = stats.get("normal_alerts", 0)
        wins       = stats.get("closed_wins", 0)
        losses     = stats.get("closed_losses", 0)
        total_c    = wins + losses
        regime     = stats.get("market_regime", "MIXED")

        # ── Header ───────────────────────────────────────────────────────
        lines = [
            "🧠 *INTELLIGENCE BRIEF*",
            "─────────────────────────────",
        ]

        # ── Alert totals ─────────────────────────────────────────────────
        if alerts == 0:
            lines.append("📊 No alerts fired yet this session.")
        else:
            lines.append(f"📊 Today: *{alerts} alerts* — {hc_alerts} 🔥HC / {norm_alerts} NORMAL")

        # ── Closed trade results ──────────────────────────────────────────
        if total_c > 0:
            wr = int(wins / total_c * 100)
            wl_emoji = "🏆" if wr >= 50 else "⚠️"
            lines.append(f"{wl_emoji} Closed: {wins}W / {losses}L | WR: *{wr}%*")
        elif alerts > 0:
            lines.append("📋 No closed positions yet — positions still open or not logged")

        # ── Setup breakdown (HC vs NORMAL) ───────────────────────────────
        if hc_alerts > 0 or norm_alerts > 0:
            lines.append("")
            lines.append("*📋 Setup breakdown:*")
            if hc_alerts > 0:
                lines.append(f"  🔥 HIGH CONVICTION: {hc_alerts} alerts")
            if norm_alerts > 0:
                bear_note = " _(suppressed in BEAR)_" if regime == "BEAR" and norm_alerts == 0 else ""
                lines.append(f"  ⚡ NORMAL: {norm_alerts} alerts{bear_note}")

        # ── Narrative performance ─────────────────────────────────────────
        narr_perf = stats.get("narrative_perf", {})
        if narr_perf:
            lines.append("")
            lines.append("*🧬 Narrative activity today:*")
            # Sort by total alerts descending
            sorted_narr = sorted(narr_perf.items(), key=lambda x: x[1].get("total", 0), reverse=True)
            for narr_name, data in sorted_narr[:5]:  # top 5 narratives
                n_total = data.get("total", 0)
                n_wins  = data.get("wins", 0)
                if n_total == 0:
                    continue
                if n_wins > 0:
                    wr_str = f"{int(n_wins/n_total*100)}% WR"
                    bullet = "✅"
                else:
                    wr_str = f"{n_total} alerts"
                    bullet = "⚪"
                lines.append(f"  {bullet} *{narr_name}* — {n_wins}W / {n_total} alerts  _{wr_str}_")

        # ── Peak alert hours ──────────────────────────────────────────────
        hourly = stats.get("hourly_alerts", {})
        if hourly:
            peak_h, peak_cnt = max(hourly.items(), key=lambda x: x[1])
            lines.append("")
            lines.append(f"⏰ Peak alert hour: *{peak_h}:00 UTC* ({peak_cnt} gems)")

        # ── Open positions summary ────────────────────────────────────────
        if open_pos:
            lines.append("")
            lines.append(f"💼 *Open positions: {len(open_pos)}*")
            for ca, pos in list(open_pos.items())[:3]:  # show up to 3
                sym_p = pos.get("symbol", "?")
                sol_p = pos.get("sol_amount", 0)
                lines.append(f"  • ${sym_p} — {sol_p:.2f} SOL in  |  CA: `{ca[:8]}…`")
            if len(open_pos) > 3:
                lines.append(f"  _…and {len(open_pos) - 3} more_")

        # ── Recommendation ────────────────────────────────────────────────
        lines.append("")
        lines.append("*💡 Recommendation:*")
        if alerts == 0:
            lines.append("  → Flash scanner is running — no gems yet. Stay patient.")
        elif regime == "BEAR":
            lines.append("  → BEAR market active — HC only, tighter size, wait for regime shift")
        elif total_c > 0 and wins / total_c < 0.40:
            lines.append("  → WR below 40% — HC signals only, skip NORMAL until it improves")
        elif hc_alerts > 0 and norm_alerts > 0:
            lines.append("  → Focus on HC alerts — they carry your real edge")
        else:
            lines.append("  → Stay disciplined, follow HIGH CONVICTION signals")

        lines.append("")
        lines.append("_Auto-reports fire at 9 UTC (morning) + 18 UTC (evening)_")
        return "\n".join(lines)

    async def _cmd_winrate(self, text: str) -> str:
        if self.agent is None:
            return "⚠️ Agent not available."
        stats = self.agent.memory.win_rate_stats()
        if stats.get("total_trades", 0) == 0:
            return "📭 No trades in memory yet. Make some trades and report results to me!"
        return (
            "📊 *Win Rate Stats*\n\n"
            f"Total trades: *{stats['total_trades']}*\n"
            f"Win rate: *{stats['win_rate_pct']}%*\n"
            f"Rug rate: *{stats.get('rug_rate_pct', 0)}%*\n"
            f"Avg PnL: *{stats.get('avg_pnl_pct', 0):+.1f}%*\n"
            f"Avg momentum on wins: *{stats.get('avg_momentum_wins', 0):.0f}/100*\n"
            f"Avg confidence on wins: *{stats.get('avg_confidence_wins', 0):.0%}*"
        )

    # ── Position tracking commands ────────────────────────────
    async def _fetch_price_for_ca(self, ca: str) -> float:
        """Fetch current USD price for a token CA.
        Tries DexScreener first (Raydium/Orca), then Birdeye fallback
        (covers Pump AMM tokens not yet indexed by DexScreener).
        """
        async with aiohttp.ClientSession() as session:
            # ── 1. DexScreener ────────────────────────────────────
            try:
                async with session.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json(content_type=None)
                pairs = data.get("pairs", [])
                if pairs:
                    return float(pairs[0].get("priceUsd", 0) or 0)
            except Exception:
                pass

            # ── 2. Birdeye fallback (Pump AMM / unlisted tokens) ──
            if BIRDEYE_KEY:
                try:
                    async with session.get(
                        f"{BIRDEYE_BASE}/defi/price",
                        params={"address": ca},
                        headers={"X-API-KEY": BIRDEYE_KEY, "x-chain": "solana"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as r:
                        data = await r.json(content_type=None)
                    price = float((data.get("data") or {}).get("value") or 0)
                    if price:
                        return price
                except Exception:
                    pass

        return 0.0

    async def _resolve_ca(self, symbol: str) -> str:
        """Find CA for a symbol from recent alerts, early signals, or best scanned."""
        sym = symbol.upper().lstrip("$")
        if self.agent:
            for src in [
                getattr(self.agent, "_recent_alerts", []),
            ]:
                for item in reversed(src):
                    if item.get("symbol", "").upper() == sym:
                        return item.get("ca", "")
            for sig in getattr(self.agent, "_early_signals", []):
                if sig.symbol.upper() == sym:
                    return sig.ca
            for t in getattr(self.agent, "_best_scanned", []):
                if t.get("symbol", "").upper() == sym:
                    return t.get("contract_address", "")
        return ""

    async def _cmd_enter(self, text: str) -> str:
        """Log a new position.
        Accepts:
          /enter <CA> <sol_amount>          — CA first (easiest after /check)
          /enter $SYMBOL <sol_amount>       — symbol lookup
          /enter $SYMBOL <sol_amount> <CA>  — explicit
        """
        if self.agent is None:
            return "⚠️ Agent not available."
        parts = text.strip().split()

        if len(parts) < 3:
            return (
                "Usage: `/enter <CA> <sol_amount>`\n"
                "Example: `/enter A7YFMg...pump 0.08`\n\n"
                "Or by symbol: `/enter $HPUMP 0.5`\n"
                "Bot will alert you at -15%, +50%, +100%, +200%."
            )

        # Detect if first arg is a raw CA (32+ chars, no $ prefix)
        _first = parts[1]
        _is_ca = len(_first) >= 32 and not _first.startswith("$")

        if _is_ca:
            # /enter <CA> <sol_amount>  — the easiest path
            ca = _first
            try:
                sol_amount = float(parts[2])
            except ValueError:
                return "⚠️ SOL amount must be a number, e.g. `/enter A7YFMg...pump 0.08`"
            # Resolve symbol from DexScreener
            symbol = "UNKNOWN"
            try:
                import json as _json
                async with aiohttp.ClientSession() as _s:
                    async with _s.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                        headers={"User-Agent": "AxiomAIAgent/2.0"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as _r:
                        _raw = await _r.text()
                        if _raw and _raw.strip():
                            _d = _json.loads(_raw)
                            _pairs = _d.get("pairs", [])
                            if _pairs:
                                symbol = (_pairs[0].get("baseToken", {})
                                          .get("symbol", "UNKNOWN")).upper()
            except Exception:
                pass
        else:
            # /enter $SYMBOL <sol_amount> [ca]
            symbol = _first.upper().lstrip("$")
            try:
                sol_amount = float(parts[2])
            except ValueError:
                return "⚠️ SOL amount must be a number, e.g. `/enter $HPUMP 0.5`"
            ca = parts[3] if len(parts) >= 4 else await self._resolve_ca(symbol)
            if not ca:
                return (
                    f"⚠️ Can't find CA for *${symbol}*.\n"
                    f"Use the CA directly:\n"
                    f"`/enter <contract_address> {sol_amount}`"
                )

        price = await self._fetch_price_for_ca(ca)
        if price == 0:
            return f"⚠️ Could not fetch price for `{ca[:16]}...` — check the CA and try again."

        # Look up confidence score from recent alerts for sizing guidance
        conf_score = 0.0
        recent_data = {}
        if self.agent:
            for item in reversed(getattr(self.agent, "_recent_alerts", [])):
                if item.get("symbol", "").upper() == symbol:
                    conf_score = float(item.get("confidence", 0) or item.get("score", 0) or 0)
                    recent_data = item
                    break

        # Sizing guidance based on confidence
        if conf_score >= 90:
            size_tip = "🔥 *High conviction* — up to 0.5 SOL reasonable"
            risk_tag = "HIGH CONFIDENCE"
        elif conf_score >= 75:
            size_tip = "✅ *Good signal* — 0.15–0.25 SOL suggested"
            risk_tag = "MODERATE CONFIDENCE"
        elif conf_score >= 60:
            size_tip = "⚠️ *Speculative* — keep it to 0.05–0.1 SOL"
            risk_tag = "LOW CONFIDENCE"
        elif conf_score > 0:
            size_tip = "🚨 *Very risky* — micro-size only (0.02–0.05 SOL)"
            risk_tag = "VERY SPECULATIVE"
        else:
            size_tip = "ℹ️ No scan data for this token — size conservatively"
            risk_tag = "UNSCORED"

        self.agent._open_positions[ca] = {
            "symbol":      symbol,
            "ca":          ca,
            "entry_price": price,
            "sol_amount":  sol_amount,
            "entry_time":  time.time(),
            "confidence":  conf_score,
            "momentum_score": recent_data.get("score", 0),
            "setup_type":  recent_data.get("setup_type", "MANUAL"),
            "alerted_sl":  False,
            "alerted_tp1": False,
            "alerted_tp2": False,
            "alerted_tp3": False,
        }
        # Persist to Redis so positions survive restarts
        from vector_memory import _redis as _mem_redis
        _mem_redis.set_json("axiom:positions", self.agent._open_positions)

        # Warn if they're over-sizing for the confidence level
        sizing_warn = ""
        if conf_score < 60 and sol_amount > 0.2:
            sizing_warn = (
                f"\n\n⚠️ *Sizing warning:* You entered {sol_amount} SOL on a "
                f"{risk_tag} setup. Consider smaller size to manage risk."
            )

        return (
            f"✅ *Position logged: ${symbol}*\n\n"
            f"Entry: `${price:.10f}`\n"
            f"SOL in: *{sol_amount} SOL*\n"
            f"Signal: *{risk_tag}*\n"
            f"{size_tip}\n"
            f"CA: `{ca}`\n\n"
            f"🔔 Auto-alerts at:\n"
            f"  🛑 -15% → Stop loss (exit!)\n"
            f"  🎯 +50% → Take 50% profit\n"
            f"  🚀 +100% → Pull initial, ride rest\n"
            f"  💎 +200% → Lock it all in\n"
            f"  ☠️ -70% → Auto-logged as RUG{sizing_warn}\n\n"
            f"_Check anytime with /positions_"
        )

    async def _cmd_exit(self, text: str) -> str:
        """Close a position — full or partial.
        Usage:
          /exit $SYMBOL          — close 100%
          /exit $SYMBOL 50%      — close 50%, keep rest open
          /exit $SYMBOL 50       — same (% optional)
          /exit <CA>             — by contract address
          /exit <CA> 50%         — partial by CA
        """
        if self.agent is None:
            return "⚠️ Agent not available."
        parts = text.strip().split()
        if len(parts) < 2:
            return (
                "Usage: `/exit $SYMBOL` or `/exit $SYMBOL 50%`\n"
                "Example: `/exit $HPUMP 50%`  — sells half, keeps rest tracked"
            )

        identifier = parts[1]

        # Parse optional percentage / full-close flag
        exit_pct = 100.0  # default: full close
        if len(parts) >= 3:
            raw_pct = parts[2].rstrip("%")
            try:
                exit_pct = float(raw_pct)
                if exit_pct <= 0 or exit_pct > 100:
                    exit_pct = 100.0
            except ValueError:
                exit_pct = 100.0

        # Resolve position — accept CA directly or symbol lookup
        found_ca = None
        _is_ca_id = len(identifier) >= 32 and not identifier.startswith("$")

        if _is_ca_id:
            if identifier in self.agent._open_positions:
                found_ca = identifier
        else:
            symbol_lookup = identifier.upper().lstrip("$")
            for ca, pos in self.agent._open_positions.items():
                if pos.get("symbol", "").upper() == symbol_lookup:
                    found_ca = ca
                    break

        if not found_ca:
            sym_display = identifier.lstrip("$")
            open_syms = [p.get("symbol","?") for p in self.agent._open_positions.values()]
            sym_list  = ", ".join(f"${s}" for s in open_syms) if open_syms else "none"
            return (
                f"⚠️ No open position found for *{identifier}*.\n"
                f"Open positions: {sym_list}\n"
                f"Use /positions to see all."
            )

        pos    = self.agent._open_positions[found_ca]
        symbol = pos.get("symbol", "?")
        price  = await self._fetch_price_for_ca(found_ca)
        if price == 0:
            if exit_pct >= 100:
                del self.agent._open_positions[found_ca]
            return f"✅ Closed *${symbol}* (could not fetch final price)."

        entry   = pos["entry_price"]
        pnl_pct = (price - entry) / entry * 100
        sol_in  = pos.get("sol_amount", 0)
        sol_exited = sol_in * (exit_pct / 100)
        sol_pnl    = sol_exited * (pnl_pct / 100)
        hold_h     = (time.time() - pos["entry_time"]) / 3600

        from vector_memory import _redis as _mem_redis

        if exit_pct >= 100:
            # Full close — remove position entirely
            del self.agent._open_positions[found_ca]
            _mem_redis.set_json("axiom:positions", self.agent._open_positions)
            emoji = "🏆" if pnl_pct > 10 else "📉" if pnl_pct < -5 else "➖"
            return (
                f"{emoji} *Closed: ${symbol}*\n\n"
                f"Entry: `${entry:.10f}`\n"
                f"Exit:  `${price:.10f}`\n"
                f"P&L: *{pnl_pct:+.1f}%* ({sol_pnl:+.3f} SOL)\n"
                f"Held: {hold_h:.1f}h\n\n"
                f"_Report the result to train the agent:_\n"
                f"`${symbol} {pnl_pct:+.0f}%`"
            )
        else:
            # Partial close — reduce sol_amount, keep position open
            sol_remaining = sol_in * (1 - exit_pct / 100)
            pos["sol_amount"] = sol_remaining
            # Reset TP flags so future TPs can fire on the remaining position
            pos.pop("alerted_tp1", None)
            pos.pop("alerted_tp2", None)
            pos.pop("alerted_tp3", None)
            pos.pop("alerted_fade", None)
            pos.pop("fade_warned_at", None)
            _mem_redis.set_json("axiom:positions", self.agent._open_positions)
            emoji = "🏆" if pnl_pct > 10 else "📊"
            return (
                f"{emoji} *Partial exit: ${symbol} ({exit_pct:.0f}%)*\n\n"
                f"Sold: *{exit_pct:.0f}%* of position\n"
                f"P&L on sold portion: *{pnl_pct:+.1f}%* ({sol_pnl:+.3f} SOL)\n"
                f"Remaining: *{sol_remaining:.3f} SOL* still tracked\n\n"
                f"🔔 Bot continues monitoring the rest.\n"
                f"_Full sell on-chain will be auto-detected._"
            )

    async def _cmd_track(self, text: str) -> str:
        """Manually register or fix a position's SOL amount.
        Use when auto-wallet doubled an entry, or when you need to re-register
        a position the bot lost track of.
        Usage:
          /track $SYMBOL <sol_amount>        — register by symbol (fetches CA from DexScreener)
          /track <CA> <sol_amount>           — register by contract address (most reliable)
        """
        if self.agent is None:
            return "⚠️ Agent not available."

        parts = text.strip().split()
        if len(parts) < 3:
            return (
                "Usage: `/track $SYMBOL <sol_amount>`\n"
                "or:    `/track <CA> <sol_amount>`\n\n"
                "Example: `/track $ANSEMHOOD 0.0527`\n\n"
                "Use this to fix a doubled position or re-register one the bot missed."
            )

        _first = parts[1].strip()
        _is_ca = len(_first) >= 32 and not _first.startswith("$")

        try:
            sol_amount = float(parts[2])
        except ValueError:
            return "⚠️ SOL amount must be a number."

        # ── Resolve CA and symbol ──────────────────────────────
        if _is_ca:
            ca = _first
            symbol = "UNKNOWN"
            price  = 0.0
            try:
                import json as _json
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                        headers={"User-Agent": "AxiomAIAgent/2.0"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as r:
                        raw = await r.text()
                        if raw and raw.strip():
                            d = _json.loads(raw)
                            p = (d.get("pairs") or [{}])[0]
                            symbol = (p.get("baseToken", {}).get("symbol") or "UNKNOWN").upper()
                            price  = float(p.get("priceUsd", 0) or 0)  # USD — must match _fetch_price_for_ca units
            except Exception:
                pass
        else:
            symbol = _first.upper().lstrip("$")
            ca     = None
            price  = 0.0
            # Check if already tracked — reuse CA
            positions = getattr(self.agent, "_open_positions", {})
            for _ca, pos in positions.items():
                if pos.get("symbol", "").upper() == symbol:
                    ca = _ca
                    price = pos.get("entry_price", 0)
                    break
            # If not tracked, resolve from DexScreener
            if not ca:
                try:
                    import json as _json
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            f"https://api.dexscreener.com/solana/search/?q={symbol}",
                            headers={"User-Agent": "AxiomAIAgent/2.0"},
                            timeout=aiohttp.ClientTimeout(total=8),
                        ) as r:
                            raw = await r.text()
                            if raw and raw.strip():
                                d     = _json.loads(raw)
                                pairs = d.get("pairs", [])
                                if pairs:
                                    p      = pairs[0]
                                    ca     = p.get("baseToken", {}).get("address", "")
                                    price  = float(p.get("priceUsd", 0) or 0)  # USD — must match _fetch_price_for_ca units
                except Exception:
                    pass

        if not ca:
            return f"⚠️ Couldn't resolve CA for ${symbol}. Try `/track <CA> {sol_amount}` instead."

        # ── Write/overwrite position ───────────────────────────
        existing = self.agent._open_positions.get(ca, {})
        old_sol  = existing.get("sol_amount", 0)

        self.agent._open_positions[ca] = {
            "symbol":       symbol,
            "entry_price":  price or existing.get("entry_price", 0),
            "sol_amount":   sol_amount,
            "entry_time":   existing.get("entry_time", time.time()),
            "auto_entry":   existing.get("auto_entry", False),
            # Preserve any existing TP/SL flags
            **{k: v for k, v in existing.items()
               if k not in ("symbol", "entry_price", "sol_amount", "entry_time", "auto_entry")},
        }
        _mem_redis.set_json("axiom:positions", self.agent._open_positions)

        action = f"Updated (was {old_sol:.4f} SOL)" if old_sol else "Registered"
        return (
            f"✅ *${symbol} position {action}*\n\n"
            f"SOL tracked: *{sol_amount:.4f}*\n"
            f"Entry price: `{price:.8f}`\n\n"
            f"_Bot will alert at -15% / +50% / +100% / +200%_"
        )

    async def _cmd_clearpos(self, text: str) -> str:
        """Force-remove a position from tracking without logging a trade.
        Use when a position is stuck / has bad data and needs to be wiped clean.
        Usage:
          /clearpos $SYMBOL   — remove by symbol
          /clearpos <CA>      — remove by contract address
          /clearpos all       — wipe ALL open positions (emergency reset)
        """
        if self.agent is None:
            return "⚠️ Agent not available."

        parts = text.strip().split()
        if len(parts) < 2:
            return (
                "Usage: `/clearpos $SYMBOL` or `/clearpos <CA>`\n\n"
                "Force-removes a stuck position without logging it as a trade.\n"
                "Use `/log` instead if you want the result saved to memory."
            )

        arg = parts[1].strip()
        positions: dict = getattr(self.agent, "_open_positions", {})

        # ── Wipe all ──────────────────────────────────────────
        if arg.lower() == "all":
            count = len(positions)
            self.agent._open_positions.clear()
            _mem_redis.set_json("axiom:positions", {})
            return f"🗑 *All {count} position(s) cleared.* Start fresh."

        # ── Remove by CA ──────────────────────────────────────
        _is_ca = len(arg) >= 32 and not arg.startswith("$")
        if _is_ca:
            ca = arg
            if ca in positions:
                sym = positions[ca].get("symbol", ca[:8])
                del self.agent._open_positions[ca]
                _mem_redis.set_json("axiom:positions", self.agent._open_positions)
                return f"🗑 *${sym}* removed from tracking."
            return f"⚠️ CA `{ca[:16]}...` not found in open positions."

        # ── Remove by symbol (fuzzy — strips $, case-insensitive, strips spaces) ──
        target = arg.upper().lstrip("$").strip()
        found_ca = None
        found_sym = None
        for ca, pos in positions.items():
            stored = pos.get("symbol", "").upper().strip()
            if stored == target or target in stored or stored in target:
                found_ca  = ca
                found_sym = pos.get("symbol", ca[:8])
                break

        if found_ca:
            del self.agent._open_positions[found_ca]
            _mem_redis.set_json("axiom:positions", self.agent._open_positions)
            remaining = len(self.agent._open_positions)
            return (
                f"🗑 *${found_sym}* removed from tracking.\n\n"
                f"Open positions remaining: {remaining}\n\n"
                f"_Use `/log` next time to also save the trade result to memory._"
            )

        # ── Not found — show what's tracked ───────────────────
        syms = [p.get("symbol", "?") for p in positions.values()]
        return (
            f"⚠️ `${target}` not found in open positions.\n\n"
            f"Currently tracked: {', '.join('$'+s for s in syms) if syms else 'none'}"
        )

    async def _cmd_mywallet(self, text: str) -> str:
        """Set or show your own trading wallet for auto-tracking.
        Usage:
          /mywallet                 — show current wallet
          /mywallet <address>       — set wallet (enables auto-tracking)
          /mywallet remove          — stop auto-tracking
        """
        if self.agent is None:
            return "⚠️ Agent not available."

        tracker = getattr(self.agent, "my_wallet_tracker", None)
        if tracker is None:
            return "⚠️ Auto-wallet module not available — check Railway logs."

        parts = text.strip().split()

        # ── Show current ──────────────────────────────────────
        if len(parts) == 1:
            wallet = tracker.get_wallet()
            if wallet:
                return (
                    f"🔗 *Auto-tracking your wallet:*\n\n"
                    f"`{wallet}`\n\n"
                    f"Every on-chain swap is detected within 2-5 seconds and auto-logged.\n\n"
                    f"• `/mywallet <new_address>` — change wallet\n"
                    f"• `/mywallet remove` — stop auto-tracking"
                )
            else:
                return (
                    f"📭 *No wallet set.*\n\n"
                    f"Set your Solana trading wallet and the bot will auto-detect every swap:\n\n"
                    f"`/mywallet <your_wallet_address>`\n\n"
                    f"Once set, you'll never need to type `/enter` or `/exit` manually again —\n"
                    f"the bot tracks positions the moment they hit the chain."
                )

        arg = parts[1].strip()

        # ── Remove ────────────────────────────────────────────
        if arg.lower() in ("remove", "clear", "stop", "off", "none"):
            tracker.set_wallet("")
            return "🗑 Wallet removed. Auto-tracking stopped.\n\nUse `/mywallet <address>` to re-enable."

        # ── Set wallet ────────────────────────────────────────
        if len(arg) < 32:
            return (
                "⚠️ That doesn't look like a valid Solana address.\n\n"
                "It should be 43-44 characters long. Copy it directly from Phantom or Axiom."
            )

        old = tracker.get_wallet() or ""
        tracker.set_wallet(arg)

        changed_note = f"\n_(Was: `{old[:8]}...`)_" if old and old != arg else ""
        return (
            f"✅ *Wallet set!*\n\n"
            f"`{arg}`\n\n"
            f"Auto-tracking is live 🟢\n"
            f"The bot will now detect your buys and sells on-chain within 2-5 seconds "
            f"and auto-log positions — no more `/enter` or `/exit` needed."
            f"{changed_note}"
        )

    async def _cmd_log(self, text: str) -> str:
        """Log a completed trade. Also closes the open position if one exists.
        Usage:
          /log $SYMBOL <pnl%> [sol_amount]
          /log $BULLATLAS +119 0.04
          /log $APE -34 0.031
          /log $APE -34%          ← sol_amount optional if position was tracked
        """
        if self.agent is None:
            return "⚠️ Agent not available."

        parts = text.strip().split()
        if len(parts) < 3:
            return (
                "📝 *Log a completed trade*\n\n"
                "Usage: `/log $SYMBOL <pnl%> [sol_amount]`\n\n"
                "Examples:\n"
                "`/log $BULLATLAS +119 0.04` — won 119% on 0.04 SOL\n"
                "`/log $APE -34 0.031`       — lost 34% on 0.031 SOL\n\n"
                "If the bot was tracking the position, sol_amount is optional."
            )

        symbol = parts[1].upper().lstrip("$")

        try:
            pnl_pct = float(parts[2].replace("%", ""))
        except ValueError:
            return "⚠️ P&L must be a number, e.g. `-34` or `+119`"

        # ── Find matching open position (fuzzy symbol match) ───
        positions  = getattr(self.agent, "_open_positions", {})
        matched_ca = None
        matched_pos = None
        for ca, pos in positions.items():
            stored = pos.get("symbol", "").upper().strip()
            # Accept exact match OR one is a substring of the other (handles ticker variants)
            if stored == symbol or symbol in stored or stored in symbol:
                matched_ca  = ca
                matched_pos = pos
                break

        # ── Resolve SOL amount ─────────────────────────────────
        if len(parts) >= 4:
            try:
                sol_amount = float(parts[3])
            except ValueError:
                return "⚠️ SOL amount must be a number, e.g. `0.04`"
        elif matched_pos:
            sol_amount = matched_pos.get("sol_amount", 0)
        else:
            return (
                "⚠️ No open position found for $" + symbol + ".\n\n"
                "Provide the SOL amount:\n`/log $" + symbol + " " + parts[2] + " <sol_amount>`"
            )

        # ── Determine outcome ──────────────────────────────────
        if pnl_pct >= 3:
            outcome = "WIN"
        elif pnl_pct <= -3:
            outcome = "RUG" if pnl_pct <= -50 else "LOSS"
        else:
            outcome = "BREAKEVEN"

        sol_pnl = sol_amount * (pnl_pct / 100)
        emoji   = "✅" if outcome == "WIN" else ("💀" if outcome == "RUG" else "❌")

        # ── Close open position if one matched ─────────────────
        closed_note = ""
        entry_price = matched_pos.get("entry_price", 1.0) if matched_pos else 1.0
        hold_hours  = 0.0
        ca_key      = matched_ca or f"manual_{symbol}_{int(time.time())}"
        setup_type  = "manual_log"

        if matched_ca and matched_pos:
            from vector_memory import _redis as _mem_redis
            hold_hours  = (time.time() - matched_pos.get("entry_time", time.time())) / 3600
            setup_type  = matched_pos.get("setup_type", "manual_log")
            del self.agent._open_positions[matched_ca]
            _mem_redis.set_json("axiom:positions", self.agent._open_positions)
            # Also update session stats
            stats = getattr(self.agent, "_session_stats", {})
            if outcome == "WIN":
                stats["closed_wins"] = stats.get("closed_wins", 0) + 1
            elif outcome in ("LOSS", "RUG"):
                stats["closed_losses"] = stats.get("closed_losses", 0) + 1
            _mem_redis.set_json("axiom:session_stats", stats)
            closed_note = "\n📂 Position closed and removed from tracking."

        # ── Build TradeRecord ──────────────────────────────────
        try:
            from vector_memory import TradeRecord
            record = TradeRecord(
                ca=ca_key,
                symbol=symbol,
                entry_price=entry_price,
                exit_price=round(entry_price * (1 + pnl_pct / 100), 8),
                hold_hours=round(hold_hours, 3),
                outcome=outcome,
                pnl_pct=round(pnl_pct, 2),
                momentum_score=matched_pos.get("momentum_score", 0) if matched_pos else 0,
                vl_ratio_5m=0, ofi=0, rsi_15m=0, sentiment=0,
                smart_wallet_buys=0, top10_pct=0, holder_count=0,
                lp_locked=False, confidence=0,
                setup_type=setup_type,
                source="manual",
                catalyst_tags=["manual_log"],
                lessons=(
                    f"Manual log: ${symbol} {pnl_pct:+.1f}% on {sol_amount} SOL. "
                    f"Outcome: {outcome}."
                ),
            )
            self.agent.memory.store(record)

            try:
                if hasattr(self.agent, "learner") and self.agent.learner:
                    await self.agent.learner.update_from_feedback(record)
            except Exception:
                pass

            if hasattr(self.agent, "_day_trades"):
                self.agent._day_trades.append(record)

        except Exception as exc:
            return f"⚠️ Failed to store trade record: {exc}"

        hold_s = f"{int(hold_hours*60)}min" if hold_hours < 1 else f"{hold_hours:.1f}h"
        return (
            f"{emoji} *Trade logged: ${symbol}*\n\n"
            f"P&L: *{pnl_pct:+.1f}%* ({sol_pnl:+.3f} SOL)\n"
            f"Outcome: *{outcome}*\n"
            f"SOL in: {sol_amount}"
            + (f" | Held: {hold_s}" if hold_hours > 0 else "")
            + f"\n{closed_note}\n\n"
            f"📚 Bot memory updated.\n"
            f"_Use `/report` to see updated win rate._"
        )

    async def _cmd_balance(self, text: str) -> str:
        """
        /balance — Show SOL balance + open position values from Telegram.
        """
        import aiohttp as _aio
        from vector_memory import _redis

        wallet = self.agent._wallet_tracker.get_wallet() if hasattr(self.agent, "_wallet_tracker") else None
        if not wallet:
            wallet = _redis.get_json("axiom:my_wallet")
        if not wallet or len(str(wallet)) < 32:
            return "⚠️ No wallet set. Use `/mywallet <address>` first."

        RPCS = [
            "https://api.mainnet-beta.solana.com",
            "https://solana-api.projectserum.com",
        ]
        LAMPORTS = 1_000_000_000

        sol_balance = None
        for rpc in RPCS:
            try:
                async with _aio.ClientSession() as s:
                    async with s.post(rpc, json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getBalance",
                        "params": [wallet, {"commitment": "confirmed"}]
                    }, headers={"Content-Type": "application/json"},
                    timeout=_aio.ClientTimeout(total=10)) as r:
                        data = await r.json()
                sol_balance = data.get("result", {}).get("value", 0) / LAMPORTS
                break
            except Exception:
                continue

        if sol_balance is None:
            return "⚠️ Could not fetch balance — RPC error. Try again."

        # Get SOL price from DexScreener
        sol_usd = 0.0
        try:
            async with _aio.ClientSession() as s:
                async with s.get(
                    "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112",
                    timeout=_aio.ClientTimeout(total=8)
                ) as r:
                    d = await r.json()
            pairs = d.get("pairs") or []
            if pairs:
                sol_usd = float(pairs[0].get("priceUsd") or 0)
        except Exception:
            sol_usd = 150.0  # fallback estimate

        sol_value_usd = sol_balance * sol_usd

        # Build open positions summary
        positions = self.agent._open_positions or {}
        pos_lines = []
        total_pos_usd = 0.0
        for ca, pos in positions.items():
            sym = pos.get("symbol", ca[:8])
            sol_in = pos.get("sol_amount", 0.0)
            entry_p = pos.get("entry_price", 0.0)

            # Try to get current price
            cur_price = 0.0
            try:
                async with _aio.ClientSession() as s:
                    async with s.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                        timeout=_aio.ClientTimeout(total=6)
                    ) as r:
                        pd = await r.json()
                pairs2 = pd.get("pairs") or []
                if pairs2:
                    cur_price = float(pairs2[0].get("priceUsd") or 0)
            except Exception:
                cur_price = entry_p

            pnl_pct = ((cur_price - entry_p) / entry_p * 100) if entry_p > 0 else 0
            pos_usd = sol_in * sol_usd if sol_usd > 0 else 0
            total_pos_usd += pos_usd
            emoji = "🟢" if pnl_pct >= 0 else "🔴"
            pos_lines.append(
                f"  {emoji} ${sym} | {sol_in:.3f} SOL in | P&L: {pnl_pct:+.1f}%"
            )

        pos_block = "\n".join(pos_lines) if pos_lines else "  None"

        return (
            f"💳 *Wallet Balance*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"◎ SOL: *{sol_balance:.4f}* (${sol_value_usd:.2f})\n"
            f"📍 `{wallet[:8]}...{wallet[-4:]}`\n\n"
            f"📊 *Open Positions:*\n{pos_block}\n\n"
            f"💰 *Est. Portfolio:* ${sol_value_usd + total_pos_usd:.2f}"
        )

    async def _cmd_sync(self, text: str) -> str:
        """
        /sync — Read wallet's live token balances from Solana RPC and
        auto-add any holdings not already tracked as open positions.
        Entry price is set to the current price (P&L tracked from now).
        SOL amount will be 0 until user runs /track $SYMBOL <sol>.
        """
        if not self.agent:
            return "⚠️ Agent not available."
        tracker = getattr(self.agent, "my_wallet_tracker", None)
        if not tracker:
            return "⚠️ Wallet tracker not loaded."
        wallet = tracker.get_wallet()
        if not wallet:
            return "⚠️ No wallet set. Use `/mywallet <address>` first."

        await self.agent.telegram.send("🔄 Syncing wallet positions... (checking Solana RPC)")

        # ── Fetch all token accounts owned by the wallet ──────────────
        SKIP_MINTS = {
            "So11111111111111111111111111111111111111112",  # WSOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
        }
        # Query BOTH token programs — classic SPL and Token-2022
        TOKEN_PROGRAMS = [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # classic SPL Token
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
        ]
        # Try multiple RPCs in case of rate limits
        SYNC_RPCS = [
            "https://api.mainnet-beta.solana.com",
            "https://solana-api.projectserum.com",
        ]

        raw_accounts = []
        for program_id in TOKEN_PROGRAMS:
            for rpc_url in SYNC_RPCS:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.post(
                            rpc_url,
                            json={
                                "jsonrpc": "2.0", "id": 1,
                                "method": "getTokenAccountsByOwner",
                                "params": [
                                    wallet,
                                    {"programId": program_id},
                                    {"encoding": "jsonParsed"},
                                ],
                            },
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as r:
                            if r.status == 429:
                                continue   # rate limited — try next RPC
                            data = await r.json()
                    batch = (data.get("result") or {}).get("value", [])
                    raw_accounts.extend(batch)
                    print(f"[Sync] {rpc_url} program={program_id[:8]}... → {len(batch)} accounts")
                    break   # got a good response — move to next program
                except Exception as exc:
                    print(f"[Sync] RPC error ({rpc_url}): {exc}")
                    continue

        # Filter to tokens with a real balance
        tokens = []
        for acct in raw_accounts:
            info = (
                acct.get("account", {})
                    .get("data", {})
                    .get("parsed", {})
                    .get("info", {})
            )
            mint   = info.get("mint", "")
            ui_amt = float((info.get("tokenAmount") or {}).get("uiAmount") or 0)
            if not mint or mint in SKIP_MINTS or ui_amt <= 0:
                continue
            tokens.append({"ca": mint, "amount": ui_amt})

        if not tokens:
            return (
                f"📭 *No token balances found.*\n\n"
                f"Wallet checked: `{wallet[:16]}...`\n"
                f"Raw accounts fetched: {len(raw_accounts)}\n\n"
                f"This means either:\n"
                f"• You're no longer holding any memecoins in this wallet\n"
                f"• The Solana RPC rate-limited — try `/sync` again in 30s\n\n"
                f"If you're still in a trade, check Axiom to confirm your wallet address matches `/mywallet`."
            )

        from vector_memory import _redis as _mem_redis

        added   = []
        already = []

        for tok in tokens[:30]:   # cap at 30 to avoid Telegram timeouts
            ca = tok["ca"]
            if ca in self.agent._open_positions:
                sym = self.agent._open_positions[ca].get("symbol", ca[:8])
                already.append(f"${sym}")
                continue

            # Lookup symbol + current price on DexScreener
            symbol = ca[:8]
            price  = 0.0
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                        headers={"User-Agent": "AxiomAIAgent/2.0"},
                        timeout=aiohttp.ClientTimeout(total=6),
                    ) as r:
                        d = await r.json()
                pairs = d.get("pairs") or []
                if pairs:
                    p      = pairs[0]
                    symbol = (p.get("baseToken", {}).get("symbol") or ca[:8]).upper()
                    price  = float(p.get("priceUsd", 0) or 0)
            except Exception:
                pass

            # Skip dust/airdrop tokens — must be worth at least $0.50 USD
            token_value_usd = price * tok["amount"] if price > 0 else 0
            if token_value_usd < 0.50 and price > 0:
                print(f"[Sync] Skip ${symbol} — value ${token_value_usd:.4f} < $0.50 threshold")
                continue
            if price == 0:
                # No price data on DexScreener — likely dead token, skip
                print(f"[Sync] Skip {ca[:16]}... — no price data")
                continue

            self.agent._open_positions[ca] = {
                "symbol":      symbol,
                "entry_price": price,       # USD — same unit as live price lookups
                "sol_amount":  0.0,         # unknown — update with /track $SYMBOL <sol>
                "entry_time":  time.time(),
                "auto_entry":  False,
                "synced":      True,
            }
            added.append(f"${symbol}")
            print(f"[Sync] Added ${symbol} | ca={ca[:16]}... | price={price:.8f} | value=${token_value_usd:.2f}")

        if added:
            _mem_redis.set_json("axiom:positions", self.agent._open_positions)

        lines = ["🔄 *Wallet Sync Complete*\n"]
        if added:
            lines.append(f"✅ *Imported {len(added)} position(s):* {', '.join(added)}")
            lines.append(
                "\n⚠️ *SOL amounts unknown* — set them so P&L is accurate:\n"
                + "\n".join(f"`/track {sym} <sol_amount>`" for sym in added)
            )
        if already:
            lines.append(f"\n↩️ Already tracked: {', '.join(already)}")
        if not added and not already:
            lines.append("No new positions to import.")

        return "\n".join(lines)

    async def _cmd_positions(self, text: str) -> str:
        """Show all open positions with live P&L."""
        if self.agent is None:
            return "⚠️ Agent not available."
        positions = getattr(self.agent, "_open_positions", {})
        if not positions:
            return (
                "📭 No open positions.\n\n"
                "Your swaps are tracked automatically.\n"
                "Make sure `/mywallet` is set — bot detects every on-chain buy."
            )

        lines = ["📊 *Open Positions*\n"]
        total_sol_in  = 0.0
        total_sol_pnl = 0.0

        for ca, pos in positions.items():
            price = await self._fetch_price_for_ca(ca)
            entry = pos["entry_price"]
            sym   = pos["symbol"]
            sol   = pos.get("sol_amount", 0)
            if price > 0 and entry > 0:
                pnl_pct = (price - entry) / entry * 100
                sol_pnl = sol * (pnl_pct / 100)
                total_sol_in  += sol
                total_sol_pnl += sol_pnl
                hold_h = (time.time() - pos["entry_time"]) / 3600
                emoji  = "🟢" if pnl_pct > 5 else "🔴" if pnl_pct < -5 else "🟡"
                lines.append(
                    f"{emoji} *${sym}* | {pnl_pct:+.1f}% | {sol_pnl:+.3f} SOL\n"
                    f"   In: {sol} SOL | Held: {hold_h:.1f}h\n"
                    f"   CA: `{ca[:16]}...`"
                )
            else:
                lines.append(f"⚪ *${sym}* | price unavailable\n   CA: `{ca[:16]}...`")

        if len(positions) > 1:
            pnl_emoji = "🟢" if total_sol_pnl > 0 else "🔴"
            lines.append(
                f"\n{pnl_emoji} *Total P&L: {total_sol_pnl:+.3f} SOL*"
            )

        lines.append("\n_Bot auto-detects your sells on-chain_")
        return "\n".join(lines)

    async def _cmd_addwallet(self, text: str) -> str:
        """Add a wallet address to track. Usage: /addwallet <address>"""
        parts = text.strip().split()
        if len(parts) < 2:
            return (
                "Usage: `/addwallet <wallet_address>`\n\n"
                "Example:\n`/addwallet 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU`\n\n"
                "The bot will alert you whenever this wallet buys a token."
            )
        address = parts[1].strip()
        if len(address) < 32:
            return "⚠️ That doesn't look like a valid Solana wallet address. Check and try again."
        if self.agent is None or not hasattr(self.agent, "wallet_registry"):
            return "⚠️ Wallet tracking module not loaded."
        already = address in self.agent.wallet_registry.wallets
        if already:
            return f"✅ `{address[:8]}...` is already being tracked."
        self.agent.wallet_registry.add(address)
        total = len(self.agent.wallet_registry.wallets)
        return (
            f"✅ *Wallet added!*\n\n"
            f"`{address}`\n\n"
            f"Now tracking *{total}* wallet(s). You'll get an alert the next time this wallet buys a token."
        )

    async def _cmd_rmwallet(self, text: str) -> str:
        """Remove a tracked wallet. Usage: /rmwallet <address>"""
        parts = text.strip().split()
        if len(parts) < 2:
            return "Usage: `/rmwallet <wallet_address>`"
        address = parts[1].strip()
        if self.agent is None or not hasattr(self.agent, "wallet_registry"):
            return "⚠️ Wallet tracking module not loaded."
        if address not in self.agent.wallet_registry.wallets:
            return f"⚠️ `{address[:8]}...` is not in your tracked wallets."
        self.agent.wallet_registry.remove(address)
        return f"🗑 Removed `{address[:8]}...` from tracking (saved to Redis)."

    async def _cmd_wallets(self, text: str) -> str:
        if self.agent is None or not hasattr(self.agent, "wallet_registry"):
            return "⚠️ Smart wallet tracking not yet active. Use /addwallet <address> to start."
        top = self.agent.wallet_registry.top_wallets(10)
        if not top:
            return "📭 No wallets tracked yet.\n\nUse `/addwallet <address>` to add one."
        lines = [f"🏆 *Tracked Wallets*\n"]
        for i, w in enumerate(top, 1):
            lines.append(
                f"{i}. `{w.address[:8]}...{w.address[-4:]}` — "
                f"Win rate: *{w.win_rate:.0f}%* | "
                f"Trades: {w.wins + w.losses}"
            )
        lines.append("\n_Use /addwallet <address> to add | /rmwallet <address> to remove_")
        return "\n".join(lines)

    async def _cmd_check(self, text: str) -> str:
        """
        On-demand safety check for any CA.
        Usage: /check <contract_address>
        Fetches live DexScreener data and scores the token.
        """
        parts = text.strip().split()
        if len(parts) < 2:
            return (
                "Usage: `/check <contract_address>`\n\n"
                "Paste any Solana token CA and I'll check it live:\n"
                "`/check 8wy3gjarQaZDggULCp7ZwuZ6xYVZsNNB3BNNLnukpump`"
            )
        ca = parts[1].strip()
        if len(ca) < 32:
            return "⚠️ That doesn't look like a valid Solana CA. Check and try again."

        data = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                    headers={"User-Agent": "AxiomAIAgent/2.0"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    raw = await r.text()
                    if raw and raw.strip():
                        import json as _json
                        try:
                            data = _json.loads(raw)
                        except Exception:
                            data = {}
        except Exception as exc:
            return (
                f"⚠️ Couldn't reach DexScreener right now.\n"
                f"Try again in 30s, or check manually:\n"
                f"dexscreener.com/solana/{ca}"
            )

        pairs = data.get("pairs", [])
        if not pairs:
            pf_url = f"pump.fun/{ca}" if ca.endswith("pump") else f"dexscreener.com/solana/{ca}"
            return (
                f"⚠️ *Token not indexed yet.*\n\n"
                f"DexScreener hasn't picked this up — it's likely under 1–2 minutes old.\n\n"
                f"Check it directly:\n"
                f"🔗 {pf_url}\n\n"
                f"Paste the CA again in ~2 min and the bot will be able to analyze it."
            )

        # Use highest-liquidity Solana pair
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return f"⚠️ This token exists but not on Solana. CA: `{ca}`"
        p = sorted(sol_pairs, key=lambda x: float((x.get("liquidity") or {}).get("usd", 0) or 0), reverse=True)[0]

        # ── Check if user already holds this token ────────────────
        already_in   = False
        position_note = ""
        if self.agent:
            open_pos = getattr(self.agent, "_open_positions", {})
            if ca in open_pos:
                already_in = True

        base      = p.get("baseToken", {})
        sym       = base.get("symbol", "?")
        name      = base.get("name", "")
        price     = float(p.get("priceUsd", 0) or 0)
        mcap      = float(p.get("marketCap", 0) or 0)
        liq       = float((p.get("liquidity") or {}).get("usd", 0) or 0)
        vol5m     = float((p.get("volume") or {}).get("m5", 0) or 0)
        vol1h     = float((p.get("volume") or {}).get("h1", 0) or 0)
        vol24h    = float((p.get("volume") or {}).get("h24", 0) or 0)
        chg5m     = float((p.get("priceChange") or {}).get("m5", 0) or 0)
        chg1h     = float((p.get("priceChange") or {}).get("h1", 0) or 0)
        chg24h    = float((p.get("priceChange") or {}).get("h24", 0) or 0)
        txns_5m   = (p.get("txns") or {}).get("m5", {})
        buys_5m   = txns_5m.get("buys", 0)
        sells_5m  = txns_5m.get("sells", 0)
        created   = p.get("pairCreatedAt", 0)
        age_min   = int((time.time() - created / 1000) / 60) if created else 0
        age_str   = f"{age_min}m" if age_min < 60 else f"{age_min // 60}h {age_min % 60}m"
        dex_url   = p.get("url", f"https://dexscreener.com/solana/{ca}")

        # ── Scoring ──────────────────────────────────────────────
        flags   = []   # danger flags
        greens  = []   # good signs
        score   = 50   # start neutral

        # Liquidity checks
        if liq < 2_000:
            flags.append("🚨 Liquidity VERY LOW (<$2k) — extreme rug risk")
            score -= 30
        elif liq < 10_000:
            flags.append("⚠️ Low liquidity (<$10k) — easy to rug")
            score -= 15
        elif liq >= 50_000:
            greens.append("✅ Solid liquidity (>$50k)")
            score += 10

        # MCap checks
        if mcap > 100_000_000:
            flags.append("⚠️ MCap >$100M — not a fresh gem, upside limited")
            score -= 10
        elif 0 < mcap < 500_000:
            greens.append("✅ Micro-cap (<$500k) — high upside potential")
            score += 10

        # Volume checks
        if vol5m == 0 and vol1h == 0:
            flags.append("🚨 Zero volume — nobody is trading this")
            score -= 25
        elif vol1h > 0 and mcap > 0:
            vl_ratio = vol1h / mcap
            if vl_ratio > 0.5:
                greens.append(f"✅ Strong volume/MCap ratio ({vl_ratio:.1f}x) — active momentum")
                score += 15
            elif vl_ratio < 0.05:
                flags.append("⚠️ Very low volume relative to MCap — weak momentum")
                score -= 10

        # Price action
        if chg1h < -40:
            flags.append(f"🚨 Price dumped {chg1h:.0f}% in 1h — possible rug in progress")
            score -= 25
        elif chg1h < -20:
            flags.append(f"⚠️ Price down {chg1h:.0f}% in 1h — selling pressure")
            score -= 10
        elif chg1h > 50:
            greens.append(f"📈 Up {chg1h:.0f}% in 1h — strong momentum")
            score += 10

        # Buy/sell pressure
        if buys_5m > 0 or sells_5m > 0:
            total_txns = buys_5m + sells_5m
            buy_pct    = buys_5m / total_txns * 100 if total_txns else 50
            if buy_pct >= 65:
                greens.append(f"✅ {buy_pct:.0f}% buys in last 5m — buyers in control")
                score += 10
            elif buy_pct <= 35:
                flags.append(f"🚨 {100 - buy_pct:.0f}% sells in last 5m — heavy selling")
                score -= 15

        # Age check
        if age_min < 10:
            flags.append(f"⚠️ Token only {age_min}m old — very high risk, DYOR")
            score -= 5
        elif age_min < 60:
            greens.append(f"⏱ Fresh token ({age_str} old) — early entry window")

        score = max(0, min(100, score))

        # ── Score label ───────────────────────────────────────────
        if score >= 65:
            verdict = "🟢 *LOOKS INTERESTING*"
        elif score >= 40:
            verdict = "🟡 *PROCEED WITH CAUTION*"
        else:
            verdict = "🔴 *HIGH RISK — BE CAREFUL*"

        # ── Smart verdict engine: BUY / WATCH / SKIP ─────────────
        regime    = (getattr(self.agent, "_session_stats", {}).get("market_regime", "MIXED")
                     if self.agent else "MIXED")
        has_crit  = any("VERY LOW" in f or "Zero volume" in f or "rug in progress" in f
                        for f in flags)
        buy_pct_v = (buys_5m / max(buys_5m + sells_5m, 1) * 100
                     if (buys_5m + sells_5m) > 0 else 50.0)

        # Decision tree — score + flags + buy pressure drive the verdict
        if has_crit or score < 40:
            verdict_action = "SKIP"
        elif score < 55:
            verdict_action = "WATCH"
        elif score >= 70 and not has_crit and buy_pct_v >= 50:
            verdict_action = "BUY"
        else:
            verdict_action = "WATCH"
        # BEAR market: never recommend BUY (cap at WATCH)
        if regime == "BEAR" and verdict_action == "BUY":
            verdict_action = "WATCH"
        # Already in this trade: advice changes from entry to hold/exit
        if already_in:
            verdict_action = "HOLD_CHECK"

        # Build analysis lines
        analysis  = []
        size_line = ""
        if verdict_action == "HOLD_CHECK":
            verdict_emoji = "📍"
            open_pos = getattr(self.agent, "_open_positions", {}) if self.agent else {}
            pos      = open_pos.get(ca, {})
            entry_p  = pos.get("entry_price", 0)
            sol_in   = pos.get("sol_amount", 0)
            entry_time = pos.get("entry_time", 0)
            if entry_p > 0 and price > 0:
                pos_pnl  = (price - entry_p) / entry_p * 100
                sol_pnl  = sol_in * (pos_pnl / 100) if sol_in else 0
                hold_h   = (time.time() - entry_time) / 3600 if entry_time else 0
                pnl_e    = "🟢" if pos_pnl > 0 else "🔴"
                analysis.append(f"{pnl_e} P&L: *{pos_pnl:+.1f}%* ({sol_pnl:+.3f} SOL) | Held: {hold_h:.1f}h")
            if score >= 70 and greens:
                analysis.append(f"📈 {greens[0].replace('✅ ','').replace('📈 ','')}")
                analysis.append("✅ Token still looks strong — hold position")
            if flags:
                analysis.append(f"⚠️ New flag: {flags[0].replace('⚠️ ','').replace('🚨 ','')}")
        elif verdict_action == "BUY":
            verdict_emoji = "✅"
            pos_parts = [g.replace("✅ ", "").replace("📈 ", "") for g in greens[:2]]
            if pos_parts:
                analysis.append(f"📈 {' · '.join(pos_parts)}")
            if flags:
                analysis.append(f"⚠️ Watch: {flags[0].replace('⚠️ ','').replace('🚨 ','')}")
            size_rec  = "0.10–0.15 SOL" if score >= 80 else "0.05–0.08 SOL"
            tracker   = getattr(self.agent, "my_wallet_tracker", None)
            wallet_ok = tracker and tracker.get_wallet()
            track_line = (
                "🔗 *Your buy will be auto-tracked on-chain*"
                if wallet_ok else
                "⚡ Set `/mywallet <address>` to auto-track this trade"
            )
            size_line = f"💡 Size: *{size_rec}*\n{track_line}"
        elif verdict_action == "WATCH":
            verdict_emoji = "👀"
            if greens:
                analysis.append(f"📈 {greens[0].replace('✅ ','').replace('📈 ','')}")
            if flags:
                analysis.append(f"⚠️ {flags[0].replace('⚠️ ','').replace('🚨 ','')}")
            if regime == "BEAR":
                analysis.append("🔴 BEAR market — max caution")
            analysis.append("⏳ Wait for stronger momentum before entry")
            size_line = "💡 Size: *0.03 SOL max* if you enter"
        else:  # SKIP
            verdict_emoji = "❌"
            for f in flags[:2]:
                analysis.append(f"🚫 {f.replace('🚨 ','').replace('⚠️ ','')}")
            if regime == "BEAR":
                analysis.append("🔴 BEAR market — skip marginal setups")

        # ── Assemble output ───────────────────────────────────────
        lines = [
            f"🔍 *Token Check: ${sym}* ({name})\n",
            f"Score: *{score}/100*  {verdict}\n",
            f"💰 Price: `${price:.10f}`",
            f"📊 MCap: ${mcap:,.0f}",
            f"💧 Liquidity: ${liq:,.0f}",
            f"📈 Volume: 5m ${vol5m:,.0f} | 1h ${vol1h:,.0f} | 24h ${vol24h:,.0f}",
            f"📉 Change: 5m {chg5m:+.1f}% | 1h {chg1h:+.1f}% | 24h {chg24h:+.1f}%",
            f"🔄 Txns (5m): {buys_5m} buys / {sells_5m} sells",
            f"⏱ Age: {age_str}",
            f"CA: `{ca}`\n",
        ]
        if greens:
            lines.append("*Green flags:*\n" + "\n".join(greens))
        if flags:
            lines.append("*Red flags:*\n" + "\n".join(flags))

        # ── Verdict block ─────────────────────────────────────────
        verdict_label = "HOLD — YOU'RE IN THIS TRADE" if verdict_action == "HOLD_CHECK" else verdict_action
        lines.append(f"\n{'─' * 26}")
        lines.append(f"{verdict_emoji} *{verdict_label}*")
        if size_line:
            lines.append(size_line)
        lines.extend(analysis)
        lines.append(f"{'─' * 26}")
        lines.append(f"[View on DexScreener]({dex_url})")

        return "\n".join(lines)

    async def _cmd_early(self, text: str) -> str:
        if self.agent is None or not hasattr(self.agent, "_early_signals"):
            return "⚠️ No early entry signals yet — scanning in progress."
        signals = getattr(self.agent, "_early_signals", [])
        if not signals:
            return "📭 No early entry signals found in the last scan. Check again in a few minutes."
        lines = ["⚡ *Latest Early Entry Signals*\n"]
        for sig in signals[:5]:
            emoji = "🔴" if sig.urgency == "URGENT" else "🟡" if sig.urgency == "ALERT" else "🟢"
            lines.append(
                f"{emoji} *${sig.symbol}* — Score {sig.score}/100\n"
                f"  `{sig.ca[:16]}...`\n"
                f"  {sig.recommended_action[:80]}"
            )
        return "\n\n".join(lines)

    # ── Live market fetch for chat context (Birdeye primary) ──
    async def _fetch_live_movers(self) -> str:
        """Primary: Birdeye API. Fallback: DexScreener trending."""
        # Try Birdeye first
        result = await self._fetch_birdeye_movers()
        if not result.startswith("⚠️"):
            return result

        # Fallback: DexScreener trending boosts
        BOOST_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
        TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"
        HEADERS   = {"User-Agent": "AxiomAIAgent/2.0"}
        results   = []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    BOOST_URL, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    boosts = await r.json(content_type=None)
                sol_cas = [
                    b.get("tokenAddress", "") for b in (boosts if isinstance(boosts, list) else [])
                    if b.get("chainId") == "solana" and b.get("tokenAddress")
                ][:10]

                for ca in sol_cas:
                    try:
                        async with session.get(
                            TOKEN_URL.format(ca), headers=HEADERS,
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as pr:
                            pdata = await pr.json(content_type=None)
                            pairs = pdata.get("pairs", [])
                            if not pairs:
                                continue
                            p    = pairs[0]
                            base = p.get("baseToken", {})
                            chg  = p.get("priceChange", {}) or {}
                            liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                            vol5 = float((p.get("volume") or {}).get("m5", 0) or 0)
                            results.append({
                                "symbol": base.get("symbol", "?"),
                                "ca":     ca,
                                "price":  p.get("priceUsd", "0"),
                                "vol5m":  vol5, "liq": liq,
                                "chg1h":  float(chg.get("h1", 0) or 0),
                                "mcap":   float(p.get("marketCap") or 0),
                            })
                    except Exception:
                        continue
        except Exception as exc:
            return f"⚠️ All data sources failed: {exc}"

        if not results:
            return "⚠️ No live data — check API keys."

        lines = ["📡 *LIVE Solana Scan* (DexScreener fallback)\n"]
        for i, t in enumerate(results[:8], 1):
            lines.append(
                f"{i}. *${t['symbol']}*\n"
                f"   Price: ${t['price']} | MCap: ${t['mcap']:,.0f}\n"
                f"   Vol5m: ${t['vol5m']:,.0f} | 1h: {t['chg1h']:+.1f}%\n"
                f"   CA: `{t['ca']}`\n"
            )
        return "\n".join(lines)

    # ── Natural Language Q&A ──────────────────────────────────
    async def _natural_language(self, text: str) -> str:
        """
        Passes the user's message to Claude with full agent context
        + live DexScreener data fetched in real time.
        """
        # Always fetch live data for market questions
        market_keywords = [
            "pump", "pumping", "trending", "hot", "moving", "mover",
            "solana", "sol", "what's", "whats", "token", "coin", "buy",
            "trade", "alpha", "setup", "play", "signal", "live", "now",
            "market", "scan", "find", "look", "show", "top", "gem",
            "low", "micro", "cap", "launch", "new", "axiom",
        ]
        wants_live = any(kw in text.lower() for kw in market_keywords)
        live_data = ""
        if wants_live:
            # Fetch Birdeye + pump.fun in parallel
            birdeye_task = asyncio.ensure_future(self._fetch_birdeye_movers())
            pump_task    = asyncio.ensure_future(self._fetch_pumpfun())
            birdeye_data, pump_data = await asyncio.gather(birdeye_task, pump_task)
            live_data = birdeye_data + "\n\n" + pump_data

        # Build context from agent state
        context_parts = []

        if live_data:
            context_parts.append(live_data)

        if self.agent:
            candidates = getattr(self.agent, "_candidates", [])
            if candidates:
                top5 = candidates[-5:]
                context_parts.append(
                    "Tokens that passed ALL filters today:\n" +
                    "\n".join(
                        f"- *${c['snap'].symbol}* | CA: `{c['snap'].ca}` | "
                        f"Momentum: {c['ta'].momentum_score:.0f}/100 | "
                        f"Confidence: {c['ta'].confidence:.0%} | "
                        f"Setup: {c['ta'].setup_type} | "
                        f"Est. win rate: {c['win_rate']*100:.0f}%"
                        for c in top5
                    )
                )

            early = getattr(self.agent, "_early_signals", [])
            if early:
                context_parts.append(
                    "Early entry signals (pre-pump):\n" +
                    "\n".join(
                        f"- *${s.symbol}* | Score: {s.score}/100 | "
                        f"Urgency: {s.urgency} | CA: `{s.ca}`"
                        for s in early[-3:]
                    )
                )

            # Recently alerted tokens (last 2 hours) — lets user ask about tokens
            # that cycled out of the active scanner after the alert fired
            recent_alerts = getattr(self.agent, "_recent_alerts", [])
            cutoff = time.time() - 7200  # 2 hours
            fresh  = [a for a in recent_alerts if a.get("alerted_at", 0) >= cutoff]
            if fresh:
                alert_lines = []
                for a in reversed(fresh):  # newest first
                    age_min = int((time.time() - a["alerted_at"]) / 60)
                    mcap    = float(a.get("market_cap", 0) or 0)
                    vol5    = float(a.get("volume_5m",  0) or 0)
                    score   = a.get("score", 0)
                    src     = a.get("source", "scan")
                    alert_lines.append(
                        f"- *${a['symbol']}* | Alerted {age_min}m ago | Score {score}/100 | "
                        f"MCap ${mcap:,.0f} | Vol5m ${vol5:,.0f} | "
                        f"CA: `{a['ca']}` | Source: {src}"
                    )
                context_parts.append(
                    "Tokens I alerted on in the last 2 hours (may have cycled out of active scan):\n"
                    + "\n".join(alert_lines)
                )

            stats = self.agent.memory.win_rate_stats()
            if stats.get("total_trades", 0) > 0:
                context_parts.append(
                    f"Historical win rate: {stats['win_rate_pct']}% "
                    f"over {stats['total_trades']} trades"
                )

        context = "\n\n".join(context_parts) if context_parts else "Scanner just started — accumulating data."

        # Rolling conversation (last 6 turns only — prevents hallucination drift)
        self._chat_history.append({"role": "user", "content": text})
        if len(self._chat_history) > 6:
            self._chat_history = self._chat_history[-6:]

        system = f"""You are a Solana memecoin market scanner bot for pollyn (Lagos, Nigeria).

WHAT YOU CAN DO:
- Show tokens from the live scan data below
- Discuss tokens I alerted on in the last 2 hours (listed under "Tokens I alerted on...")
- Suggest entry price, stop loss (-15%), and take profit (+50/+100%) for scanned or recently alerted tokens
- Log trade results when pollyn reports them (e.g. "$WIF +80%" or "$BONK rugged")
- Answer questions about tokens visible in the live data or recent alerts

WHAT YOU CANNOT DO — never claim or suggest these:
- Execute trades or place orders automatically
- Access pollyn's personal wallet, trade history, or positions
- Copy-paste trades or automate execution
- Build dashboards or charts
- Do backtesting on historical data
- Guarantee profits or returns
- Connect to exchanges or wallets

If pollyn asks about something you cannot do, say clearly: "I can't do that — I'm a scanner and analyst only."

LIVE SCAN DATA (fetched right now from DexScreener + pump.fun):
{context}

STRICT RULES:
- Only recommend tokens that appear in the live scan data above OR in the recent alerts section
- If pollyn asks about a token I alerted on recently, reference that alert data (score, MCap, CA)
- If no tokens are in either section, say "No strong setups right now — wait for a graduation alert"
- Always include CA so pollyn can check on Axiom.trade
- Keep responses under 200 words
- Use Telegram markdown (*bold*, `code` for CAs)
- Never invent features, capabilities, or token data"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                system=system,
                messages=self._chat_history,
            )
            reply = response.content[0].text.strip()
            self._chat_history.append({"role": "assistant", "content": reply})
            return reply
        except Exception as exc:
            return f"⚠️ AI error: {exc}"

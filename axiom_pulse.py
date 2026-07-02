# axiom_pulse.py — Real-time Axiom-equivalent feed via Helius REST API
#
# Uses Helius Enhanced Transactions API (free tier) to poll for:
#   1. New pump.fun token launches (CREATE instructions)
#   2. Raydium AMM pool initializations (graduations)
# Polls every 20s — same data Axiom.trade shows, no WebSocket needed.
#
# Helius REST API: https://api.helius.xyz/v0/ (free tier ✅)
# Programs monitored:
#   pump.fun  → 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
#   Raydium   → 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8

import asyncio
import json
import os
import time
import aiohttp
from dataclasses import dataclass, field
from typing import Optional, Callable
from dotenv import load_dotenv

load_dotenv()

# ── Extract Helius API key from SOLANA_RPC_URL ────────────────
_RPC_URL    = os.getenv("SOLANA_RPC_URL", "")
_HELIUS_KEY = ""
if "api-key=" in _RPC_URL:
    _HELIUS_KEY = _RPC_URL.split("api-key=")[-1].strip()

HELIUS_API = f"https://api.helius.xyz/v0"
HELIUS_RPC = _RPC_URL or "https://api.mainnet-beta.solana.com"

# ── Solana program IDs ────────────────────────────────────────
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

BIRDEYE_KEY  = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_BASE = "https://public-api.birdeye.so"

# Poll interval — how often to check for new transactions
POLL_INTERVAL = 20   # seconds


# ── Token data model ──────────────────────────────────────────
@dataclass
class LiveToken:
    ca:            str
    symbol:        str   = "NEW"
    name:          str   = ""
    source:        str   = "pump.fun"   # "pump.fun" | "graduation"
    price_usd:     float = 0.0
    market_cap:    float = 0.0
    liquidity:     float = 0.0
    volume_1h:     float = 0.0
    signature:     str   = ""
    is_graduation: bool  = False
    timestamp:     float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """Convert to the raw dict format the orchestrator's _process_token expects."""
        return {
            "contract_address": self.ca,
            "symbol":           self.symbol,
            "price_usd":        self.price_usd,
            "volume_5m":        self.volume_1h / 12,  # rough 5m estimate
            "volume_15m":       self.volume_1h / 4,
            "volume_1h":        self.volume_1h,
            "liquidity_usd":    self.liquidity,
            "market_cap":       self.market_cap,
            "buy_txns_15m":     0,
            "sell_txns_15m":    0,
            "top10_wallet_pct": 0,
            "holder_count":     0,
            "lp_locked":        self.is_graduation,  # graduated = Raydium pool exists
            "is_honeypot":      False,
            "total_supply":     0,
            # Extra context for Telegram alerts
            "_is_graduation":   self.is_graduation,
            "_source":          self.source,
            "_sig":             self.signature,
        }


# ── Main Pulse Feed ───────────────────────────────────────────
class AxiomPulseFeed:
    """
    Real-time Solana token feed — same data Axiom.trade shows.
    Connects via Helius WebSocket to the actual blockchain.
    """

    def __init__(self, on_new_launch: Callable, on_graduation: Callable):
        self.on_new_launch = on_new_launch   # async callback(LiveToken)
        self.on_graduation = on_graduation   # async callback(LiveToken)
        self._running      = False
        self._seen_sigs: set = set()

    async def start(self):
        if not _HELIUS_KEY:
            print("[AxiomPulse] ⚠️  No Helius key — set SOLANA_RPC_URL with api-key param")
            return
        self._running = True
        print(f"[AxiomPulse] ✅ Starting REST poller with key: {_HELIUS_KEY[:8]}...")
        # Run both pollers concurrently
        await asyncio.gather(
            self._pump_poller(),
            self._raydium_poller(),
            return_exceptions=True,
        )

    def stop(self):
        self._running = False

    async def _fetch_recent_sigs(self, program: str, limit: int = 20) -> list[str]:
        """
        Use standard Solana RPC getSignaturesForAddress — free on all Helius plans.
        Returns list of recent transaction signatures for the given program.
        """
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [program, {"limit": limit, "commitment": "confirmed"}],
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    HELIUS_RPC, json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json(content_type=None)
            results = data.get("result", [])
            # Filter out failed transactions
            return [r["signature"] for r in results if not r.get("err")]
        except Exception as exc:
            print(f"[AxiomPulse] getSignaturesForAddress error: {exc}")
            return []

    async def _fetch_tx(self, sig: str) -> Optional[dict]:
        """Fetch full transaction via standard Solana RPC getTransaction."""
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [sig, {"encoding": "json",
                             "maxSupportedTransactionVersion": 0,
                             "commitment": "confirmed"}],
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    HELIUS_RPC, json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json(content_type=None)
            return data.get("result")
        except Exception as exc:
            print(f"[AxiomPulse] getTransaction error: {exc}")
            return None

    # ── pump.fun poller (standard Solana RPC) ─────────────────
    async def _pump_poller(self):
        """
        Polls pump.fun program via standard getSignaturesForAddress RPC.
        Free on all Helius plans. Detects new token launches.
        """
        print("[AxiomPulse] 🔄 pump.fun poller started (standard RPC)")
        while self._running:
            sigs = await self._fetch_recent_sigs(PUMPFUN_PROGRAM, limit=20)
            new_count = 0
            for sig in sigs:
                if sig in self._seen_sigs:
                    continue
                self._seen_sigs.add(sig)
                self._trim_seen()

                tx = await self._fetch_tx(sig)
                if not tx:
                    continue

                # Check for token creation (new mint in postTokenBalances)
                meta = tx.get("meta", {}) or {}
                pre  = meta.get("preTokenBalances",  []) or []
                post = meta.get("postTokenBalances", []) or []

                # New mint = appears in post but NOT in pre
                pre_mints  = {b.get("mint") for b in pre}
                post_mints = {b.get("mint") for b in post}
                new_mints  = post_mints - pre_mints - {
                    "So11111111111111111111111111111111111111112"
                }

                if not new_mints:
                    continue

                ca = next(iter(new_mints))
                new_count += 1
                token = await self._enrich(ca, sig, is_graduation=False)
                print(f"[AxiomPulse] 🆕 LAUNCH  ${token.symbol:10s} | {ca[:12]}...")
                await self.on_new_launch(token)

            if sigs:
                print(f"[AxiomPulse] pump.fun: {len(sigs)} sigs, {new_count} new launches")
            await asyncio.sleep(POLL_INTERVAL)

    # ── Raydium graduation poller (standard Solana RPC) ───────
    async def _raydium_poller(self):
        """
        Polls Raydium AMM via getSignaturesForAddress.
        Detects new pool initializations = pump.fun graduations.
        """
        print("[AxiomPulse] 🔄 Raydium graduation poller started (standard RPC)")
        await asyncio.sleep(10)
        while self._running:
            sigs = await self._fetch_recent_sigs(RAYDIUM_PROGRAM, limit=10)
            for sig in sigs:
                if sig in self._seen_sigs:
                    continue
                self._seen_sigs.add(sig)
                self._trim_seen()

                tx = await self._fetch_tx(sig)
                if not tx:
                    continue

                meta = tx.get("meta", {}) or {}
                post = meta.get("postTokenBalances", []) or []
                pre  = meta.get("preTokenBalances",  []) or []

                # Pool init = 2+ new token mints (token A + token B in pool)
                pre_mints  = {b.get("mint") for b in pre}
                post_mints = {b.get("mint") for b in post}
                new_mints  = post_mints - pre_mints - {
                    "So11111111111111111111111111111111111111112"
                }

                if len(new_mints) < 1:
                    continue

                ca = next(iter(new_mints))
                token = await self._enrich(ca, sig, is_graduation=True)
                print(f"[AxiomPulse] 🎓 BONDED  ${token.symbol:10s} | {ca[:12]}...")
                await self.on_graduation(token)

            await asyncio.sleep(POLL_INTERVAL)

    # ── Helpers ───────────────────────────────────────────────
    def _extract_mint(self, tx: dict) -> Optional[str]:
        """
        Extract token mint address from a Helius parsed transaction.
        Helius returns rich structured data — no separate RPC call needed.
        """
        SOL_MINT = "So11111111111111111111111111111111111111112"

        # 1. tokenTransfers array (Helius enhanced parsing)
        for transfer in tx.get("tokenTransfers", []):
            mint = transfer.get("mint", "")
            if mint and mint != SOL_MINT:
                return mint

        # 2. accountData with token standard
        for acct in tx.get("accountData", []):
            mint = acct.get("account", "")
            if mint and len(mint) > 32 and mint != SOL_MINT:
                return mint

        # 3. instructions → accounts
        for ix in tx.get("instructions", []):
            for acct in ix.get("accounts", []):
                if acct and len(acct) > 32 and acct != SOL_MINT:
                    return acct

        return None

    async def _enrich(self, ca: str, sig: str, is_graduation: bool) -> LiveToken:
        """Fetch token price/mcap/liquidity from Birdeye."""
        token = LiveToken(
            ca=ca,
            source="graduation" if is_graduation else "pump.fun",
            signature=sig,
            is_graduation=is_graduation,
        )
        if not BIRDEYE_KEY:
            return token
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BIRDEYE_BASE}/defi/token_overview",
                    headers={"X-API-KEY": BIRDEYE_KEY, "x-chain": "solana", "accept": "application/json"},
                    params={"address": ca},
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as r:
                    data = await r.json(content_type=None)

            d = (data.get("data") or {}) if isinstance(data, dict) else {}
            token.symbol     = d.get("symbol", "NEW") or "NEW"
            token.name       = d.get("name", "") or ""
            token.price_usd  = float(d.get("price", 0) or 0)
            token.market_cap = float(d.get("mc", 0) or 0)
            token.liquidity  = float(d.get("liquidity", 0) or 0)
            token.volume_1h  = float(d.get("v1hUSD", 0) or 0)
        except Exception as exc:
            print(f"[AxiomPulse] enrich error for {ca[:8]}: {exc}")
        return token


# ── Telegram formatters ───────────────────────────────────────
def format_launch_alert(token: LiveToken) -> str:
    mcap_str = f"${token.market_cap:,.0f}" if token.market_cap else "?"
    liq_str  = f"${token.liquidity:,.0f}"  if token.liquidity  else "seeding"
    vol_str  = f"${token.volume_1h:,.0f}"  if token.volume_1h  else "?"
    return (
        f"🚀 *EARLY MOMENTUM — pump.fun*\n\n"
        f"*${token.symbol}* — {token.name}\n"
        f"MCap: *{mcap_str}* ✅ | Liq: {liq_str}\n"
        f"Vol 1h: {vol_str}\n"
        f"CA: `{token.ca}`\n\n"
        f"⚡ _Hit $50k+ fast — early entry window open_"
    )


def format_graduation_alert(token: LiveToken) -> str:
    mcap_str = f"${token.market_cap:,.0f}" if token.market_cap else "?"
    liq_str  = f"${token.liquidity:,.0f}"  if token.liquidity  else "?"
    return (
        f"🎓 *GRADUATED to Raydium* ✅\n\n"
        f"*${token.symbol}* — {token.name}\n"
        f"MCap: {mcap_str} | Liq: {liq_str}\n"
        f"CA: `{token.ca}`\n\n"
        f"⚡ _Early entry window open — now tradeable on Raydium & Axiom_"
    )

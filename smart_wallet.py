# smart_wallet.py — Smart Wallet Tracker + Auto-Discovery
# Monitors wallets via Solana RPC (free, no paid API needed).
# Detection method: poll token accounts every 30s, diff new mints = buy.
# Wallets persisted in Redis — survive Railway deploys.

import asyncio
import aiohttp
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

SOLANA_RPC  = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
WALLETS_KEY = "axiom:smart_wallets"
TOKEN_PROG  = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# ── Redis import (shared singleton) ───────────────────────────
try:
    from vector_memory import _redis as _mem_redis
    _REDIS_OK = _mem_redis.ok
except Exception:
    _mem_redis = None
    _REDIS_OK  = False


# ── Wallet performance record ─────────────────────────────────
class WalletStats:
    def __init__(self, address: str):
        self.address   = address
        self.wins      = 0
        self.losses    = 0
        self.total_pnl = 0.0
        self.last_seen = 0.0

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        return round(self.wins / total * 100, 1) if total > 0 else 50.0

    def to_dict(self) -> dict:
        return {
            "address":   self.address,
            "wins":      self.wins,
            "losses":    self.losses,
            "total_pnl": self.total_pnl,
            "last_seen": self.last_seen,
        }

    @staticmethod
    def from_dict(d: dict) -> "WalletStats":
        w = WalletStats(d["address"])
        w.wins      = d.get("wins", 0)
        w.losses    = d.get("losses", 0)
        w.total_pnl = d.get("total_pnl", 0.0)
        w.last_seen = d.get("last_seen", 0.0)
        return w


# ── Smart Wallet Registry (Redis-backed) ──────────────────────
class SmartWalletRegistry:
    def __init__(self):
        self.wallets: dict[str, WalletStats] = {}
        self._load()

    def _load(self):
        loaded = False
        if _REDIS_OK and _mem_redis:
            try:
                data = _mem_redis.get_json(WALLETS_KEY, []) or []
                for d in data:
                    ws = WalletStats.from_dict(d)
                    self.wallets[ws.address] = ws
                if data:
                    loaded = True
                    print(f"[SmartWallet] Loaded {len(self.wallets)} wallets from Redis")
            except Exception as exc:
                print(f"[SmartWallet] Redis load error: {exc}")

        if not loaded:
            raw = os.getenv("SMART_WALLETS", "[]")
            try:
                env_wallets = json.loads(raw)
            except Exception:
                env_wallets = [w.strip() for w in raw.split(",") if w.strip()]
            for addr in env_wallets:
                if addr not in self.wallets:
                    self.wallets[addr] = WalletStats(addr)
            if env_wallets:
                self._save()
                print(f"[SmartWallet] Seeded {len(env_wallets)} wallets from env var")

    def _save(self):
        data = [w.to_dict() for w in self.wallets.values()]
        if _REDIS_OK and _mem_redis:
            try:
                _mem_redis.set_json(WALLETS_KEY, data)
            except Exception as exc:
                print(f"[SmartWallet] Redis save error: {exc}")

    def add(self, address: str):
        if address not in self.wallets:
            self.wallets[address] = WalletStats(address)
            self._save()
            print(f"[SmartWallet] Added + saved: {address[:8]}...")

    def remove(self, address: str):
        if address in self.wallets:
            del self.wallets[address]
            self._save()

    def record_result(self, address: str, pnl_pct: float):
        if address in self.wallets:
            w = self.wallets[address]
            w.total_pnl += pnl_pct
            if pnl_pct > 5:   w.wins   += 1
            elif pnl_pct < -5: w.losses += 1
            self._save()

    def top_wallets(self, n: int = 10) -> list[WalletStats]:
        return sorted(self.wallets.values(), key=lambda w: w.win_rate, reverse=True)[:n]

    def all_addresses(self) -> list[str]:
        return list(self.wallets.keys())


# ── Solana Wallet Monitor ─────────────────────────────────────
class SolanaWalletMonitor:
    """
    Detects token buys by polling getTokenAccountsByOwner every 30s.
    When a new mint appears in the wallet → it's a buy → fire alert.
    Works on the free public Solana RPC — no Birdeye wallet API needed.
    """

    def __init__(self, registry: SmartWalletRegistry, on_buy_cb):
        self.registry  = registry
        self.on_buy    = on_buy_cb
        # Previous snapshot of token mints per wallet: {wallet: set(mint...)}
        self._holdings: dict[str, set] = {}
        # Dedup: {wallet:ca} → timestamp, suppress repeat alerts within 10min
        self._alerted:  dict[str, float] = {}

    async def _get_token_holdings(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> dict[str, float]:
        """
        Returns {mint_ca: ui_amount} for all token accounts with >0 balance.
        Uses getTokenAccountsByOwner (free RPC, no rate limit issues).
        """
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method":  "getTokenAccountsByOwner",
            "params":  [
                wallet,
                {"programId": TOKEN_PROG},
                {"encoding": "jsonParsed"},
            ],
        }
        try:
            async with session.post(
                SOLANA_RPC, json=payload,
                timeout=aiohttp.ClientTimeout(total=12),
            ) as r:
                data     = await r.json(content_type=None)
                accounts = (data.get("result") or {}).get("value", [])

            holdings = {}
            for acct in accounts:
                info = (acct.get("account", {}) or {}) \
                           .get("data", {}).get("parsed", {}) \
                           .get("info", {})
                mint   = info.get("mint", "")
                amt    = float(
                    (info.get("tokenAmount") or {}).get("uiAmount") or 0
                )
                if mint and amt > 0:
                    holdings[mint] = amt
            return holdings

        except Exception as exc:
            print(f"[SmartWallet] RPC error for {wallet[:8]}: {exc}")
            return {}

    async def _get_recent_buy_details(
        self, session: aiohttp.ClientSession, wallet: str, mint: str
    ) -> dict:
        """
        Try to find approximate SOL spent by looking at recent signatures.
        Best-effort — returns 0 if it can't parse.
        """
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method":  "getSignaturesForAddress",
            "params":  [wallet, {"limit": 5}],
        }
        try:
            async with session.post(
                SOLANA_RPC, json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json(content_type=None)
                sigs = data.get("result", [])

            for sig_obj in sigs[:3]:
                if sig_obj.get("err"):
                    continue
                sig = sig_obj.get("signature", "")
                tx_payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method":  "getTransaction",
                    "params":  [sig, {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                    }],
                }
                async with session.post(
                    SOLANA_RPC, json=tx_payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    tx_data = await r.json(content_type=None)

                result = tx_data.get("result") or {}
                meta   = result.get("meta")   or {}

                # Check if this tx involves our mint
                post_balances = meta.get("postTokenBalances", [])
                mints_in_tx   = {b.get("mint") for b in post_balances}
                if mint not in mints_in_tx:
                    continue

                # Estimate SOL spent from pre/post SOL balances
                pre_sol  = meta.get("preBalances",  [0])
                post_sol = meta.get("postBalances", [0])
                if pre_sol and post_sol:
                    sol_change = (pre_sol[0] - post_sol[0]) / 1e9  # lamports → SOL
                    if sol_change > 0:
                        return {"sol_spent": round(sol_change, 4), "sig": sig}

        except Exception:
            pass
        return {}

    async def monitor_loop(self):
        """Poll every 30s. New mint in wallet = buy alert."""
        print(f"[SmartWallet] Starting token-account monitor for "
              f"{len(self.registry.all_addresses())} wallet(s)")

        async with aiohttp.ClientSession() as session:
            while True:
                addresses = self.registry.all_addresses()

                for wallet in addresses:
                    try:
                        holdings = await self._get_token_holdings(session, wallet)
                        new_mints = set(holdings.keys())

                        if wallet not in self._holdings:
                            # First poll — record baseline, no alert
                            self._holdings[wallet] = new_mints
                            print(f"[SmartWallet] Baseline {wallet[:8]}: "
                                  f"{len(new_mints)} tokens held")
                            continue

                        prev_mints  = self._holdings[wallet]
                        appeared    = new_mints - prev_mints   # new tokens since last poll
                        self._holdings[wallet] = new_mints

                        for mint in appeared:
                            # Dedup
                            key = f"{wallet}:{mint}"
                            if time.time() - self._alerted.get(key, 0) < 600:
                                continue
                            self._alerted[key] = time.time()

                            # Try to get SOL amount from recent tx
                            details = await self._get_recent_buy_details(
                                session, wallet, mint
                            )
                            sol_spent = details.get("sol_spent", 0)

                            wallet_stats = self.registry.wallets.get(wallet)
                            await self.on_buy({
                                "wallet":   wallet,
                                "ca":       mint,
                                "symbol":   "?",   # enriched in orchestrator callback
                                "amount":   sol_spent,
                                "currency": "SOL",
                                "win_rate": wallet_stats.win_rate if wallet_stats else 50.0,
                                "sig":      details.get("sig", ""),
                            })
                            print(f"[SmartWallet] 🔔 NEW BUY: {wallet[:8]} → {mint[:12]} "
                                  f"({sol_spent:.3f} SOL)")

                        # Stagger wallet polls slightly to avoid RPC burst
                        await asyncio.sleep(1)

                    except Exception as exc:
                        print(f"[SmartWallet] monitor error {wallet[:8]}: {exc}")

                await asyncio.sleep(30)


# ── Auto-Discovery: find smart wallets from DexScreener ───────
class WalletAutoDiscovery:
    """
    Scans DexScreener top boosted tokens and adds their top holders
    as potential smart wallets to track.
    """

    BOOST_URL = "https://api.dexscreener.com/token-boosts/latest/v1"

    def __init__(self, registry: SmartWalletRegistry):
        self.registry = registry

    async def discover(self, session: aiohttp.ClientSession):
        try:
            async with session.get(
                self.BOOST_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data  = await r.json(content_type=None)
                items = data if isinstance(data, list) else []

            solana = [i for i in items if i.get("chainId") == "solana"][:5]
            print(f"[AutoDiscover] Checking {len(solana)} top Solana tokens")

            for token in solana:
                ca = token.get("tokenAddress", "")
                if not ca:
                    continue
                await self._extract_top_holders(session, ca)

        except Exception as exc:
            print(f"[AutoDiscover] error: {exc}")

    async def _extract_top_holders(self, session: aiohttp.ClientSession, ca: str):
        """Use getTokenLargestAccounts to find top holders, then resolve to wallet."""
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method":  "getTokenLargestAccounts",
            "params":  [ca],
        }
        try:
            async with session.post(
                SOLANA_RPC, json=payload,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                data     = await r.json(content_type=None)
                accounts = (data.get("result") or {}).get("value", [])

            # Resolve token accounts to wallet owners
            for acct in accounts[1:6]:   # skip #1 = usually LP
                token_acct = acct.get("address", "")
                if not token_acct:
                    continue

                info_payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method":  "getAccountInfo",
                    "params":  [token_acct, {"encoding": "jsonParsed"}],
                }
                async with session.post(
                    SOLANA_RPC, json=info_payload,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as r:
                    info_data = await r.json(content_type=None)

                owner = (
                    (info_data.get("result") or {})
                    .get("value", {}).get("data", {})
                    .get("parsed", {}).get("info", {})
                    .get("owner", "")
                )
                if owner and len(owner) > 30 and owner not in self.registry.wallets:
                    self.registry.add(owner)
                    print(f"[AutoDiscover] Found wallet: {owner[:8]}...")

        except Exception:
            pass

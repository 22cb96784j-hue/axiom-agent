# my_wallet_tracker.py
# Polls getSignaturesForAddress every 5s — catches ALL trades including Jupiter/Axiom routes.
# Replaces unreliable logsSubscribe WebSocket which misses most DEX transactions.
# Supports partial sells: fires on_sell with pct_sold so orchestrator can handle accordingly.

import asyncio
import json
import time
from typing import Optional, Callable, Awaitable

import aiohttp

# ── Solana RPC endpoints (rotate on 429) ─────────────────────
RPCS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-api.projectserum.com",
]

# ── Known DEX / swap program IDs (for logging only — not used as a filter) ────
# Axiom Pro and other aggregators wrap these or use their own router programs,
# so we do NOT gate on this list. Token balance changes are sufficient.
DEX_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # pump.fun AMM (classic)
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # pump.fun AMM v2
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CLMM
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",  # Jupiter v6
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  # Orca Whirlpool
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",  # Meteora DLMM
    "AXiomFqX5Z6mPKRVLSJa4RDDaRqnvDQDPPzqcBrVkRj",  # Axiom Pro router (if any)
}

WSOL      = "So11111111111111111111111111111111111111112"
LAMPORTS  = 1_000_000_000
REDIS_KEY = "axiom:my_wallet"
POLL_SECS = 5   # Check for new transactions every 5 seconds


class MyWalletTracker:
    """
    Detects the user's own Solana wallet buys, full exits, and partial exits.

    on_buy(wallet, ca, sol_spent)               — new position opened
    on_sell(wallet, ca, sol_received, pct_sold) — partial (pct_sold<0.9) or full exit (>=0.9)
    """

    def __init__(
        self,
        redis,
        on_buy:  Callable[[str, str, float],         Awaitable[None]],
        on_sell: Callable[[str, str, float, float],  Awaitable[None]],
    ):
        self._redis   = redis
        self._on_buy  = on_buy
        self._on_sell = on_sell
        self._wallet:  Optional[str] = None
        self._running  = False
        self._seen_sigs: set = set()
        self._rpc_idx  = 0
        self._warmed_up = False   # True after first poll bookmarks existing sigs

    # ── RPC rotation ───────────────────────────────────────────
    @property
    def _rpc(self) -> str:
        return RPCS[self._rpc_idx % len(RPCS)]

    def _rotate_rpc(self):
        self._rpc_idx += 1
        print(f"[MyWallet] Rotating RPC → {self._rpc}")

    # ── Wallet management ──────────────────────────────────────
    def load(self) -> Optional[str]:
        """Load wallet from Redis. Call in __init__ before startup message."""
        val = self._redis.get_json(REDIS_KEY)
        if val and isinstance(val, str) and len(val) >= 32:
            self._wallet = val.strip()
            print(f"[MyWallet] Loaded wallet: {self._wallet[:8]}...")
        return self._wallet

    def set_wallet(self, address: str):
        """Set wallet. Clears seen-sig cache and resets warm-up so next poll bookmarks fresh."""
        address = address.strip()
        self._wallet = address if len(address) >= 32 else None
        self._redis.set_json(REDIS_KEY, self._wallet or "")
        self._seen_sigs.clear()
        self._warmed_up = False   # Force re-bookmark on next poll
        print(f"[MyWallet] Wallet {'set: ' + address[:8] + '...' if self._wallet else 'cleared'}")

    def get_wallet(self) -> Optional[str]:
        return self._wallet

    # ── Main poll loop ─────────────────────────────────────────
    async def run(self):
        # Note: load() is called in orchestrator __init__ before run() starts.
        self._running = True
        print(f"[MyWallet] Polling mode active — checking every {POLL_SECS}s")
        while self._running:
            if not self._wallet:
                await asyncio.sleep(10)
                continue
            try:
                await self._poll_once(self._wallet)
            except Exception as exc:
                print(f"[MyWallet] Poll error: {exc}")
            await asyncio.sleep(POLL_SECS)

    async def _poll_once(self, wallet: str):
        """Fetch recent signatures and process new ones sequentially oldest-first."""
        # getSignaturesForAddress returns newest-first — we reverse to process oldest-first
        # so buy is always processed before sell within the same session.
        sigs = await self._get_recent_sigs(wallet)   # newest → oldest

        if not self._warmed_up:
            # ── WARM-UP: first poll after deploy/startup ──────────────
            # Just bookmark all current sigs as "seen" WITHOUT processing them.
            # This prevents re-firing old trades that happened before this deploy.
            for sig in sigs:
                self._seen_sigs.add(sig)
                self._redis.set_json(f"axiom:seen_sig:{sig}", 1)
            self._warmed_up = True
            print(f"[MyWallet] Warm-up complete — bookmarked {len(sigs)} existing sigs, watching for NEW trades only")
            return

        # ── NORMAL POLL: find genuinely new sigs ─────────────────────
        new_sigs = []
        for sig in sigs:
            if sig in self._seen_sigs:
                break   # Everything after this is already known (list is newest-first)
            rkey = f"axiom:seen_sig:{sig}"
            if self._redis.get_json(rkey):
                self._seen_sigs.add(sig)
                break
            new_sigs.append(sig)

        if not new_sigs:
            return

        # Mark all as seen before processing (prevents double-fire on retry)
        for sig in new_sigs:
            self._redis.set_json(f"axiom:seen_sig:{sig}", 1)
            self._seen_sigs.add(sig)
        if len(self._seen_sigs) > 500:
            self._seen_sigs = set(list(self._seen_sigs)[-200:])

        # Process OLDEST first (reverse of API order) — buy before sell, correct ordering
        for sig in reversed(new_sigs):
            await self._handle_sig(sig, wallet)   # sequential, not concurrent

    # ── RPC calls ──────────────────────────────────────────────
    async def _get_recent_sigs(self, wallet: str, limit: int = 15) -> list[str]:
        """Return list of recent confirmed tx signatures for the wallet."""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    self._rpc,
                    json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getSignaturesForAddress",
                        "params": [wallet, {"limit": limit, "commitment": "confirmed"}],
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 429:
                        self._rotate_rpc()
                        return []
                    data = await r.json()
            result = data.get("result") or []
            # Skip errored transactions
            return [item["signature"] for item in result if not item.get("err")]
        except Exception as exc:
            print(f"[MyWallet] getSignaturesForAddress error: {exc}")
            return []

    async def _fetch_tx(self, sig: str) -> Optional[dict]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    self._rpc,
                    json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getTransaction",
                        "params": [sig, {
                            "encoding":                       "jsonParsed",
                            "commitment":                     "confirmed",
                            "maxSupportedTransactionVersion": 0,
                        }],
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as r:
                    data = await r.json()
            return data.get("result")
        except Exception as exc:
            print(f"[MyWallet] fetchTx error {sig[:12]}: {exc}")
            return None

    # ── Transaction processing ─────────────────────────────────
    async def _handle_sig(self, sig: str, wallet: str):
        await asyncio.sleep(1.5)   # Brief buffer for RPC indexing
        try:
            tx = await self._fetch_tx(sig)
            if tx:
                await self._parse_swap(tx, wallet)
        except Exception as exc:
            print(f"[MyWallet] parse error {sig[:12]}: {exc}")

    async def _parse_swap(self, tx: dict, wallet: str):
        meta = tx.get("meta", {})
        if meta.get("err"):
            return   # Failed tx — skip

        # Flatten account keys
        keys_raw = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
        accounts = [
            (k["pubkey"] if isinstance(k, dict) else str(k))
            for k in keys_raw
        ]

        # Log which DEX programs are involved (for debugging), but DON'T filter.
        # Axiom Pro uses aggregator routing — the known DEX program may not appear
        # directly in the accounts list. Token balance changes are the real signal.
        has_known_dex = any(p in accounts for p in DEX_PROGRAMS)
        print(f"[MyWallet] tx accounts={len(accounts)} known_dex={has_known_dex}")

        # Find wallet position in account list
        try:
            widx = accounts.index(wallet)
        except ValueError:
            return

        # Net SOL change for the wallet (fee added back = actual economic cost/receipt)
        pre_bals  = meta.get("preBalances",  [])
        post_bals = meta.get("postBalances", [])
        if widx >= len(pre_bals) or widx >= len(post_bals):
            return

        fee       = meta.get("fee", 0)
        sol_delta = (post_bals[widx] - pre_bals[widx] + fee) / LAMPORTS
        # sol_delta < 0 → spent SOL (buy)
        # sol_delta > 0 → received SOL (sell)

        # Build token balance maps
        pre_tok  = {t["accountIndex"]: t for t in (meta.get("preTokenBalances")  or [])}
        post_tok = {t["accountIndex"]: t for t in (meta.get("postTokenBalances") or [])}
        all_idx  = set(pre_tok) | set(post_tok)

        # Collect ALL candidates — pick the one with the largest % position change.
        # This prevents false positives from dust/LP side-effects in multi-token txs.
        best_buy:  Optional[tuple] = None   # (ca, sol_spent, pct_gained)
        best_sell: Optional[tuple] = None   # (ca, sol_recv,  pct_of_position_sold, pct_change)

        for idx in all_idx:
            pre_e  = pre_tok.get(idx, {})
            post_e = post_tok.get(idx, {})

            owner = post_e.get("owner") or pre_e.get("owner", "")
            if owner != wallet:
                continue

            ca = post_e.get("mint") or pre_e.get("mint", "")
            if not ca or ca == WSOL:
                continue

            pre_amt   = float((pre_e.get("uiTokenAmount")  or {}).get("uiAmount") or 0)
            post_amt  = float((post_e.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            tok_delta = post_amt - pre_amt

            if tok_delta > 0 and sol_delta < -0.001:
                # BUY candidate: received tokens, paid SOL
                pct = (tok_delta / pre_amt) if pre_amt > 0 else float("inf")
                if pct >= 0.01 and (best_buy is None or pct > best_buy[2]):
                    best_buy = (ca, round(abs(sol_delta), 5), pct)

            elif tok_delta < 0 and sol_delta > 0.001:
                # SELL candidate: gave tokens, received SOL
                # pct_sold = fraction of pre-balance sold (key for partial vs full detection)
                pct_sold = (abs(tok_delta) / pre_amt) if pre_amt > 0 else 1.0
                if pct_sold >= 0.01 and (best_sell is None or pct_sold > best_sell[3]):
                    best_sell = (ca, round(sol_delta, 5), pct_sold, pct_sold)

        # ── Fire the best match ────────────────────────────────
        if best_buy:
            ca, sol_spent, pct = best_buy
            print(f"[MyWallet] 🟢 BUY {sol_spent:.4f} SOL → {ca[:16]}... ({pct:.0%} pos change)")
            await self._on_buy(wallet, ca, sol_spent)

        elif best_sell:
            ca, sol_recv, pct_sold, _ = best_sell
            is_full = pct_sold >= 0.90
            label   = "FULL EXIT" if is_full else f"PARTIAL EXIT ({pct_sold:.0%})"
            print(f"[MyWallet] 🔴 SELL {ca[:16]}... → {sol_recv:.4f} SOL [{label}]")
            await self._on_sell(wallet, ca, sol_recv, pct_sold)

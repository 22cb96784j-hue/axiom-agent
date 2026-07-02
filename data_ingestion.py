# data_ingestion.py — Real-time feed aggregator + Solana on-chain verification
# Modules: PulseFeedListener, DiscoverPoller, WalletTracker,
#          TwitterSentimentPipeline, OnChainVerifier, SolanaRPCClient, DataAggregator

import asyncio
import aiohttp
import json
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

AXIOM_BASE   = os.getenv("AXIOM_BASE_URL", "https://api.axiom.trade/v1")
AXIOM_WS     = os.getenv("AXIOM_WS_URL", "wss://stream.axiom.trade/pulse")
AXIOM_KEY    = os.getenv("AXIOM_API_KEY", "")
GOPLUS_BASE  = "https://api.gopluslabs.io/api/v1"
SOLANA_RPC   = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
BIRDEYE_KEY  = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_BASE = "https://public-api.birdeye.so"


# ── Token data model ──────────────────────────────────────────
@dataclass
class TokenSnapshot:
    ca: str                           # Contract / mint address
    symbol: str
    price_usd: float
    volume_5m: float
    volume_15m: float
    volume_1h: float
    liquidity_usd: float
    market_cap: float
    buy_txns_15m: int
    sell_txns_15m: int
    # Supply distribution (on-chain verified)
    top10_wallet_pct: float
    holder_count: int
    creator_percent: float
    can_take_back_ownership: bool
    # LP info
    lp_locked: bool
    lp_lock_provider: str
    lp_lock_expiry_days: int
    # Contract security (GoPlus)
    is_honeypot: bool
    has_mint_function: bool
    has_blacklist: bool
    # Social signals
    twitter_mentions_10m: int
    twitter_sentiment_score: float    # -1.0 to +1.0
    # Smart money
    smart_wallet_buys_1h: float       # USD value
    timestamp: float = field(default_factory=time.time)


# ── GoPlus Security API ───────────────────────────────────────
class OnChainVerifier:
    """
    GoPlus Labs — free, no API key required for Solana.
    Returns honeypot / mint / blacklist / LP lock status.
    Docs: https://docs.gopluslabs.io/reference/api-overview
    """
    CHAIN = "solana"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def check(self, ca: str) -> dict:
        url = f"{GOPLUS_BASE}/token_security/{self.CHAIN}"
        try:
            async with self.session.get(
                url,
                params={"contract_addresses": ca},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                data = await r.json(content_type=None)
                if not isinstance(data, dict):
                    return {}
                result_map = data.get("result") or {}
                # GoPlus may key by lowercase or original address
                result = (result_map.get(ca.lower())
                          or result_map.get(ca)
                          or {})
                if not isinstance(result, dict):
                    return {}
                return {
                    "is_honeypot":             result.get("honeypot", "0") == "1",
                    "has_mint_function":       result.get("can_be_minted", "0") == "1",
                    "has_blacklist":           result.get("is_blacklisted", "0") == "1",
                    "creator_percent":         float(result.get("creator_percent") or 0),
                    "holder_count":            int(result.get("holder_count") or 0),
                    "can_take_back_ownership": result.get("can_take_back_ownership", "0") == "1",
                    "lp_locked":               result.get("lp_locked_percent", "0") != "0",
                }
        except Exception as exc:
            print(f"[GoPlus] {ca[:8]}: {exc}")
            return {}


# ── Solana RPC ────────────────────────────────────────────────
class SolanaRPCClient:
    """Direct Solana JSON-RPC for top-holder concentration."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def top10_concentration(self, mint_ca: str, total_supply: float) -> float:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [mint_ca],
        }
        try:
            async with self.session.post(
                SOLANA_RPC, json=payload, timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                data     = await r.json()
                accounts = data.get("result", {}).get("value", [])
                top10    = sum(float(a.get("uiAmount", 0)) for a in accounts[:10])
                return round(top10 / max(total_supply, 1) * 100, 2)
        except Exception:
            return 0.0


# ── Axiom Pulse WebSocket ─────────────────────────────────────
class PulseFeedListener:
    """Monitors pump.fun → Raydium migrations in real-time."""

    def __init__(self, on_migration_cb):
        self.cb      = on_migration_cb
        self.running = False

    async def listen(self):
        self.running = True
        while self.running:
            try:
                async with websockets.connect(
                    AXIOM_WS,
                    extra_headers={"Authorization": f"Bearer {AXIOM_KEY}"},
                ) as ws:
                    await ws.send(json.dumps({"subscribe": "migrations"}))
                    async for raw in ws:
                        event = json.loads(raw)
                        if event.get("type") == "migration":
                            await self.cb(event["data"])
            except Exception as exc:
                print(f"[PulseFeed] reconnect: {exc}")
                await asyncio.sleep(3)


# ── Birdeye Scanner (primary live data source) ────────────────
class BirdeyeScanner:
    """
    Birdeye public API — real-time Solana token data.
    Requires BIRDEYE_API_KEY env var (free tier: 50k req/month).
    Docs: https://docs.birdeye.so/reference/get_defi-tokenlist
    """

    TRENDING_URL  = f"{BIRDEYE_BASE}/defi/trending_tokens"
    TOKENLIST_URL = f"{BIRDEYE_BASE}/defi/tokenlist"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            "X-API-KEY": BIRDEYE_KEY,
            "x-chain":   "solana",
            "accept":    "application/json",
        }

    async def fetch_trending(self, limit: int = 30) -> list[dict]:
        """Fetch trending Solana tokens sorted by 24h price change."""
        try:
            async with self.session.get(
                self.TRENDING_URL,
                headers=self.headers,
                params={"chain": "solana"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data  = await r.json(content_type=None)
                items = data.get("data", {}).get("tokens", []) if isinstance(data, dict) else []
                return items[:limit]
        except Exception as exc:
            print(f"[Birdeye] trending error: {exc}")
            return []

    async def fetch_movers(self, limit: int = 50, min_liq: float = 5_000, min_vol5m: float = 200) -> list[dict]:
        """
        Fetch trending Solana tokens via Birdeye free-tier endpoints.
        Uses /defi/trending_tokens (free) then enriches with /defi/token_overview.
        Falls back to tokenlist if trending is unavailable.
        """
        token_addresses = []

        # ── Try trending endpoint (free tier) ─────────────────
        try:
            async with self.session.get(
                self.TRENDING_URL,
                headers=self.headers,
                params={"chain": "solana"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data   = await r.json(content_type=None)
                items  = (data.get("data", {}) or {}).get("tokens", []) if isinstance(data, dict) else []
                token_addresses = [t.get("address") for t in items if t.get("address")][:limit]
                print(f"[Birdeye] trending returned {len(token_addresses)} addresses")
        except Exception as exc:
            print(f"[Birdeye] trending error: {exc}")

        # ── Fallback: tokenlist (may need higher tier) ────────
        if not token_addresses:
            try:
                async with self.session.get(
                    self.TOKENLIST_URL,
                    headers=self.headers,
                    params={"sort_by": "v24hChangePercent", "sort_type": "desc",
                            "offset": 0, "limit": 50, "chain": "solana"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data  = await r.json(content_type=None)
                    items = (data.get("data", {}) or {}).get("tokens", []) if isinstance(data, dict) else []
                    token_addresses = [t.get("address") for t in items if t.get("address")][:limit]
                    print(f"[Birdeye] tokenlist returned {len(token_addresses)} addresses")
            except Exception as exc:
                print(f"[Birdeye] tokenlist error: {exc}")

        if not token_addresses:
            print("[Birdeye] 0 Solana tokens fetched")
            return []

        # ── Enrich each address with token_overview ────────────
        results = []
        OVERVIEW_URL = f"{BIRDEYE_BASE}/defi/token_overview"
        for addr in token_addresses[:limit]:
            try:
                async with self.session.get(
                    OVERVIEW_URL,
                    headers=self.headers,
                    params={"address": addr},
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as r:
                    d = (await r.json(content_type=None) or {}).get("data", {}) or {}
                liq  = float(d.get("liquidity", 0) or 0)
                vol5 = float(d.get("v5mUSD", 0) or 0)
                if liq < min_liq:
                    continue
                results.append({
                    "contract_address": addr,
                    "symbol":           d.get("symbol", "?"),
                    "price_usd":        float(d.get("price", 0) or 0),
                    "volume_5m":        vol5,
                    "volume_15m":       float(d.get("v15mUSD", 0) or 0),
                    "volume_1h":        float(d.get("v1hUSD", 0) or 0),
                    "liquidity_usd":    liq,
                    "market_cap":       float(d.get("mc", 0) or 0),
                    "buy_txns_15m":     int(d.get("trade15m", 0) or 0),
                    "sell_txns_15m":    0,
                    "top10_wallet_pct": 0,
                    "holder_count":     int(d.get("holder", 0) or 0),
                    "lp_locked":        False,
                    "is_honeypot":      False,
                    "total_supply":     0,
                    "chg5m":            float(d.get("priceChange5mPercent", 0) or 0),
                    "chg1h":            float(d.get("priceChange1hPercent", 0) or 0),
                    "chg24h":           float(d.get("priceChange24hPercent", 0) or 0),
                    "name":             d.get("name", ""),
                })
            except Exception:
                continue

        print(f"[Birdeye] {len(results)} Solana tokens fetched")
        return results

    async def fetch_high_gainers(
        self,
        min_chg1h: float = 50.0,   # minimum 1h price gain %
        max_age_h: float = 6.0,    # only tokens <6h old
        max_mcap:  float = 5_000_000, # gems up to $5M MCap
        min_vol1h: float = 3_000,  # must have $3k+ volume in 1h
    ) -> list[dict]:
        """
        Flash gem scanner — finds tokens pumping >50% in 1h that are <6h old.
        Uses DexScreener (free, no API key) as primary source — Birdeye tokenlist
        requires a paid plan so we don't use it here.

        Strategy:
        1. Fetch latest boosted + profiled Solana tokens from DexScreener
        2. Batch-fetch their pair data (up to 30 CAs per request)
        3. Filter: h1 gain > threshold, age < max_age_h, mcap in range
        """
        DEXS_HEADERS  = {"User-Agent": "AxiomAIAgent/2.0"}
        BOOST_URL     = "https://api.dexscreener.com/token-boosts/latest/v1"
        PROFILES_URL  = "https://api.dexscreener.com/token-profiles/latest/v1"
        TOKENS_URL    = "https://api.dexscreener.com/latest/dex/tokens"

        # ── Step 1: gather candidate CAs ──────────────────────────
        ca_list: list[str] = []
        for url, label in [(BOOST_URL, "boosts"), (PROFILES_URL, "profiles")]:
            try:
                async with self.session.get(
                    url, headers=DEXS_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    items = await r.json(content_type=None)
                    if isinstance(items, list):
                        added = [
                            i["tokenAddress"] for i in items
                            if i.get("chainId") == "solana" and i.get("tokenAddress")
                        ]
                        ca_list.extend(added)
                        print(f"[FlashScan] {len(added)} {label} candidates")
            except Exception as exc:
                print(f"[FlashScan] {label} fetch error: {exc}")

        # Deduplicate while preserving order
        seen_set: set[str] = set()
        unique_cas = [c for c in ca_list if c not in seen_set and not seen_set.add(c)]  # type: ignore

        if not unique_cas:
            print("[Birdeye/Flash] 0 high-gain gems found — no DexScreener candidates")
            return []

        # ── Step 2: batch-fetch pair data, filter high gainers ───
        results:  list[dict] = []
        seen_res: set[str]   = set()
        now_ts               = time.time()
        BATCH                = 29   # DexScreener supports up to 30 CAs per call

        for i in range(0, min(len(unique_cas), 120), BATCH):
            batch    = unique_cas[i : i + BATCH]
            ca_param = ",".join(batch)
            try:
                async with self.session.get(
                    f"{TOKENS_URL}/{ca_param}",
                    headers=DEXS_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data  = await r.json(content_type=None)
                pairs = data.get("pairs", []) if isinstance(data, dict) else []
            except Exception as exc:
                print(f"[FlashScan] batch fetch error: {exc}")
                continue

            for p in pairs:
                if p.get("chainId") != "solana":
                    continue
                base = p.get("baseToken", {})
                ca   = base.get("address", "")
                if not ca or ca in seen_res:
                    continue
                seen_res.add(ca)

                mc    = float(p.get("marketCap") or p.get("fdv") or 0)
                if mc == 0 or mc > max_mcap:
                    continue

                chg1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                if chg1h < min_chg1h:
                    continue

                vol   = p.get("volume", {})
                vol1h = float(vol.get("h1", 0) or 0)
                if vol1h < min_vol1h:
                    continue

                created = p.get("pairCreatedAt", 0) or 0  # milliseconds
                age_h   = (now_ts - created / 1000) / 3600 if created else 99
                if age_h > max_age_h:
                    continue

                liq  = float((p.get("liquidity") or {}).get("usd", 0) or 0)
                txns = p.get("txns", {})
                chg5m = float((p.get("priceChange") or {}).get("m5", 0) or 0)

                results.append({
                    "contract_address": ca,
                    "symbol":           base.get("symbol", "?"),
                    "price_usd":        float(p.get("priceUsd") or 0),
                    "volume_1h":        vol1h,
                    "volume_5m":        float(vol.get("m5", 0) or 0),
                    "liquidity_usd":    liq,
                    "market_cap":       mc,
                    "chg1h":            chg1h,
                    "chg5m":            chg5m,
                    "age_hours":        round(age_h, 2),
                    "pair_created_at":  created,
                    "buy_txns_15m":     int((txns.get("m5") or {}).get("buys", 0)) * 3,
                    "sell_txns_15m":    int((txns.get("m5") or {}).get("sells", 0)) * 3,
                    "holder_count":     0,
                })

        # Sort by 1h gain descending
        results.sort(key=lambda x: x["chg1h"], reverse=True)
        print(f"[Birdeye/Flash] {len(results)} high-gain gems found (>{min_chg1h:.0f}% 1h, <{max_age_h:.0f}h old)")
        return results


# ── DexScreener Poller (fallback) ─────────────────────────────
class DiscoverPoller:
    """
    Polls DexScreener for NEW Solana memecoins using boosted tokens
    and latest profile endpoints. Free public API — no key required.

    Key improvements:
    - Uses /token-boosts and /token-profiles to find genuinely new tokens
      (not established coins like $PUMP at $526M or $SOL)
    - Deduplicates by BASE TOKEN CA so the same token never appears twice
    - Filters MCap > $100M — those are established tokens, not fresh gems
    - Blacklists known major symbols
    """

    SEARCH_URL   = "https://api.dexscreener.com/latest/dex/search"
    BOOST_URL    = "https://api.dexscreener.com/token-boosts/latest/v1"
    PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
    TOKENS_URL   = "https://api.dexscreener.com/latest/dex/tokens"
    HEADERS      = {"User-Agent": "AxiomAIAgent/2.0"}

    # Established tokens to always skip — not fresh memecoins
    _SKIP_SYMBOLS = {
        "PUMP", "SOL", "WSOL", "WBTC", "BTC", "ETH", "WETH",
        "USDC", "USDT", "USDS", "DAI", "BUSD",
        "BONK", "WIF", "JUP", "RAY", "ORCA", "MSOL", "BSOL", "JSOL",
        "PYTH", "W", "JITO", "MOON", "RENDER", "HNT", "MOBILE",
    }

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _pair_to_token_dict(self, p: dict) -> Optional[dict]:
        """Convert a DexScreener pair object to our standard token dict."""
        base  = p.get("baseToken", {})
        txns  = p.get("txns", {})
        vol   = p.get("volume", {})
        liq_d = p.get("liquidity") or {}
        mc    = float(p.get("marketCap") or p.get("fdv") or 0)
        liq   = float(liq_d.get("usd", 0) or 0)
        return {
            "contract_address": base.get("address", ""),
            "symbol":           base.get("symbol", "UNKNOWN"),
            "price_usd":        float(p.get("priceUsd") or 0),
            "volume_5m":        float(vol.get("m5", 0) or 0),
            "volume_15m":       float(vol.get("m5", 0) or 0) * 3,
            "volume_1h":        float(vol.get("h1", 0) or 0),
            "volume_24h":       float(vol.get("h24", 0) or 0),
            "liquidity_usd":    liq,
            "market_cap":       mc,
            "buy_txns_15m":     int((txns.get("m5") or {}).get("buys", 0)) * 3,
            "sell_txns_15m":    int((txns.get("m5") or {}).get("sells", 0)) * 3,
            "pair_created_at":  p.get("pairCreatedAt", 0),
            "top10_wallet_pct": 0,
            "holder_count":     0,
            "lp_locked":        False,
            "is_honeypot":      False,
            "total_supply":     0,
        }

    async def _fetch_gainer_cas(self) -> list[str]:
        """
        Discover top gainers via DexScreener search (free, no API key needed).
        Catches tokens like $HAAALAND that aren't in the boost feed but are
        moving hard on their own.
        Note: Birdeye tokenlist requires a paid plan — removed entirely.
        """
        cas: list[str] = []

        # ── DexScreener: search terms that catch pumping tokens ───────
        gainer_queries = ["solana pump", "new sol gem", "raydium memecoin", "sol 100x"]
        for q in gainer_queries:
            try:
                async with self.session.get(
                    self.SEARCH_URL, headers=self.HEADERS,
                    params={"q": q},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data  = await r.json(content_type=None)
                    pairs = data.get("pairs", []) if isinstance(data, dict) else []
                    for p in pairs:
                        if p.get("chainId") != "solana":
                            continue
                        base = p.get("baseToken", {})
                        ca   = base.get("address", "")
                        sym  = base.get("symbol", "").upper()
                        mc   = float(p.get("marketCap") or 0)
                        chg1h = float((p.get("priceChange") or {}).get("h1", 0) or 0)
                        # Only add if actually gaining (>20% in 1h) and micro-cap
                        if ca and sym not in self._SKIP_SYMBOLS and mc < 100_000_000 and chg1h > 20:
                            cas.append(ca)
            except Exception as exc:
                print(f"[Gainers] search error ({q}): {exc}")

        # Deduplicate
        seen: set[str] = set()
        unique = []
        for ca in cas:
            if ca not in seen:
                seen.add(ca)
                unique.append(ca)
        print(f"[Gainers] {len(unique)} unique gainer CAs discovered")
        return unique[:40]

    async def fetch_top_movers(
        self, min_volume_5m: float = 100, min_liquidity: float = 2_000
    ) -> list[dict]:
        """
        Primary: fetch newly boosted + profiled Solana tokens.
        Also fetches top gainers (high 1h price change) so pumping tokens
        like $HAAALAND are caught even if they're not being boosted.
        All results deduplicated by token CA, capped at MCap $100M.
        """
        # ── Step 1: Gather CAs from boosted + profiles endpoints ──────
        ca_list: list[str] = []
        for url, label in [
            (self.BOOST_URL,    "boosts"),
            (self.PROFILES_URL, "profiles"),
        ]:
            try:
                async with self.session.get(
                    url, headers=self.HEADERS,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    items = await r.json(content_type=None)
                    if isinstance(items, list):
                        added = [
                            i["tokenAddress"] for i in items
                            if i.get("chainId") == "solana" and i.get("tokenAddress")
                        ]
                        ca_list.extend(added)
                        print(f"[DexScreener] {len(added)} {label} tokens")
            except Exception as exc:
                print(f"[DexScreener] {label} error: {exc}")

        # ── Step 1b: Add top gainers (catches pumping tokens not in boosts) ─
        try:
            gainer_cas = await self._fetch_gainer_cas()
            ca_list.extend(gainer_cas)
        except Exception as exc:
            print(f"[Gainers] discovery error: {exc}")

        # Remove duplicates while preserving order
        seen_cas: set[str] = set()
        unique_cas: list[str] = []
        for ca in ca_list:
            if ca not in seen_cas:
                seen_cas.add(ca)
                unique_cas.append(ca)

        # ── Step 2: Fetch pair data for each CA (batch, max 30) ───────
        all_pairs: list[dict] = []
        if unique_cas:
            batch = ",".join(unique_cas[:50])
            try:
                async with self.session.get(
                    f"{self.TOKENS_URL}/{batch}",
                    headers=self.HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    data = await r.json(content_type=None)
                    all_pairs = data.get("pairs", []) if isinstance(data, dict) else []
                print(f"[DexScreener] {len(all_pairs)} pairs for {len(unique_cas)} CAs")
            except Exception as exc:
                print(f"[DexScreener] tokens batch error: {exc}")

        # ── Step 3: Fallback to search if primary endpoints failed ────
        if not all_pairs:
            return await self._fetch_via_search(min_volume_5m, min_liquidity)

        # ── Step 4: Deduplicate by CA, keep best pair per token ───────
        ca_best: dict[str, dict] = {}
        for p in all_pairs:
            if p.get("chainId") != "solana":
                continue
            base = p.get("baseToken", {})
            ca   = base.get("address", "")
            sym  = base.get("symbol", "").upper()
            if not ca or sym in self._SKIP_SYMBOLS:
                continue
            liq = float((p.get("liquidity") or {}).get("usd", 0) or 0)
            mc  = float(p.get("marketCap") or p.get("fdv") or 0)
            if liq < min_liquidity:
                continue
            if mc > 100_000_000:  # Skip $100M+ MCap — not fresh memecoins
                continue
            # Keep the pair with highest liquidity for this CA
            prev_liq = float((ca_best.get(ca, {}).get("liquidity") or {}).get("usd", 0) or 0)
            if liq > prev_liq:
                ca_best[ca] = p

        # ── Step 5: Apply volume filter and convert ───────────────────
        tokens = []
        for ca, p in ca_best.items():
            vol5 = float((p.get("volume") or {}).get("m5", 0) or 0)
            if vol5 < min_volume_5m:
                continue
            t = self._pair_to_token_dict(p)
            if t:
                tokens.append(t)

        print(f"[DexScreener] {len(tokens)} unique Solana memecoins after dedup+filter")
        return tokens[:50]

    async def _fetch_via_search(
        self, min_volume_5m: float, min_liquidity: float
    ) -> list[dict]:
        """
        Fallback: search DexScreener with memecoin-specific queries.
        Still deduplicates by CA and filters MCap / blacklist.
        """
        all_pairs: list[dict] = []
        for q in ("solana new", "raydium launch", "sol meme"):
            try:
                async with self.session.get(
                    self.SEARCH_URL, headers=self.HEADERS,
                    params={"q": q},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json(content_type=None)
                    all_pairs.extend(data.get("pairs", []))
            except Exception as exc:
                print(f"[DexScreener] search error ({q}): {exc}")

        ca_best: dict[str, dict] = {}
        for p in all_pairs:
            if p.get("chainId") != "solana":
                continue
            base = p.get("baseToken", {})
            ca   = base.get("address", "")
            sym  = base.get("symbol", "").upper()
            if not ca or sym in self._SKIP_SYMBOLS:
                continue
            liq = float((p.get("liquidity") or {}).get("usd", 0) or 0)
            mc  = float(p.get("marketCap") or p.get("fdv") or 0)
            if liq < min_liquidity or mc > 100_000_000:
                continue
            prev_liq = float((ca_best.get(ca, {}).get("liquidity") or {}).get("usd", 0) or 0)
            if liq > prev_liq:
                ca_best[ca] = p

        tokens = []
        for ca, p in ca_best.items():
            vol5 = float((p.get("volume") or {}).get("m5", 0) or 0)
            if vol5 < min_volume_5m:
                continue
            t = self._pair_to_token_dict(p)
            if t:
                tokens.append(t)

        print(f"[DexScreener] search fallback: {len(tokens)} tokens")
        return tokens[:50]


# ── Smart Wallet Tracker ──────────────────────────────────────
class WalletTracker:
    """Detects accumulation from whitelisted 'smart money' wallets."""

    def __init__(self, session: aiohttp.ClientSession, watched: list[str]):
        self.session = session
        self.wallets = watched
        self.headers = {"Authorization": f"Bearer {AXIOM_KEY}"}

    async def get_recent_buys(self, lookback_minutes: int = 60) -> dict[str, float]:
        """Returns {ca: total_usd_bought} across all watched wallets."""
        result: dict[str, float] = {}
        tasks = [self._wallet_buys(w, lookback_minutes) for w in self.wallets]
        for wallet_result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(wallet_result, dict):
                for ca, usd in wallet_result.items():
                    result[ca] = result.get(ca, 0) + usd
        return result

    async def _wallet_buys(self, wallet: str, mins: int) -> dict[str, float]:
        url    = f"{AXIOM_BASE}/wallet/{wallet}/transactions"
        params = {"since_minutes": mins, "type": "buy"}
        async with self.session.get(
            url, headers=self.headers, params=params,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            txns = (await r.json()).get("transactions", [])
            return {t["token_ca"]: t["usd_value"] for t in txns}


# ── Twitter Sentiment Pipeline ────────────────────────────────
class TwitterSentimentPipeline:
    """Pulls Axiom's integrated tweet feed and scores sentiment."""

    ENDPOINT = f"{AXIOM_BASE}/twitter/alpha-feed"
    POS = ["gem", "100x", "bullish", "buy", "moon", "accumulate", "alpha", "lfg"]
    NEG = ["rug", "scam", "honeypot", "exit", "dump", "avoid", "warning", "fake"]

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {"Authorization": f"Bearer {AXIOM_KEY}"}

    def _score(self, text: str) -> float:
        t     = text.lower()
        score = sum(1 for w in self.POS if w in t)
        score -= sum(1 for w in self.NEG if w in t)
        return max(-1.0, min(1.0, score / 3.0))

    async def fetch(self, ca: str, window: int = 10) -> tuple[int, float]:
        try:
            async with self.session.get(
                self.ENDPOINT,
                headers=self.headers,
                params={"ca": ca, "window_minutes": window},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                tweets = (await r.json()).get("tweets", [])
            if not tweets:
                return 0, 0.0
            scores = [self._score(t["text"]) for t in tweets]
            return len(tweets), round(sum(scores) / len(scores), 3)
        except Exception:
            return 0, 0.0


# ── Master Aggregator ─────────────────────────────────────────
class DataAggregator:
    """
    Orchestrates all data sources into TokenSnapshot objects.
    All I/O calls run in parallel via asyncio.gather for speed.
    """

    def __init__(self, smart_wallets: list[str]):
        self.smart_wallets = smart_wallets

    async def build_snapshot(self, raw: dict) -> Optional[TokenSnapshot]:
        ca           = raw.get("contract_address", "")
        total_supply = raw.get("total_supply", 0)

        async with aiohttp.ClientSession() as session:
            verifier = OnChainVerifier(session)
            rpc      = SolanaRPCClient(session)
            tracker  = WalletTracker(session, self.smart_wallets)
            twitter  = TwitterSentimentPipeline(session)

            # Run all external calls in parallel
            results = await asyncio.gather(
                verifier.check(ca),
                twitter.fetch(ca),
                tracker.get_recent_buys(),
                rpc.top10_concentration(ca, total_supply) if total_supply > 0 else asyncio.sleep(0),
                return_exceptions=True,
            )

        onchain, tw_result, smart_buys, rpc_top10 = results

        if isinstance(onchain,    Exception): onchain    = {}
        if isinstance(tw_result,  Exception): tw_result  = (0, 0.0)
        if isinstance(smart_buys, Exception): smart_buys = {}
        if isinstance(rpc_top10,  Exception): rpc_top10  = 0.0

        mentions, sentiment = tw_result

        # Prefer Solana RPC concentration if we got it; fall back to Axiom field
        top10 = (rpc_top10 if isinstance(rpc_top10, float) and rpc_top10 > 0
                 else onchain.get("creator_percent", raw.get("top10_wallet_pct", 0)))

        return TokenSnapshot(
            ca=ca,
            symbol=raw.get("symbol", "UNKNOWN"),
            price_usd=raw.get("price_usd", 0),
            volume_5m=raw.get("volume_5m", 0),
            volume_15m=raw.get("volume_15m", 0),
            volume_1h=raw.get("volume_1h", 0),
            liquidity_usd=raw.get("liquidity_usd", 0),
            market_cap=raw.get("market_cap", 0),
            buy_txns_15m=raw.get("buy_txns_15m", 0),
            sell_txns_15m=raw.get("sell_txns_15m", 0),
            top10_wallet_pct=top10,
            holder_count=onchain.get("holder_count", raw.get("holder_count", 0)),
            creator_percent=onchain.get("creator_percent", 0),
            can_take_back_ownership=onchain.get("can_take_back_ownership", False),
            lp_locked=onchain.get("lp_locked", raw.get("lp_locked", False)),
            lp_lock_provider=raw.get("lp_lock_provider", "None"),
            lp_lock_expiry_days=raw.get("lp_lock_expiry_days", 0),
            is_honeypot=onchain.get("is_honeypot", raw.get("is_honeypot", True)),
            has_mint_function=onchain.get("has_mint_function", True),
            has_blacklist=onchain.get("has_blacklist", False),
            twitter_mentions_10m=mentions,
            twitter_sentiment_score=sentiment,
            smart_wallet_buys_1h=smart_buys.get(ca, 0.0) if isinstance(smart_buys, dict) else 0.0,
        )
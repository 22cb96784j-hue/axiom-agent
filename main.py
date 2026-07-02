# main.py — FastAPI backend proxy
# Keeps ANTHROPIC_API_KEY server-side; the browser never sees it.
# Run: uvicorn main:app --host 0.0.0.0 --port 8000 --reload

import os, json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import anthropic
from dotenv import load_dotenv

load_dotenv()

# ── App ───────────────────────────────────────────────────────
app = FastAPI(title="Axiom AI Agent API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

WEIGHTS_FILE = os.getenv("WEIGHTS_FILE", "./weights_and_bias.json")
DEFAULT_WEIGHTS = {
    "momentum_score_threshold": 65,
    "min_smart_wallet_usd": 1000,
    "min_twitter_sentiment": 0.2,
    "max_vl_ratio_entry": 5.0,
    "ofi_threshold": 0.25,
    "min_confidence": 0.6,
    "min_holder_count": 50,
    "blacklisted_influencers": [],
    "trusted_lp_lockers": ["Streamflow", "PinkLock"],
    "version": 1,
    "last_updated": "",
}


# ── Request model ─────────────────────────────────────────────
class TokenInput(BaseModel):
    symbol: str
    ca: str
    price_usd: float
    volume_5m: float
    volume_15m: float
    liquidity_usd: float
    market_cap: float
    buy_txns_15m: int
    sell_txns_15m: int
    top10_wallet_pct: float
    holder_count: int = 0
    lp_locked: bool
    lp_lock_provider: str = "None"
    is_honeypot: bool = False
    has_mint_function: bool = False
    has_blacklist: bool = False
    twitter_mentions_10m: int = 0
    twitter_sentiment_score: float = 0.0
    smart_wallet_buys_1h: float = 0.0
    creator_percent: float = 0.0
    can_take_back_ownership: bool = False


# ── Health ────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}


# ── Alpha Report (main endpoint) ──────────────────────────────
@app.post("/api/analyze")
async def analyze(token: TokenInput):
    """
    Receives token data from the frontend, runs Claude analysis server-side,
    returns the alpha report markdown. API key never leaves this server.
    """
    vl5m   = round(token.volume_5m / max(token.liquidity_usd, 1), 2)
    ofi    = round(
        (token.buy_txns_15m - token.sell_txns_15m)
        / max(token.buy_txns_15m + token.sell_txns_15m, 1),
        3
    )
    liq2mc = round(token.liquidity_usd / max(token.market_cap, 1) * 100, 1)

    # Load learned weights to contextualise the prompt
    try:
        with open(WEIGHTS_FILE) as f:
            weights = json.load(f)
    except FileNotFoundError:
        weights = DEFAULT_WEIGHTS

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            system=(
                "You are the Axiom AI Trading Agent v2. Your job is to generate "
                "concise, structured intraday alpha reports for Solana memecoins. "
                "Use professional trading language. Output clean markdown. Be "
                "specific, factual, and data-driven. Never add hype or emotional "
                "language. Always include a clear RISK WARNING at the end."
            ),
            messages=[{
                "role": "user",
                "content": (
                    f"Analyze this Solana token and generate an Alpha Report:\n\n"
                    f"**Token:** ${token.symbol} | CA: `{token.ca}`\n"
                    f"**Price:** ${token.price_usd}\n"
                    f"**Volume:** 5m=${token.volume_5m:,.0f} | 15m=${token.volume_15m:,.0f}\n"
                    f"**Liquidity:** ${token.liquidity_usd:,.0f} | MCap: ${token.market_cap:,.0f}\n"
                    f"**Order Flow 15m:** {token.buy_txns_15m} buys / {token.sell_txns_15m} sells\n"
                    f"**Holders:** {token.holder_count} | Top-10: {token.top10_wallet_pct}%\n"
                    f"**LP Locked:** {token.lp_locked} via {token.lp_lock_provider}\n"
                    f"**Security:** honeypot={token.is_honeypot} | mint={token.has_mint_function} "
                    f"| blacklist={token.has_blacklist} | ownership_reclaim={token.can_take_back_ownership}\n"
                    f"**Creator %:** {token.creator_percent:.1f}%\n"
                    f"**Twitter:** {token.twitter_mentions_10m} mentions (10m) | "
                    f"sentiment {token.twitter_sentiment_score:+.2f}\n"
                    f"**Smart wallet buys:** ${token.smart_wallet_buys_1h:,.0f} in 1h\n\n"
                    f"**Computed metrics:** V/L={vl5m}x | OFI={ofi} | Liq/MCap={liq2mc}%\n\n"
                    f"**Learned thresholds (current weights):**\n"
                    f"- Momentum threshold: {weights.get('momentum_score_threshold', 65)}\n"
                    f"- Min OFI: {weights.get('ofi_threshold', 0.25)}\n"
                    f"- Min sentiment: {weights.get('min_twitter_sentiment', 0.2)}\n\n"
                    f"**Portfolio:** $10,000\n\n"
                    f"Generate:\n"
                    f"1. Catalyst narrative (2 sentences, factual)\n"
                    f"2. Entry range, Stop Loss, TP1 (scalp), TP2 (swing), TP3 (moon bag)\n"
                    f"3. Risk level: LOW / MEDIUM / HIGH + risk score /100\n"
                    f"4. Recommended position size + max slippage\n"
                    f"5. Setup type: BREAKOUT / CONSOLIDATION / BLOWOFF_TOP / WEAK\n"
                    f"6. One key lesson from a similar historical setup\n"
                    f"7. RISK WARNING"
                )
            }]
        )
        return {
            "report": response.content[0].text,
            "usage": response.usage.model_dump(),
        }
    except anthropic.AuthenticationError:
        raise HTTPException(status_code=401, detail="Invalid ANTHROPIC_API_KEY — check your .env file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Current learned weights ───────────────────────────────────
@app.get("/api/weights")
def get_weights():
    try:
        with open(WEIGHTS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return DEFAULT_WEIGHTS


# ── Memory / performance stats ────────────────────────────────
@app.get("/api/stats")
def get_stats():
    try:
        from vector_memory import TradingMemory
        return TradingMemory().win_rate_stats()
    except Exception as e:
        return {"error": str(e), "note": "Run the agent first to populate ChromaDB"}

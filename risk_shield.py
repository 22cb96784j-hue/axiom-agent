# risk_shield.py — Anti-Rug Filter Matrix
# All thresholds are configurable via environment variables.
# A token must PASS ALL checks before any TA is even run.

import os
from dataclasses import dataclass, field
from data_ingestion import TokenSnapshot

# ── Configurable thresholds (override in .env) ────────────────
MIN_LIQUIDITY      = float(os.getenv("MIN_LIQUIDITY_USD",    "20000"))
MAX_TOP10_PCT      = float(os.getenv("MAX_TOP10_WALLET_PCT", "15.0"))
MIN_LP_LOCK_DAYS   = int(os.getenv("MIN_LP_LOCK_DAYS",       "30"))
PORTFOLIO_USD      = float(os.getenv("PORTFOLIO_VALUE_USD",  "10000"))
MAX_POSITION_PCT   = float(os.getenv("MAX_POSITION_PCT",     "0.01"))   # 1%
MAX_SLIPPAGE_PCT   = float(os.getenv("MAX_SLIPPAGE_PCT",     "0.03"))   # 3%
MIN_HOLDERS        = int(os.getenv("MIN_HOLDER_COUNT",       "50"))
MAX_CREATOR_PCT    = float(os.getenv("MAX_CREATOR_PCT",      "10.0"))
TRUSTED_LP_LOCKS   = os.getenv("TRUSTED_LP_LOCKERS", "Streamflow,PinkLock").split(",")


@dataclass
class RiskAssessment:
    passed: bool
    risk_level: str                  # LOW | MEDIUM | HIGH | BLACKLIST
    fail_reasons: list[str] = field(default_factory=list)
    warnings: list[str]    = field(default_factory=list)
    position_size_usd: float = 0.0
    recommended_slippage_pct: float = 0.0
    liquidity_to_mcap: float = 0.0
    risk_score: int = 0              # 0 (safe) → 100 (extreme danger)


class AntiRugShield:
    """
    8-point filter matrix.  All checks run even on failure so the full
    risk picture is surfaced in fail_reasons for logging and learning.
    """

    # 1 ── Smart contract security (GoPlus verified) ──────────
    @staticmethod
    def check_contract(snap: TokenSnapshot) -> list[str]:
        fails = []
        if snap.is_honeypot:
            fails.append("🚫 HONEYPOT — sells are permanently blocked (GoPlus verified)")
        if snap.has_mint_function:
            fails.append("🚫 MINT FUNCTION — deployer can inflate supply at any time")
        if snap.has_blacklist:
            fails.append("🚫 BLACKLIST — deployer can prevent your wallet from selling")
        if snap.can_take_back_ownership:
            fails.append("🚫 OWNERSHIP RECLAIM — deployer can retake contract control")
        if snap.creator_percent > MAX_CREATOR_PCT:
            fails.append(
                f"🚫 CREATOR holds {snap.creator_percent:.1f}% (max {MAX_CREATOR_PCT}%)"
            )
        return fails

    # 2 ── Liquidity health ────────────────────────────────────
    @staticmethod
    def check_liquidity(snap: TokenSnapshot) -> tuple[list[str], list[str]]:
        fails, warns = [], []
        if snap.liquidity_usd < MIN_LIQUIDITY:
            fails.append(
                f"🚫 LIQUIDITY ${snap.liquidity_usd:,.0f} < minimum ${MIN_LIQUIDITY:,.0f}"
            )
        if not snap.lp_locked:
            fails.append("🚫 LP NOT LOCKED — rug pull possible at any moment")
        elif snap.lp_lock_expiry_days < MIN_LP_LOCK_DAYS:
            warns.append(
                f"⚠️ LP lock expires in {snap.lp_lock_expiry_days}d "
                f"(minimum {MIN_LP_LOCK_DAYS}d)"
            )
        if snap.lp_locked and snap.lp_lock_provider not in TRUSTED_LP_LOCKS:
            warns.append(
                f"⚠️ LP locker '{snap.lp_lock_provider}' is not in trusted list "
                f"({', '.join(TRUSTED_LP_LOCKS)})"
            )
        return fails, warns

    # 3 ── Supply distribution ─────────────────────────────────
    @staticmethod
    def check_supply(snap: TokenSnapshot) -> tuple[list[str], list[str]]:
        fails, warns = [], []
        if snap.top10_wallet_pct > MAX_TOP10_PCT:
            fails.append(
                f"🚫 WHALE CONCENTRATION: top-10 hold "
                f"{snap.top10_wallet_pct:.1f}% (max {MAX_TOP10_PCT}%)"
            )
        if snap.holder_count < MIN_HOLDERS:
            warns.append(
                f"⚠️ Only {snap.holder_count} holders "
                f"— very early / potentially low distribution"
            )
        return fails, warns

    # 4 ── Dynamic position sizing ─────────────────────────────
    @staticmethod
    def position_size(snap: TokenSnapshot) -> tuple[float, float]:
        """
        Position = min(portfolio_pct, max_slippage_allowed_by_pool)
        Slippage formula: slippage ≈ position_usd / (2 × liquidity_usd)
        We solve for the position_usd that keeps slippage ≤ MAX_SLIPPAGE_PCT.
        """
        base_position  = PORTFOLIO_USD * MAX_POSITION_PCT
        max_by_slippage = MAX_SLIPPAGE_PCT * 2 * snap.liquidity_usd
        position        = min(base_position, max_by_slippage)
        slippage        = position / (2 * max(snap.liquidity_usd, 1))
        return round(position, 2), round(min(slippage, 0.20) * 100, 2)

    # 5 ── Composite risk score ────────────────────────────────
    @staticmethod
    def compute_risk_score(snap: TokenSnapshot, fails: list[str]) -> int:
        score = len(fails) * 25          # each hard fail = 25 pts
        if snap.top10_wallet_pct > 10:   score += 10
        if not snap.lp_locked:           score += 15
        if snap.liquidity_usd < 50_000:  score += 8
        if snap.holder_count < 100:      score += 8
        liq_to_mc = snap.liquidity_usd / max(snap.market_cap, 1)
        if liq_to_mc < 0.05:             score += 12   # thin liquidity vs MCap
        if snap.creator_percent > 5:     score += 7
        return min(score, 100)

    # ── Master evaluate ───────────────────────────────────────
    def evaluate(self, snap: TokenSnapshot) -> RiskAssessment:
        all_fails, all_warns = [], []

        all_fails.extend(self.check_contract(snap))
        lf, lw = self.check_liquidity(snap)
        all_fails.extend(lf)
        all_warns.extend(lw)
        sf, sw = self.check_supply(snap)
        all_fails.extend(sf)
        all_warns.extend(sw)

        passed  = len(all_fails) == 0
        score   = self.compute_risk_score(snap, all_fails)
        pos, sl = self.position_size(snap)
        l2m     = round(snap.liquidity_usd / max(snap.market_cap, 1), 4)
        level   = (
            "BLACKLIST" if not passed
            else "LOW"    if score < 20
            else "MEDIUM" if score < 45
            else "HIGH"
        )

        return RiskAssessment(
            passed=passed,
            risk_level=level,
            fail_reasons=all_fails,
            warnings=all_warns,
            position_size_usd=pos if passed else 0.0,
            recommended_slippage_pct=sl if passed else 0.0,
            liquidity_to_mcap=l2m,
            risk_score=score,
        )

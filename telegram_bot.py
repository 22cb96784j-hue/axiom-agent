# telegram_bot.py — Real-time alerts via Telegram (direct HTTP, no library needed)
# Setup: create a bot via @BotFather, get your chat ID from @userinfobot
# Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

import os
import aiohttp
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")
_MAX_LEN  = 4096


class TelegramAlerter:
    """
    Sends Telegram messages via direct HTTPS calls to the Bot API.
    No third-party library needed — just aiohttp.
    Silently no-ops if credentials are not configured.
    """

    BASE = "https://api.telegram.org/bot"

    def __init__(self):
        if BOT_TOKEN and CHAT_ID:
            self.token   = BOT_TOKEN
            self.chat_id = CHAT_ID
            self._active = True
            print("[Telegram] Bot initialized ✓")
        else:
            self.token   = None
            self.chat_id = None
            self._active = False
            print("[Telegram] No credentials — alerts disabled.")

    # ── Send text message ─────────────────────────────────────
    async def send(self, text: str) -> bool:
        if not self._active:
            print(f"[Telegram MOCK] {text[:120]}")
            return True
        url = f"{self.BASE}{self.token}/sendMessage"
        chunks = [text[i: i + _MAX_LEN] for i in range(0, len(text), _MAX_LEN)]
        try:
            async with aiohttp.ClientSession() as session:
                for chunk in chunks:
                    payload = {
                        "chat_id":                  self.chat_id,
                        "text":                     chunk,
                        "parse_mode":               "Markdown",
                        "disable_web_page_preview": True,
                    }
                    async with session.post(url, json=payload,
                                            timeout=aiohttp.ClientTimeout(total=15)) as r:
                        resp = await r.json()
                        if not resp.get("ok"):
                            print(f"[Telegram] API error: {resp.get('description')}")
                            return False
            return True
        except Exception as exc:
            print(f"[Telegram] send error: {exc}")
            return False

    # ── Send file ─────────────────────────────────────────────
    async def send_file(self, file_path: str, caption: str = "") -> bool:
        if not self._active:
            print(f"[Telegram MOCK] send_file: {file_path}")
            return True
        path = Path(file_path)
        if not path.exists():
            print(f"[Telegram] file not found: {file_path}")
            return False
        url = f"{self.BASE}{self.token}/sendDocument"
        try:
            async with aiohttp.ClientSession() as session:
                with open(path, "rb") as f:
                    data = aiohttp.FormData()
                    data.add_field("chat_id", self.chat_id)
                    data.add_field("caption", caption)
                    data.add_field("document", f, filename=path.name)
                    async with session.post(url, data=data,
                                            timeout=aiohttp.ClientTimeout(total=30)) as r:
                        resp = await r.json()
                        if not resp.get("ok"):
                            print(f"[Telegram] send_file error: {resp.get('description')}")
                            return False
            return True
        except Exception as exc:
            print(f"[Telegram] send_file error: {exc}")
            return False

    # ── Convenience wrappers ──────────────────────────────────
    async def alert(self, symbol: str, message: str) -> bool:
        return await self.send(f"🚨 *URGENT ALPHA ALERT* — `${symbol}`\n\n{message}")

    async def daily_summary(self, total: int, win_rate: float, setups: int) -> bool:
        return await self.send(
            f"📊 *Daily Summary*\n"
            f"Setups qualified: *{setups}*\n"
            f"Trades in DB: *{total}*\n"
            f"All-time win rate: *{win_rate:.1f}%*"
        )

    async def weights_updated(self, version: int, win_rate: float) -> bool:
        return await self.send(
            f"🧠 *Self-Learning Update*\n"
            f"Weights updated to v{version}\n"
            f"Current win rate: *{win_rate:.1f}%*"
        )


# ── Quick test ────────────────────────────────────────────────
if __name__ == "__main__":
    import asyncio

    async def _test():
        bot = TelegramAlerter()
        ok  = await bot.send("✅ Axiom AI Agent v2 — Telegram connected!")
        print("Sent:", ok)

    asyncio.run(_test())

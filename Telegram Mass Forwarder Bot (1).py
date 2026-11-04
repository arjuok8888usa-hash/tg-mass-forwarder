#!/usr/bin/env python3
"""
Telegram Mass Message Forwarder & Broadcaster Bot (Mobile & Cloud Ready)
========================================================================
Supports two operating modes using Telethon:
  1. BOT MODE: Uses a standard BOT_TOKEN from @BotFather (safe for broadcasting
     to subscribers and forwarding between chats where the bot is an admin).
  2. USERBOT MODE: Uses your Telegram account (API_ID + API_HASH + STRING_SESSION
     for cloud hosting, or interactive phone login on Termux) to forward from
     ANY joined channel/group to multiple target chats.

Mobile Deployment Ready:
  - Android (Termux): Runs natively with `pkg install python && pip install telethon`
  - Mobile Cloud (Render / Railway / Koyeb): Includes a built-in async HTTP
    health-check server on $PORT so free/cloud web services stay alive 24/7.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.sessions import StringSession
from telethon.tl.types import Message

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MassForwarder")

# ---------------------------------------------------------------------------
# Configuration & State Management
# ---------------------------------------------------------------------------
CONFIG_FILE = Path(os.getenv("CONFIG_FILE", "forwarder_config.json"))

DEFAULT_CONFIG = {
    "enabled": True,
    "forward_mode": "copy",  # "copy" (no 'Forwarded from' tag) or "forward" (keeps tag)
    "delay_seconds": 1.5,    # Delay between each target chat to prevent FloodWait bans
    "sources": [],           # List of source chat IDs or @usernames (int or str)
    "targets": [],           # List of destination chat IDs or @usernames (int or str)
    "blacklist_words": [],   # Skip messages containing any of these words
    "replacements": {},      # Dict of {"old_text": "new_text"} applied in "copy" mode
    "stats": {
        "forwarded_count": 0,
        "failed_count": 0,
    },
}


class ConfigManager:
    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                merged = {**DEFAULT_CONFIG, **loaded}
                return merged
            except Exception as exc:
                logger.error(f"Failed to load config ({exc}), using defaults.")
        return json.loads(json.dumps(DEFAULT_CONFIG))

    def save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.error(f"Failed to save config: {exc}")

    @staticmethod
    def parse_chat_id(raw: str):
        raw = raw.strip()
        if re.match(r"^-?\d+$", raw):
            return int(raw)
        return raw


config = ConfigManager(CONFIG_FILE)

# ---------------------------------------------------------------------------
# Environment Credentials (with interactive mobile terminal fallback)
# ---------------------------------------------------------------------------
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()  # Comma-separated Telegram user IDs
SESSION_NAME = os.getenv("SESSION_NAME", "forwarder_session")
PORT = int(os.getenv("PORT", "8080"))


def get_credentials():
    global API_ID_RAW, API_HASH, BOT_TOKEN, STRING_SESSION, ADMIN_IDS_RAW
    if not API_ID_RAW or not API_HASH:
        print("\n=== Telegram Mass Forwarder Setup ===")
        print("Get API_ID and API_HASH from https://my.telegram.org\n")
        if not API_ID_RAW:
            API_ID_RAW = input("Enter API_ID: ").strip()
        if not API_HASH:
            API_HASH = input("Enter API_HASH: ").strip()
        if not BOT_TOKEN and not STRING_SESSION:
            mode_choice = input(
                "Enter BOT_TOKEN from @BotFather (or press Enter for Userbot phone login): "
            ).strip()
            BOT_TOKEN = mode_choice
        if not ADMIN_IDS_RAW:
            ADMIN_IDS_RAW = input(
                "Enter your Telegram User ID for admin commands (send /id to @userinfobot): "
            ).strip()

    api_id = int(API_ID_RAW)
    admin_ids = [
        int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().lstrip("-").isdigit()
    ]
    return api_id, API_HASH, BOT_TOKEN, STRING_SESSION, admin_ids


# ---------------------------------------------------------------------------
# Helper Logic: Filtering, Text Transformation & Safe Sending
# ---------------------------------------------------------------------------
def should_skip_message(text: Optional[str]) -> bool:
    if not text:
        return False
    lower_text = text.lower()
    for word in config.data["blacklist_words"]:
        if word.lower() in lower_text:
            return True
    return False


def apply_replacements(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    result = text
    for old_val, new_val in config.data["replacements"].items():
        result = result.replace(old_val, new_val)
    return result


async def dispatch_to_target(
    client: TelegramClient,
    message: Message,
    target_chat,
    mode: str,
) -> bool:
    """Sends or forwards a single message to one target chat with FloodWait handling."""
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            if mode == "forward":
                await client.forward_messages(
                    entity=target_chat,
                    messages=message.id,
                    from_peer=message.chat_id,
                )
            else:
                # "copy" mode strips the "Forwarded from" header and applies text replacements
                new_text = apply_replacements(message.message or "")
                await client.send_message(
                    entity=target_chat,
                    message=new_text,
                    file=message.media,
                    formatting_entities=message.entities if not config.data["replacements"] else None,
                    link_preview=True,
                )
            return True
        except FloodWaitError as fw:
            wait_time = fw.seconds + 2
            logger.warning(
                f"FloodWait triggered for target {target_chat}: sleeping {wait_time}s (attempt {attempt}/{max_retries})"
            )
            await asyncio.sleep(wait_time)
        except RPCError as rpc_err:
            logger.error(f"Telegram RPCError on target {target_chat}: {rpc_err}")
            return False
        except Exception as exc:
            logger.error(f"Unexpected error sending to {target_chat}: {exc}")
            return False
    return False


async def mass_forward_message(client: TelegramClient, message: Message) -> Dict[str, int]:
    """Forwards a message to all configured target chats with rate-limiting delay."""
    targets: List = config.data["targets"]
    mode: str = config.data["forward_mode"]
    delay: float = float(config.data["delay_seconds"])

    results = {"sent": 0, "failed": 0}
    for idx, target in enumerate(targets):
        ok = await dispatch_to_target(client, message, target, mode)
        if ok:
            results["sent"] += 1
            config.data["stats"]["forwarded_count"] += 1
        else:
            results["failed"] += 1
            config.data["stats"]["failed_count"] += 1

        # Rate-limit delay between targets (skip delay after last target)
        if idx < len(targets) - 1 and delay > 0:
            await asyncio.sleep(delay)

    config.save()
    return results


# ---------------------------------------------------------------------------
# Lightweight Async Health Server (For Mobile Cloud Hosting: Render/Koyeb)
# ---------------------------------------------------------------------------
async def _handle_health_check(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.read(1024)
        stats = config.data["stats"]
        body = json.dumps(
            {
                "status": "online",
                "enabled": config.data["enabled"],
                "sources_count": len(config.data["sources"]),
                "targets_count": len(config.data["targets"]),
                "stats": stats,
            }
        )
        response = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
            f"{body}"
        )
        writer.write(response.encode("utf-8"))
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def start_health_server(port: int):
    try:
        server = await asyncio.start_server(_handle_health_check, "0.0.0.0", port)
        logger.info(f"Health-check server listening on port {port}")
        async with server:
            await server.serve_forever()
    except Exception as exc:
        logger.warning(f"Could not bind health-check port {port}: {exc}")


# ---------------------------------------------------------------------------
# Bot & Userbot Event Handlers
# ---------------------------------------------------------------------------
def register_handlers(client: TelegramClient, admin_ids: List[int]):
    def is_admin(event) -> bool:
        if not admin_ids:
            # If no ADMIN_IDS configured, allow outgoing messages from self (Userbot)
            return bool(event.out)
        return event.sender_id in admin_ids or bool(event.out)

    @client.on(events.NewMessage(pattern=r"^/start$|^/help$"))
    async def cmd_help(event):
        if not is_admin(event):
            return
        help_text = (
            "🚀 **Telegram Mass Forwarder Control Panel**\n\n"
            "**Source & Target Management:**\n"
            "• `/add_source <chat_id or @username>` — Add a source chat\n"
            "• `/rm_source <chat_id or @username>` — Remove a source chat\n"
            "• `/add_target <chat_id or @username>` — Add a destination chat\n"
            "• `/rm_target <chat_id or @username>` — Remove a destination chat\n"
            "• `/clear_targets` — Remove all destination chats\n\n"
            "**Forwarding Behavior & Filters:**\n"
            "• `/toggle` — Pause or resume auto-forwarding\n"
            "• `/mode <copy|forward>` — `copy` hides 'Forwarded from', `forward` keeps it\n"
            "• `/delay <seconds>` — Set delay between sends (default: 1.5s)\n"
            "• `/replace <old> -> <new>` — Replace text/links in `copy` mode\n"
            "• `/blacklist <word>` — Toggle a blocked keyword\n\n"
            "**Mass Broadcast & Bulk History Actions:**\n"
            "• `/broadcast` — Reply to ANY message with `/broadcast` to blast it to all targets\n"
            "• `/bulk <source> <start_msg_id> <end_msg_id>` — Mass-forward a range of past messages\n"
            "• `/id` — Get the Chat ID of the current chat\n"
            "• `/status` — View active configuration and delivery stats"
        )
        await event.reply(help_text)

    @client.on(events.NewMessage(pattern=r"^/id$"))
    async def cmd_id(event):
        if not is_admin(event):
            return
        await event.reply(
            f"**Chat ID:** `{event.chat_id}`\n**Your User ID:** `{event.sender_id}`"
        )

    @client.on(events.NewMessage(pattern=r"^/status$"))
    async def cmd_status(event):
        if not is_admin(event):
            return
        d = config.data
        sources_str = ", ".join(f"`{s}`" for s in d["sources"]) or "None"
        targets_str = ", ".join(f"`{t}`" for t in d["targets"]) or "None"
        bl_str = ", ".join(f"`{w}`" for w in d["blacklist_words"]) or "None"
        status_msg = (
            "📊 **Mass Forwarder Status**\n\n"
            f"• **State:** {'🟢 Running' if d['enabled'] else '🔴 Paused'}\n"
            f"• **Mode:** `{d['forward_mode']}`\n"
            f"• **Rate-Limit Delay:** `{d['delay_seconds']}s` per target\n"
            f"• **Sources ({len(d['sources'])}):** {sources_str}\n"
            f"• **Targets ({len(d['targets'])}):** {targets_str}\n"
            f"• **Blacklisted Words:** {bl_str}\n"
            f"• **Replacements:** `{len(d['replacements'])}` active rules\n\n"
            f"📈 **Total Delivered:** `{d['stats']['forwarded_count']}`\n"
            f"⚠️ **Total Failed:** `{d['stats']['failed_count']}`"
        )
        await event.reply(status_msg)

    @client.on(events.NewMessage(pattern=r"^/toggle$"))
    async def cmd_toggle(event):
        if not is_admin(event):
            return
        config.data["enabled"] = not config.data["enabled"]
        config.save()
        state = "🟢 Enabled" if config.data["enabled"] else "🔴 Paused"
        await event.reply(f"Auto-forwarding is now **{state}**.")

    @client.on(events.NewMessage(pattern=r"^/mode(?:\s+(copy|forward))?$"))
    async def cmd_mode(event):
        if not is_admin(event):
            return
        arg = event.pattern_match.group(1)
        if not arg:
            await event.reply("Usage: `/mode copy` (anonymous) or `/mode forward` (with header)")
            return
        config.data["forward_mode"] = arg
        config.save()
        await event.reply(f"✅ Forwarding mode set to `{arg}`.")

    @client.on(events.NewMessage(pattern=r"^/delay\s+([0-9.]+)$"))
    async def cmd_delay(event):
        if not is_admin(event):
            return
        val = float(event.pattern_match.group(1))
        config.data["delay_seconds"] = max(0.1, val)
        config.save()
        await event.reply(f"✅ Delay between targets set to `{config.data['delay_seconds']}s`.")

    @client.on(events.NewMessage(pattern=r"^/add_source\s+(.+)$"))
    async def cmd_add_source(event):
        if not is_admin(event):
            return
        chat_ref = ConfigManager.parse_chat_id(event.pattern_match.group(1))
        if chat_ref not in config.data["sources"]:
            config.data["sources"].append(chat_ref)
            config.save()
            await event.reply(f"✅ Added `{chat_ref}` to sources.")
        else:
            await event.reply(f"ℹ️ `{chat_ref}` is already in sources.")

    @client.on(events.NewMessage(pattern=r"^/rm_source\s+(.+)$"))
    async def cmd_rm_source(event):
        if not is_admin(event):
            return
        chat_ref = ConfigManager.parse_chat_id(event.pattern_match.group(1))
        if chat_ref in config.data["sources"]:
            config.data["sources"].remove(chat_ref)
            config.save()
            await event.reply(f"🗑️ Removed `{chat_ref}` from sources.")
        else:
            await event.reply(f"⚠️ `{chat_ref}` not found in sources.")

    @client.on(events.NewMessage(pattern=r"^/add_target\s+(.+)$"))
    async def cmd_add_target(event):
        if not is_admin(event):
            return
        # Support adding multiple space/comma-separated targets at once
        raw_items = re.split(r"[\s,]+", event.pattern_match.group(1).strip())
        added = []
        for item in raw_items:
            if not item:
                continue
            chat_ref = ConfigManager.parse_chat_id(item)
            if chat_ref not in config.data["targets"]:
                config.data["targets"].append(chat_ref)
                added.append(str(chat_ref))
        config.save()
        await event.reply(f"✅ Added targets: `{', '.join(added) or 'None (already present)'}`")

    @client.on(events.NewMessage(pattern=r"^/rm_target\s+(.+)$"))
    async def cmd_rm_target(event):
        if not is_admin(event):
            return
        chat_ref = ConfigManager.parse_chat_id(event.pattern_match.group(1))
        if chat_ref in config.data["targets"]:
            config.data["targets"].remove(chat_ref)
            config.save()
            await event.reply(f"🗑️ Removed `{chat_ref}` from targets.")
        else:
            await event.reply(f"⚠️ `{chat_ref}` not found in targets.")

    @client.on(events.NewMessage(pattern=r"^/clear_targets$"))
    async def cmd_clear_targets(event):
        if not is_admin(event):
            return
        config.data["targets"] = []
        config.save()
        await event.reply("🗑️ Cleared all target chats.")

    @client.on(events.NewMessage(pattern=r"^/replace\s+(.+?)\s*->\s*(.*)$"))
    async def cmd_replace(event):
        if not is_admin(event):
            return
        old_txt = event.pattern_match.group(1).strip()
        new_txt = event.pattern_match.group(2).strip()
        config.data["replacements"][old_txt] = new_txt
        config.save()
        await event.reply(f"✅ Added text replacement: `{old_txt}` → `{new_txt}`")

    @client.on(events.NewMessage(pattern=r"^/blacklist\s+(.+)$"))
    async def cmd_blacklist(event):
        if not is_admin(event):
            return
        word = event.pattern_match.group(1).strip()
        if word in config.data["blacklist_words"]:
            config.data["blacklist_words"].remove(word)
            action = "Removed from"
        else:
            config.data["blacklist_words"].append(word)
            action = "Added to"
        config.save()
        await event.reply(f"✅ {action} blacklist: `{word}`")

    @client.on(events.NewMessage(pattern=r"^/broadcast$"))
    async def cmd_broadcast(event):
        """Reply to any message with /broadcast to blast it to all target chats."""
        if not is_admin(event):
            return
        if not event.is_reply:
            await event.reply("⚠️ Reply to the message you want to mass-forward with `/broadcast`.")
            return
        if not config.data["targets"]:
            await event.reply("⚠️ No target chats configured. Use `/add_target <chat_id>` first.")
            return

        reply_msg = await event.get_reply_message()
        status_note = await event.reply(
            f"⏳ Broadcasting message to `{len(config.data['targets'])}` targets..."
        )
        res = await mass_forward_message(client, reply_msg)
        await status_note.edit(
            f"✅ **Broadcast Complete**\n• Sent: `{res['sent']}`\n• Failed: `{res['failed']}`"
        )

    @client.on(events.NewMessage(pattern=r"^/bulk\s+(\S+)\s+(\d+)\s+(\d+)$"))
    async def cmd_bulk(event):
        """Mass-forward a historical range of messages from a source chat to all targets."""
        if not is_admin(event):
            return
        if not config.data["targets"]:
            await event.reply("⚠️ No target chats configured. Use `/add_target <chat_id>` first.")
            return

        source_ref = ConfigManager.parse_chat_id(event.pattern_match.group(1))
        start_id = int(event.pattern_match.group(2))
        end_id = int(event.pattern_match.group(3))
        if start_id > end_id or (end_id - start_id) > 500:
            await event.reply("⚠️ Please provide a valid range (max 500 messages per batch).")
            return

        status_note = await event.reply(
            f"⏳ Bulk forwarding messages `{start_id}` to `{end_id}` from `{source_ref}`..."
        )
        total_sent = 0
        total_failed = 0

        for msg_id in range(start_id, end_id + 1):
            try:
                msg = await client.get_messages(source_ref, ids=msg_id)
                if not msg or should_skip_message(msg.message):
                    continue
                res = await mass_forward_message(client, msg)
                total_sent += res["sent"]
                total_failed += res["failed"]
                await asyncio.sleep(float(config.data["delay_seconds"]))
            except Exception as exc:
                logger.warning(f"Skipping message {msg_id}: {exc}")

        await status_note.edit(
            f"✅ **Bulk Forward Complete**\n"
            f"• Total Deliveries: `{total_sent}`\n"
            f"• Failed Deliveries: `{total_failed}`"
        )

    @client.on(events.NewMessage())
    async def auto_forward_listener(event):
        """Real-time listener that forwards new messages from configured source chats."""
        if not config.data["enabled"] or not config.data["sources"] or not config.data["targets"]:
            return

        # Ignore admin commands
        raw_text = event.raw_text or ""
        if raw_text.startswith("/"):
            return

        # Check if message came from a configured source (by ID or username)
        chat_id = event.chat_id
        chat = await event.get_chat()
        username = f"@{chat.username}" if getattr(chat, "username", None) else None

        sources = config.data["sources"]
        is_source = (chat_id in sources) or (username and username in sources)
        if not is_source:
            return

        if should_skip_message(raw_text):
            logger.info(f"Skipped message {event.id} from {chat_id} due to blacklist filter.")
            return

        logger.info(
            f"Auto-forwarding message {event.id} from {chat_id} to {len(config.data['targets'])} targets..."
        )
        await mass_forward_message(client, event.message)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
async def main():
    api_id, api_hash, bot_token, string_session, admin_ids = get_credentials()

    session = StringSession(string_session) if string_session else SESSION_NAME
    client = TelegramClient(session, api_id, api_hash)
    register_handlers(client, admin_ids)

    # Start health server in background for cloud platforms (Render, Koyeb, Railway)
    asyncio.create_task(start_health_server(PORT))

    if bot_token:
        logger.info("Starting in BOT MODE (via BOT_TOKEN)...")
        await client.start(bot_token=bot_token)
    else:
        logger.info("Starting in USERBOT MODE...")
        await client.start()

    me = await client.get_me()
    logger.info(f"Connected as: {me.first_name} (@{getattr(me, 'username', 'N/A')}) | ID: {me.id}")
    logger.info("Send /help to the bot (or in Saved Messages for Userbot) from Telegram Mobile.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped cleanly.")
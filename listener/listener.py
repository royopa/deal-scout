import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict

import aiohttp
import yaml
from dotenv import load_dotenv
from telethon import TelegramClient, events


URL_PATTERN = re.compile(
    r"(https?://\S+|www\.\S+|\b\S+\.(?:com|net|org|io|co|deals)\b)",
    re.IGNORECASE,
)


def message_contains_url(message: str | None) -> bool:
    if not message:
        return False
    return bool(URL_PATTERN.search(message))


def _normalize_retry(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        raw_config = path.read_text(encoding="utf-8")
        if path.suffix.lower() in {".yaml", ".yml"}:
            config = yaml.safe_load(raw_config) or {}
        else:
            config = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON config: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML config: {config_path}") from exc

    if not isinstance(config, dict):
        raise ValueError("Config must be an object")

    channels = config.get("channels") or []
    if not isinstance(channels, list):
        raise ValueError("'channels' must be a list")

    normalized_channels = []
    for idx, channel in enumerate(channels):
        if not isinstance(channel, dict):
            raise ValueError(f"Channel entry at index {idx} must be an object")
        if "id" not in channel:
            raise ValueError(f"Channel entry at index {idx} is missing 'id'")
        normalized_channels.append(channel)

    return {
        "webhook_url": config.get("webhook_url", ""),
        "retry_attempts": _normalize_retry(config.get("retry_attempts"), 3),
        "retry_delay_seconds": _normalize_retry(config.get("retry_delay_seconds"), 5),
        "channels": normalized_channels,
    }


async def send_webhook(
    session: aiohttp.ClientSession,
    webhook_url: str,
    payload: Dict[str, Any],
    retry_attempts: int,
    retry_delay_seconds: int,
    logger: logging.Logger,
) -> bool:
    for attempt in range(1, retry_attempts + 1):
        try:
            async with session.post(webhook_url, json=payload) as response:
                if 200 <= response.status < 300:
                    logger.info(
                        "Webhook delivered (message_id=%s, attempt=%s)",
                        payload.get("message_id"),
                        attempt,
                    )
                    return True
                logger.warning(
                    "Webhook failed with status %s (attempt %s/%s)",
                    response.status,
                    attempt,
                    retry_attempts,
                )
        except aiohttp.ClientError as exc:
            logger.error(
                "Webhook request error on attempt %s/%s: %s",
                attempt,
                retry_attempts,
                exc,
            )

        if attempt < retry_attempts:
            await asyncio.sleep(retry_delay_seconds)

    logger.error("Failed to deliver webhook after %s attempts", retry_attempts)
    return False


async def start_listener(
    api_id: int,
    api_hash: str,
    phone: str,
    session_name: str,
    config: Dict[str, Any],
) -> None:
    logger = logging.getLogger("dealscout.listener")
    channel_ids = [channel["id"] for channel in config.get("channels", [])]
    webhook_url = config.get("webhook_url", "")
    retry_attempts = config.get("retry_attempts", 3)
    retry_delay_seconds = config.get("retry_delay_seconds", 5)

    if not webhook_url:
        raise ValueError("'webhook_url' is required in the channel config")
    if not channel_ids:
        logger.warning("No channels configured. Listener will run without monitored chats.")

    client = TelegramClient(session_name, api_id, api_hash)

    async with aiohttp.ClientSession() as http_session:

        @client.on(events.NewMessage(chats=channel_ids if channel_ids else None))
        async def handler(event: events.NewMessage.Event) -> None:
            message_text = event.raw_text or ""
            if not message_contains_url(message_text):
                return

            payload = {
                "source_channel_id": event.chat_id,
                "message": message_text,
                "message_id": event.id,
                "date": event.date.isoformat() if event.date else None,
            }

            await send_webhook(
                session=http_session,
                webhook_url=webhook_url,
                payload=payload,
                retry_attempts=retry_attempts,
                retry_delay_seconds=retry_delay_seconds,
                logger=logger,
            )

        await client.start(phone=phone)
        logger.info("Telegram listener started. Waiting for messages...")
        await client.run_until_disconnected()


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("dealscout.listener")

    api_id_raw = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    phone = os.getenv("TG_PHONE")
    session_name = os.getenv("DEALSCOUT_SESSION", "dealscout_session")
    config_path = os.getenv("DEALSCOUT_CONFIG", "channels.json")

    if not api_id_raw or not api_hash or not phone:
        raise ValueError("Missing required env vars: TG_API_ID, TG_API_HASH, TG_PHONE")

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise ValueError("TG_API_ID must be an integer") from exc

    config = load_config(config_path)

    try:
        asyncio.run(start_listener(api_id, api_hash, phone, session_name, config))
    except KeyboardInterrupt:
        logger.info("Listener stopped by user")


if __name__ == "__main__":
    main()

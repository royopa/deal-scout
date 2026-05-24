import asyncio
import json
import logging
import os
import re
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict

import aiohttp
import yaml
from dotenv import load_dotenv
from telethon import TelegramClient, events
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

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


class RuntimeConfig:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._lock = Lock()
        self._channel_ids: set[int] = set()
        self._webhook_url = ""
        self._retry_attempts = 3
        self._retry_delay_seconds = 5
        self.update(config)

    def update(self, config: Dict[str, Any]) -> None:
        channel_ids = {
            channel["id"]
            for channel in config.get("channels", [])
            if isinstance(channel, dict) and "id" in channel
        }

        with self._lock:
            self._channel_ids = channel_ids
            self._webhook_url = config.get("webhook_url", "")
            self._retry_attempts = config.get("retry_attempts", 3)
            self._retry_delay_seconds = config.get("retry_delay_seconds", 5)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "channel_ids": set(self._channel_ids),
                "webhook_url": self._webhook_url,
                "retry_attempts": self._retry_attempts,
                "retry_delay_seconds": self._retry_delay_seconds,
            }


class ConfigFileChangeHandler(FileSystemEventHandler):
    def __init__(
        self,
        config_path: Path,
        on_change: Callable[[], None],
        logger: logging.Logger,
    ) -> None:
        self.config_path = config_path.resolve()
        self.on_change = on_change
        self.logger = logger

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle_event(event)

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle_event(event)

    def on_moved(self, event: FileSystemEvent) -> None:
        self._handle_event(event)

    def _handle_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        src_matches = Path(event.src_path).resolve() == self.config_path
        dest_path = getattr(event, "dest_path", None)
        dest_matches = bool(dest_path) and Path(dest_path).resolve() == self.config_path
        if not src_matches and not dest_matches:
            return

        self.logger.info("Detected config file change: %s", self.config_path)
        self.on_change()


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
    config_state = RuntimeConfig(config)
    initial_snapshot = config_state.snapshot()

    if not initial_snapshot["webhook_url"]:
        raise ValueError("'webhook_url' is required in the channel config")
    if not initial_snapshot["channel_ids"]:
        logger.warning(
            "No channels configured. Listener will run without monitored chats."
        )

    config_path = Path(os.getenv("DEALSCOUT_CONFIG", "channels.json")).resolve()
    loop = asyncio.get_running_loop()

    def apply_runtime_config(new_config: Dict[str, Any]) -> None:
        if not new_config.get("webhook_url"):
            logger.error("Ignoring config reload because 'webhook_url' is missing")
            return

        config_state.update(new_config)
        snapshot = config_state.snapshot()
        logger.info(
            "Config reloaded. Monitoring %s channels.",
            len(snapshot["channel_ids"]),
        )

    def reload_config_from_disk() -> None:
        try:
            fresh_config = load_config(str(config_path))
        except (FileNotFoundError, OSError, ValueError) as exc:
            logger.error("Failed to reload config from %s: %s", config_path, exc)
            return

        loop.call_soon_threadsafe(apply_runtime_config, fresh_config)

    observer = None
    if config_path.exists():
        event_handler = ConfigFileChangeHandler(
            config_path, reload_config_from_disk, logger
        )
        observer = Observer()
        observer.schedule(event_handler, str(config_path.parent), recursive=False)
        observer.start()
        logger.info("Watching config file for changes: %s", config_path)
    else:
        logger.warning(
            "Config file watcher not started; file not found: %s", config_path
        )

    client = TelegramClient(session_name, api_id, api_hash)

    async with aiohttp.ClientSession() as http_session:

        @client.on(events.NewMessage())
        async def handler(event: events.NewMessage.Event) -> None:
            runtime_config = config_state.snapshot()
            channel_ids = runtime_config["channel_ids"]
            if not channel_ids or event.chat_id not in channel_ids:
                return

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
                webhook_url=runtime_config["webhook_url"],
                payload=payload,
                retry_attempts=runtime_config["retry_attempts"],
                retry_delay_seconds=runtime_config["retry_delay_seconds"],
                logger=logger,
            )

        try:
            await client.start(phone=phone)
            logger.info("Telegram listener started. Waiting for messages...")
            await client.run_until_disconnected()
        finally:
            if observer is not None:
                observer.stop()
                observer.join(timeout=5)
                logger.info("Config file watcher stopped")


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

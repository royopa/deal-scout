import csv
import asyncio
import base64
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict

import aiohttp
import yaml
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

URL_PATTERN = re.compile(
    r"(https?://\S+|www\.\S+|\b\S+\.(?:com|net|org|io|co|deals)\b)",
    re.IGNORECASE,
)

CSV_FIELDNAMES = [
    "archived_at",
    "processed_at",
    "source_channel_id",
    "source_channel_title",
    "source_channel_username",
    "source_channel_type",
    "sender_id",
    "sender_name",
    "sender_username",
    "message_id",
    "message_date",
    "message_length",
    "message_text",
    "contains_url",
    "extracted_urls",
    "url_count",
    "has_image",
    "image_base64",
    "webhook_status",
]

CSV_WRITE_LOCK = Lock()


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


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def extract_urls(message: str | None) -> list[str]:
    if not message:
        return []
    return URL_PATTERN.findall(message)


async def extract_image_base64(
    client: TelegramClient,
    event: events.NewMessage.Event,
) -> tuple[str | None, bool]:
    message = getattr(event, "message", None)
    has_image = bool(
        getattr(event, "photo", None) or getattr(message, "photo", None)
    )
    if not has_image:
        return None, False

    media_bytes = await client.download_media(event.message, file=bytes)
    if not media_bytes:
        return None, True

    if not isinstance(media_bytes, (bytes, bytearray)):
        return None, True

    return base64.b64encode(media_bytes).decode("ascii"), True


def _format_person_name(entity: Any) -> str | None:
    if entity is None:
        return None

    title = getattr(entity, "title", None)
    if title:
        return title

    parts = [
        part
        for part in [
            getattr(entity, "first_name", None),
            getattr(entity, "last_name", None),
        ]
        if part
    ]
    if parts:
        return " ".join(parts)

    username = getattr(entity, "username", None)
    return username


def _format_entity_type(entity: Any) -> str | None:
    if entity is None:
        return None
    return type(entity).__name__


def _env_flag(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


async def log_visible_monitored_channels(
    client: TelegramClient,
    monitored_channel_ids: set[int],
    logger: logging.Logger,
) -> None:
    if not monitored_channel_ids:
        logger.warning(
            "Verbose startup check skipped because no channels are configured"
        )
        return

    try:
        dialogs = await client.get_dialogs()
    except (AttributeError, OSError, RuntimeError, TypeError) as exc:
        logger.debug(
            "Unable to inspect visible Telegram dialogs during startup: %s",
            exc,
        )
        return

    visible_ids: set[int] = set()
    visible_channels: list[str] = []

    for dialog in dialogs or []:
        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue

        try:
            peer_id = utils.get_peer_id(entity)
        except (AttributeError, TypeError, ValueError):
            continue

        if peer_id not in monitored_channel_ids:
            continue

        visible_ids.add(peer_id)
        title = _format_person_name(entity) or getattr(dialog, "name", None)
        visible_channels.append(
            f"{peer_id} ({title or 'unknown'})"
        )

    if visible_channels:
        logger.info(
            "Verbose startup: monitored chats visible in this session: %s",
            ", ".join(visible_channels),
        )

    missing_channel_ids = sorted(monitored_channel_ids - visible_ids)
    if missing_channel_ids:
        logger.warning(
            (
                "Verbose startup: monitored chat ids not visible in this "
                "session: %s"
            ),
            missing_channel_ids,
        )


def build_archive_record(
    event: events.NewMessage.Event,
    message_text: str,
    contains_url: bool,
    webhook_status: str,
    image_base64: str | None,
    has_image: bool,
    chat: Any = None,
    sender: Any = None,
) -> Dict[str, Any]:
    extracted_urls = extract_urls(message_text)
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "archived_at": now_utc,
        "processed_at": now_utc,
        "source_channel_id": event.chat_id,
        "source_channel_title": _format_person_name(chat),
        "source_channel_username": getattr(chat, "username", None),
        "source_channel_type": _format_entity_type(chat),
        "sender_id": event.sender_id,
        "sender_name": _format_person_name(sender),
        "sender_username": getattr(sender, "username", None),
        "message_id": event.id,
        "message_date": event.date.isoformat() if event.date else None,
        "message_length": len(message_text),
        "message_text": message_text,
        "contains_url": contains_url,
        "extracted_urls": " | ".join(extracted_urls),
        "url_count": len(extracted_urls),
        "has_image": has_image,
        "image_base64": image_base64,
        "webhook_status": webhook_status,
    }


def archive_message_to_csv(
    csv_path: str | Path,
    record: Dict[str, Any],
) -> None:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    row = {field_name: record.get(field_name) for field_name in CSV_FIELDNAMES}

    with CSV_WRITE_LOCK:
        needs_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_FIELDNAMES)
            if needs_header:
                writer.writeheader()
            writer.writerow(row)


async def process_monitored_message(
    event: Any,
    client: TelegramClient,
    runtime_config: Dict[str, Any],
    http_session: aiohttp.ClientSession,
    archive_path: str | Path,
    logger: logging.Logger,
) -> Dict[str, Any]:
    logger.info(
        "New message event received: chat_id=%s message_id=%s",
        event.chat_id,
        event.id,
    )

    message_text = event.raw_text or ""
    contains_url = message_contains_url(message_text)
    webhook_status = "skipped_no_url"
    chat = None
    sender = None
    image_base64 = None
    has_image = False

    try:
        chat = await event.get_chat()
    except (AttributeError, OSError, RuntimeError, TypeError) as exc:
        logger.debug(
            "Could not load chat metadata for message %s: %s",
            event.id,
            exc,
        )

    try:
        sender = await event.get_sender()
    except (AttributeError, OSError, RuntimeError, TypeError) as exc:
        logger.debug(
            "Could not load sender metadata for message %s: %s",
            event.id,
            exc,
        )

    try:
        image_base64, has_image = await extract_image_base64(client, event)
    except (AttributeError, OSError, RuntimeError, TypeError) as exc:
        logger.debug(
            "Could not extract image media for message %s: %s",
            event.id,
            exc,
        )

    logger.info(
        (
            "Message received from monitored chat: "
            "chat_id=%s title=%s message_id=%s contains_url=%s"
        ),
        event.chat_id,
        _format_person_name(chat) or "unknown",
        event.id,
        contains_url,
    )
    logger.info(
        (
            "Media inspection for message_id=%s: "
            "has_image=%s image_base64=%s"
        ),
        event.id,
        has_image,
        "present" if image_base64 else "absent",
    )
    logger.info(
        (
            "Archiving message to CSV: "
            "message_id=%s length=%s url_count=%s"
        ),
        event.id,
        len(message_text),
        len(extract_urls(message_text)),
    )

    payload = {
        "source_channel_id": event.chat_id,
        "message": message_text,
        "message_id": event.id,
        "date": event.date.isoformat() if event.date else None,
    }

    webhook_enabled = runtime_config.get("webhook_enabled", False)

    if contains_url and webhook_enabled:
        logger.info(
            "URL detected for message_id=%s; sending webhook",
            event.id,
        )
        webhook_sent = await send_webhook(
            session=http_session,
            webhook_url=runtime_config["webhook_url"],
            payload=payload,
            retry_attempts=runtime_config["retry_attempts"],
            retry_delay_seconds=runtime_config["retry_delay_seconds"],
            logger=logger,
        )
        webhook_status = "sent" if webhook_sent else "failed"
        logger.info(
            "Webhook processing finished for message_id=%s status=%s",
            event.id,
            webhook_status,
        )
    elif contains_url and not webhook_enabled:
        logger.info(
            (
                "URL detected for message_id=%s; webhook is disabled, "
                "message will only be archived"
            ),
            event.id,
        )
        webhook_status = "skipped_webhook_disabled"
    else:
        logger.info(
            (
                "No URL found for message_id=%s; "
                "message will only be archived"
            ),
            event.id,
        )

    archive_record = build_archive_record(
        event=event,
        message_text=message_text,
        contains_url=contains_url,
        webhook_status=webhook_status,
        image_base64=image_base64,
        has_image=has_image,
        chat=chat,
        sender=sender,
    )

    archive_message_to_csv(archive_path, archive_record)
    logger.info(
        (
            "Message archived successfully: "
            "message_id=%s csv=%s status=%s"
        ),
        event.id,
        archive_path,
        webhook_status,
    )

    return archive_record


class RuntimeConfig:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._lock = Lock()
        self._channel_ids: set[int] = set()
        self._webhook_url = ""
        self._retry_attempts = 3
        self._retry_delay_seconds = 5
        self._webhook_enabled = False
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
            self._webhook_enabled = _normalize_bool(
                config.get("webhook_enabled"),
                False,
            )

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "channel_ids": set(self._channel_ids),
                "webhook_url": self._webhook_url,
                "retry_attempts": self._retry_attempts,
                "retry_delay_seconds": self._retry_delay_seconds,
                "webhook_enabled": self._webhook_enabled,
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
        dest_matches = bool(dest_path)
        dest_matches = dest_matches and (
            Path(dest_path).resolve() == self.config_path
        )
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
        "retry_delay_seconds": _normalize_retry(
            config.get("retry_delay_seconds"),
            5,
        ),
        "webhook_enabled": _normalize_bool(
            config.get("webhook_enabled"),
            False,
        ),
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
    archive_csv_path: str = "message_archive.csv",
) -> None:
    logger = logging.getLogger("dealscout.listener")
    config_state = RuntimeConfig(config)
    initial_snapshot = config_state.snapshot()

    if initial_snapshot["webhook_enabled"] and not initial_snapshot[
        "webhook_url"
    ]:
        raise ValueError("'webhook_url' is required in the channel config")
    if not initial_snapshot["channel_ids"]:
        logger.warning(
            "No channels configured. "
            "Listener will run without monitored chats."
        )
    if not initial_snapshot["webhook_enabled"]:
        logger.info(
            "Webhook delivery is disabled; messages will only be archived"
        )

    config_path = Path(
        os.getenv("DEALSCOUT_CONFIG", "channels.json")
    ).resolve()
    archive_path = Path(
        os.getenv("DEALSCOUT_ARCHIVE_CSV", archive_csv_path)
    ).resolve()
    loop = asyncio.get_running_loop()

    def apply_runtime_config(new_config: Dict[str, Any]) -> None:
        if new_config.get("webhook_enabled") and not new_config.get(
            "webhook_url"
        ):
            logger.error(
                "Ignoring config reload because 'webhook_url' is missing"
            )
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
            logger.error(
                "Failed to reload config from %s: %s",
                config_path,
                exc,
            )
            return

        loop.call_soon_threadsafe(apply_runtime_config, fresh_config)

    observer = None
    if config_path.exists():
        event_handler = ConfigFileChangeHandler(
            config_path, reload_config_from_disk, logger
        )
        observer = Observer()
        observer.schedule(
            event_handler,
            str(config_path.parent),
            recursive=False,
        )
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
            logger.info(
                "New Telegram message observed: chat_id=%s message_id=%s",
                event.chat_id,
                event.id,
            )
            runtime_config = config_state.snapshot()
            channel_ids = runtime_config["channel_ids"]
            if not channel_ids or event.chat_id not in channel_ids:
                logger.info(
                    (
                        "Ignoring message_id=%s from chat_id=%s because it "
                        "is not in the monitored channel list"
                    ),
                    event.id,
                    event.chat_id,
                )
                return

            await process_monitored_message(
                event=event,
                client=client,
                runtime_config=runtime_config,
                http_session=http_session,
                archive_path=archive_path,
                logger=logger,
            )

        try:
            await client.start(phone=phone)
            me = await client.get_me()
            startup_snapshot = config_state.snapshot()
            logger.info(
                "Connected to Telegram as id=%s username=%s phone=%s",
                getattr(me, "id", None),
                getattr(me, "username", None),
                getattr(me, "phone", None),
            )
            logger.info(
                "Monitoring channel ids: %s",
                sorted(startup_snapshot["channel_ids"]),
            )
            verbose_startup = _env_flag(
                os.getenv("DEALSCOUT_VERBOSE_STARTUP")
            )
            verbose_startup = verbose_startup or logger.isEnabledFor(
                logging.DEBUG
            )
            if verbose_startup:
                await log_visible_monitored_channels(
                    client=client,
                    monitored_channel_ids=startup_snapshot["channel_ids"],
                    logger=logger,
                )
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
        raise ValueError(
            "Missing required env vars: TG_API_ID, TG_API_HASH, TG_PHONE"
        )

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise ValueError("TG_API_ID must be an integer") from exc

    config = load_config(config_path)

    env_webhook_enabled = os.getenv("DEALSCOUT_ENABLE_WEBHOOK")
    if env_webhook_enabled is not None:
        config["webhook_enabled"] = _normalize_bool(
            env_webhook_enabled,
            config.get("webhook_enabled", False),
        )

    try:
        asyncio.run(
            start_listener(api_id, api_hash, phone, session_name, config)
        )
    except KeyboardInterrupt:
        logger.info("Listener stopped by user")


if __name__ == "__main__":
    main()

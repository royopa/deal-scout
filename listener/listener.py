import csv
import asyncio
import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict
from urllib.parse import urlparse

import aiohttp
import time
import yaml
from dotenv import load_dotenv
from message_parser import (
    extract_urls,
    message_contains_url,
    parse_deal_message,
)
from telethon.errors import SessionPasswordNeededError
from telethon import TelegramClient, events, utils
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class FatalListenerError(RuntimeError):
    pass

CSV_SCHEMA_VERSION = "v3"

CSV_FIELDNAMES = [
    "archived_at",
    "processed_at",
    "schema_version",
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
    "all_urls",
    "url_count",
    "message_product_index",
    "message_product_count",
    "product_url",
    "product_domain",
    "resolved_final_url",
    "resolved_domain",
    "official_product_url",
    "official_store",
    "affiliate_url",
    "affiliate_status",
    "affiliate_error",
    "product_price",
    "price_currency",
    "product_original_price",
    "product_price_text",
    "product_original_price_text",
    "coupon_code",
    "coupon_text",
    "product_description",
    "is_affiliate_url",
    "parse_status",
    "parse_confidence",
    "has_image",
    "image_base64",
    "webhook_status",
]

CSV_WRITE_LOCK = Lock()
DEFAULT_PARSED_PRODUCT = {
    "product_url": None,
    "product_domain": None,
    "resolved_final_url": None,
    "resolved_domain": None,
    "official_product_url": None,
    "official_store": "unknown",
    "affiliate_url": None,
    "affiliate_status": "skipped",
    "affiliate_error": None,
    "product_price": None,
    "price_currency": None,
    "product_original_price": None,
    "product_price_text": None,
    "product_original_price_text": None,
    "coupon_code": None,
    "coupon_text": None,
    "product_description": None,
    "is_affiliate_url": False,
    "parse_status": None,
    "parse_confidence": None,
}

OFFICIAL_STORE_DOMAINS: dict[str, tuple[str, ...]] = {
    "amazon": ("amazon.com.br", "amazon.com"),
    "mercadolivre": ("mercadolivre.com.br", "mercadolivre.com"),
    "aliexpress": ("aliexpress.com",),
    "shopee": ("shopee.com.br", "shopee.com"),
    "magalu": ("magazineluiza.com.br", "magalu.com"),
    "americanas": ("americanas.com.br",),
    "casasbahia": ("casasbahia.com.br",),
    "pontofrio": ("pontofrio.com.br",),
    "kabum": ("kabum.com.br",),
    "carrefour": ("carrefour.com.br",),
    "extra": ("extra.com.br",),
    "fastshop": ("fastshop.com.br",),
}


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


def _normalize_timeout(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


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


def _first_parsed_product(parsed_message: Dict[str, Any]) -> Dict[str, Any]:
    products = parsed_message.get("products") or []
    if products:
        return products[0]
    return DEFAULT_PARSED_PRODUCT.copy()


def _normalize_http_url(url: str | None) -> str | None:
    if not url:
        return None
    normalized = url.strip()
    if not normalized:
        return None
    if not normalized.lower().startswith(("http://", "https://")):
        normalized = f"https://{normalized}"
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return normalized


def _url_domain(url: str | None) -> str | None:
    normalized = _normalize_http_url(url)
    if not normalized:
        return None
    parsed = urlparse(normalized)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def _detect_official_store(domain: str | None) -> str:
    if not domain:
        return "unknown"
    for store_name, domains in OFFICIAL_STORE_DOMAINS.items():
        if any(domain == candidate or domain.endswith(f".{candidate}") for candidate in domains):
            return store_name
    return "unknown"


def _resolve_fallback_from_original_url(product_url: str | None) -> Dict[str, Any]:
    normalized = _normalize_http_url(product_url)
    resolved_domain = _url_domain(normalized)
    official_store = _detect_official_store(resolved_domain)
    return {
        "resolved_final_url": normalized or product_url,
        "resolved_domain": resolved_domain,
        "official_product_url": normalized if official_store != "unknown" else None,
        "official_store": official_store,
    }


def _extract_affiliate_candidate_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("affiliate_url", "url", "short_url", "link"):
        candidate = payload.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


async def resolve_product_url(
    session: aiohttp.ClientSession,
    product_url: str | None,
    timeout_seconds: float,
    max_hops: int,
    retry_attempts: int,
    logger: logging.Logger,
) -> Dict[str, Any]:
    fallback = _resolve_fallback_from_original_url(product_url)
    normalized = _normalize_http_url(product_url)
    if not normalized:
        return fallback

    attempts = max(1, retry_attempts)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    last_error = ""

    for attempt in range(1, attempts + 1):
        try:
            async with session.get(
                normalized,
                allow_redirects=True,
                max_redirects=max_hops,
                timeout=timeout,
            ) as response:
                final_url = str(response.url) if response.url else normalized
                final_domain = _url_domain(final_url)
                official_store = _detect_official_store(final_domain)
                return {
                    "resolved_final_url": final_url,
                    "resolved_domain": final_domain,
                    "official_product_url": (
                        final_url if official_store != "unknown" else None
                    ),
                    "official_store": official_store,
                }
        except aiohttp.TooManyRedirects:
            last_error = "too_many_redirects"
        except asyncio.TimeoutError:
            last_error = "timeout"
        except aiohttp.ClientError:
            last_error = "network_error"
        except ValueError:
            last_error = "invalid_url"

        if attempt < attempts:
            await asyncio.sleep(0.1)

    logger.info(
        "URL resolution fallback for product_url=%s reason=%s",
        product_url,
        last_error or "unknown",
    )
    return fallback


async def request_affiliate_link(
    session: aiohttp.ClientSession,
    official_product_url: str | None,
    official_store: str,
    runtime_config: Dict[str, Any],
    logger: logging.Logger,
) -> Dict[str, Any]:
    if not runtime_config.get("affiliate_enabled", False):
        return {
            "affiliate_url": None,
            "affiliate_status": "skipped",
            "affiliate_error": "affiliate_disabled",
        }
    if not official_product_url:
        return {
            "affiliate_url": None,
            "affiliate_status": "skipped",
            "affiliate_error": "missing_official_product_url",
        }

    api_url = (runtime_config.get("affiliate_api_url") or "").strip()
    if not api_url:
        return {
            "affiliate_url": None,
            "affiliate_status": "skipped",
            "affiliate_error": "missing_affiliate_api_url",
        }

    token = (runtime_config.get("affiliate_api_token") or "").strip()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    payload = {
        "url": official_product_url,
        "store": official_store,
    }
    attempts = max(1, runtime_config.get("affiliate_retry_attempts", 2))
    timeout = aiohttp.ClientTimeout(
        total=runtime_config.get("affiliate_timeout_seconds", 5.0)
    )
    last_error = "unknown_error"

    for attempt in range(1, attempts + 1):
        try:
            async with session.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=timeout,
            ) as response:
                if 200 <= response.status < 300:
                    data = await response.json(content_type=None)
                    affiliate_url = _extract_affiliate_candidate_url(data)
                    if affiliate_url:
                        return {
                            "affiliate_url": affiliate_url,
                            "affiliate_status": "success",
                            "affiliate_error": None,
                        }
                    last_error = "invalid_affiliate_response"
                else:
                    last_error = f"http_{response.status}"
        except asyncio.TimeoutError:
            last_error = "timeout"
        except aiohttp.ClientError:
            last_error = "network_error"
        except ValueError:
            last_error = "invalid_json"

        if attempt < attempts:
            await asyncio.sleep(0.1)

    logger.warning(
        "Affiliate API fallback for official_product_url=%s reason=%s",
        official_product_url,
        last_error,
    )
    return {
        "affiliate_url": None,
        "affiliate_status": "failed",
        "affiliate_error": last_error,
    }


async def enrich_products(
    session: aiohttp.ClientSession,
    products: list[Dict[str, Any]],
    runtime_config: Dict[str, Any],
    logger: logging.Logger,
    message_id: Any,
) -> tuple[list[Dict[str, Any]], Dict[str, int]]:
    enriched_products: list[Dict[str, Any]] = []
    counters = {
        "resolved_products": 0,
        "affiliate_success": 0,
        "affiliate_failed": 0,
        "affiliate_skipped": 0,
    }

    for index, product in enumerate(products, start=1):
        product_url = product.get("product_url")
        enriched = dict(product)
        enriched.update(
            {
                "resolved_final_url": None,
                "resolved_domain": None,
                "official_product_url": None,
                "official_store": "unknown",
                "affiliate_url": None,
                "affiliate_status": "skipped",
                "affiliate_error": None,
            }
        )

        if runtime_config.get("url_resolution_enabled", True):
            resolved_data = await resolve_product_url(
                session=session,
                product_url=product_url,
                timeout_seconds=runtime_config.get("url_resolve_timeout_seconds", 5.0),
                max_hops=runtime_config.get("url_resolve_max_hops", 5),
                retry_attempts=runtime_config.get("url_resolve_retry_attempts", 2),
                logger=logger,
            )
        else:
            resolved_data = _resolve_fallback_from_original_url(product_url)

        enriched.update(resolved_data)
        if enriched.get("resolved_final_url"):
            counters["resolved_products"] += 1

        affiliate_data = await request_affiliate_link(
            session=session,
            official_product_url=enriched.get("official_product_url"),
            official_store=enriched.get("official_store") or "unknown",
            runtime_config=runtime_config,
            logger=logger,
        )
        enriched.update(affiliate_data)

        if affiliate_data["affiliate_status"] == "success":
            counters["affiliate_success"] += 1
        elif affiliate_data["affiliate_status"] == "failed":
            counters["affiliate_failed"] += 1
        else:
            counters["affiliate_skipped"] += 1

        logger.info(
            (
                "Product enrichment message_id=%s index=%s "
                "original_url=%s resolved_url=%s official_store=%s affiliate_status=%s"
            ),
            message_id,
            index,
            product_url,
            enriched.get("resolved_final_url"),
            enriched.get("official_store"),
            enriched.get("affiliate_status"),
        )
        enriched_products.append(enriched)

    return enriched_products, counters


def _env_flag(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _can_use_interactive_auth() -> bool:
    if _env_flag(os.getenv("DEALSCOUT_FORCE_INTERACTIVE_LOGIN")):
        return True

    if sys.stdin.isatty() and sys.stdout.isatty():
        return True

    try:
        with open("/dev/tty", "r"):
            return True
    except OSError:
        return False


def _read_secret_from_tty(prompt: str, logger: logging.Logger) -> str:
    try:
        with open("/dev/tty", "r+") as tty:
            tty.write(prompt)
            tty.flush()
            value = tty.readline().strip()
            return value
    except OSError:
        logger.debug("/dev/tty not available; falling back to stdin prompt")
        return input(prompt).strip()


async def _interactive_telegram_login(
    client: TelegramClient,
    phone: str,
    logger: logging.Logger,
) -> None:
    logger.info("Requesting Telegram login code")
    await client.send_code_request(phone)
    code = _read_secret_from_tty("Please enter the Telegram code: ", logger)
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        logger.info("Telegram 2FA password required")
        password = _read_secret_from_tty(
            "Please enter your Telegram 2FA password: ",
            logger,
        )
        await client.sign_in(password=password)


def _config_example_path() -> Path:
    return Path(__file__).resolve().with_name("channels.json.example")


def resolve_config_path(
    config_path: str,
    logger: logging.Logger | None = None,
) -> Path:
    path = Path(config_path).expanduser()
    if path.exists():
        return path.resolve()

    candidate_paths: list[Path] = []
    if not path.is_absolute():
        candidate_paths.append(Path("/data") / path)
    elif path.parent == Path("/"):
        candidate_paths.append(Path("/data") / path.name)

    for candidate in candidate_paths:
        if candidate.exists():
            return candidate.resolve()

    example_path = _config_example_path()
    if not example_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            "No bundled channels.json.example was found to bootstrap it."
        )

    bootstrap_target = candidate_paths[0] if candidate_paths else path
    bootstrap_target.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_target.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")

    if logger is not None:
        logger.warning(
            "Config file missing; bootstrapped default config from %s to %s",
            example_path,
            bootstrap_target,
        )

    return bootstrap_target.resolve()


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
    parsed_message: Dict[str, Any] | None = None,
    parsed_product: Dict[str, Any] | None = None,
    product_index: int = 1,
    chat: Any = None,
    sender: Any = None,
) -> Dict[str, Any]:
    parsed_message = parsed_message or parse_deal_message(message_text)
    if parsed_product is None:
        parsed_product = _first_parsed_product(parsed_message)
    all_urls = parsed_message["all_urls"]
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "archived_at": now_utc,
        "processed_at": now_utc,
        "schema_version": CSV_SCHEMA_VERSION,
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
        "extracted_urls": " | ".join(all_urls),
        "all_urls": " | ".join(all_urls),
        "url_count": parsed_message["url_count"],
        "message_product_index": product_index,
        "message_product_count": parsed_message["product_count"],
        "product_url": parsed_product.get("product_url"),
        "product_domain": parsed_product.get("product_domain"),
        "resolved_final_url": parsed_product.get("resolved_final_url"),
        "resolved_domain": parsed_product.get("resolved_domain"),
        "official_product_url": parsed_product.get("official_product_url"),
        "official_store": parsed_product.get("official_store"),
        "affiliate_url": parsed_product.get("affiliate_url"),
        "affiliate_status": parsed_product.get("affiliate_status"),
        "affiliate_error": parsed_product.get("affiliate_error"),
        "product_price": parsed_product.get("product_price"),
        "price_currency": parsed_product.get("price_currency"),
        "product_original_price": parsed_product.get("product_original_price"),
        "product_price_text": parsed_product.get("product_price_text"),
        "product_original_price_text": parsed_product.get("product_original_price_text"),
        "coupon_code": parsed_product.get("coupon_code"),
        "coupon_text": parsed_product.get("coupon_text"),
        "product_description": parsed_product.get("product_description"),
        "is_affiliate_url": parsed_product.get("is_affiliate_url"),
        "parse_status": parsed_product.get("parse_status"),
        "parse_confidence": parsed_product.get("parse_confidence"),
        "has_image": has_image,
        "image_base64": image_base64,
        "webhook_status": webhook_status,
    }


def build_archive_records(
    event: events.NewMessage.Event,
    message_text: str,
    contains_url: bool,
    webhook_status: str,
    image_base64: str | None,
    has_image: bool,
    parsed_message: Dict[str, Any] | None = None,
    chat: Any = None,
    sender: Any = None,
) -> list[Dict[str, Any]]:
    parsed_message = parsed_message or parse_deal_message(message_text)
    records: list[Dict[str, Any]] = []

    for index, parsed_product in enumerate(parsed_message["products"], start=1):
        records.append(
            build_archive_record(
                event=event,
                message_text=message_text,
                contains_url=contains_url,
                webhook_status=webhook_status,
                image_base64=image_base64,
                has_image=has_image,
                parsed_message=parsed_message,
                parsed_product=parsed_product,
                product_index=index,
                chat=chat,
                sender=sender,
            )
        )

    return records


def _resolve_archive_target_path(csv_path: str | Path) -> Path:
    path = Path(csv_path)
    if not path.exists() or path.stat().st_size == 0:
        return path

    with path.open(encoding="utf-8", newline="") as file:
        header = next(csv.reader(file), [])

    if header == CSV_FIELDNAMES:
        return path

    versioned_path = path.with_name(f"{path.stem}_{CSV_SCHEMA_VERSION}{path.suffix}")
    if not versioned_path.exists() or versioned_path.stat().st_size == 0:
        return versioned_path

    return versioned_path


def archive_message_to_csv(
    csv_path: str | Path,
    record: Dict[str, Any],
) -> Path:
    path = _resolve_archive_target_path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    row = {field_name: record.get(field_name) for field_name in CSV_FIELDNAMES}

    with CSV_WRITE_LOCK:
        needs_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=CSV_FIELDNAMES,
                quoting=csv.QUOTE_ALL,
            )
            if needs_header:
                writer.writeheader()
            writer.writerow(row)
    return path


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
    parsed_message = parse_deal_message(message_text)
    enriched_products, enrichment_counters = await enrich_products(
        session=http_session,
        products=parsed_message["products"],
        runtime_config=runtime_config,
        logger=logger,
        message_id=event.id,
    )
    parsed_message["products"] = enriched_products
    contains_url = parsed_message["url_count"] > 0
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
            "message_id=%s length=%s url_count=%s product_count=%s"
        ),
        event.id,
        len(message_text),
        parsed_message["url_count"],
        parsed_message["product_count"],
    )

    primary_product = _first_parsed_product(parsed_message)
    payload = {
        "source_channel_id": event.chat_id,
        "message": message_text,
        "message_id": event.id,
        "date": event.date.isoformat() if event.date else None,
        "contains_url": contains_url,
        "extracted_urls": " | ".join(parsed_message["all_urls"]),
        "all_urls": parsed_message["all_urls"],
        "url_count": parsed_message["url_count"],
        "product_count": parsed_message["product_count"],
        "structured_products": parsed_message["products"],
        "schema_version": CSV_SCHEMA_VERSION,
        "product_url": primary_product.get("product_url"),
        "product_domain": primary_product.get("product_domain"),
        "resolved_final_url": primary_product.get("resolved_final_url"),
        "resolved_domain": primary_product.get("resolved_domain"),
        "official_product_url": primary_product.get("official_product_url"),
        "official_store": primary_product.get("official_store"),
        "affiliate_url": primary_product.get("affiliate_url"),
        "affiliate_status": primary_product.get("affiliate_status"),
        "affiliate_error": primary_product.get("affiliate_error"),
        "product_price": primary_product.get("product_price"),
        "price_currency": primary_product.get("price_currency"),
        "coupon_code": primary_product.get("coupon_code"),
        "coupon_text": primary_product.get("coupon_text"),
        "product_description": primary_product.get("product_description"),
        "is_affiliate_url": primary_product.get("is_affiliate_url"),
        "parse_status": primary_product.get("parse_status"),
        "parse_confidence": primary_product.get("parse_confidence"),
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

    archive_records = build_archive_records(
        event=event,
        message_text=message_text,
        contains_url=contains_url,
        webhook_status=webhook_status,
        image_base64=image_base64,
        has_image=has_image,
        parsed_message=parsed_message,
        chat=chat,
        sender=sender,
    )

    final_archive_path: Path | None = None
    for archive_record in archive_records:
        final_archive_path = archive_message_to_csv(archive_path, archive_record)
    logger.info(
        (
            "Enrichment summary for message_id=%s: resolved_products=%s "
            "affiliate_success=%s affiliate_failed=%s affiliate_skipped=%s"
        ),
        event.id,
        enrichment_counters["resolved_products"],
        enrichment_counters["affiliate_success"],
        enrichment_counters["affiliate_failed"],
        enrichment_counters["affiliate_skipped"],
    )
    logger.info(
        (
            "Message archived successfully: "
            "message_id=%s csv=%s status=%s rows=%s"
        ),
        event.id,
        final_archive_path or Path(archive_path),
        webhook_status,
        len(archive_records),
    )

    return {
        "records": archive_records,
        "webhook_status": webhook_status,
        "product_count": len(archive_records),
        "url_count": parsed_message["url_count"],
    }


class RuntimeConfig:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._lock = Lock()
        self._channel_ids: set[int] = set()
        self._webhook_url = ""
        self._retry_attempts = 3
        self._retry_delay_seconds = 5
        self._webhook_enabled = False
        self._url_resolution_enabled = True
        self._url_resolve_timeout_seconds = 5.0
        self._url_resolve_max_hops = 5
        self._url_resolve_retry_attempts = 2
        self._affiliate_enabled = False
        self._affiliate_api_url = ""
        self._affiliate_api_token = ""
        self._affiliate_timeout_seconds = 5.0
        self._affiliate_retry_attempts = 2
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
            self._url_resolution_enabled = _normalize_bool(
                config.get("url_resolution_enabled"),
                True,
            )
            self._url_resolve_timeout_seconds = _normalize_timeout(
                config.get("url_resolve_timeout_seconds"),
                5.0,
            )
            self._url_resolve_max_hops = _normalize_retry(
                config.get("url_resolve_max_hops"),
                5,
            )
            self._url_resolve_retry_attempts = _normalize_retry(
                config.get("url_resolve_retry_attempts"),
                2,
            )
            self._affiliate_enabled = _normalize_bool(
                config.get("affiliate_enabled"),
                False,
            )
            self._affiliate_api_url = (config.get("affiliate_api_url") or "").strip()
            self._affiliate_api_token = (config.get("affiliate_api_token") or "").strip()
            self._affiliate_timeout_seconds = _normalize_timeout(
                config.get("affiliate_timeout_seconds"),
                5.0,
            )
            self._affiliate_retry_attempts = _normalize_retry(
                config.get("affiliate_retry_attempts"),
                2,
            )

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "channel_ids": set(self._channel_ids),
                "webhook_url": self._webhook_url,
                "retry_attempts": self._retry_attempts,
                "retry_delay_seconds": self._retry_delay_seconds,
                "webhook_enabled": self._webhook_enabled,
                "url_resolution_enabled": self._url_resolution_enabled,
                "url_resolve_timeout_seconds": self._url_resolve_timeout_seconds,
                "url_resolve_max_hops": self._url_resolve_max_hops,
                "url_resolve_retry_attempts": self._url_resolve_retry_attempts,
                "affiliate_enabled": self._affiliate_enabled,
                "affiliate_api_url": self._affiliate_api_url,
                "affiliate_api_token": self._affiliate_api_token,
                "affiliate_timeout_seconds": self._affiliate_timeout_seconds,
                "affiliate_retry_attempts": self._affiliate_retry_attempts,
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


def load_config(
    config_path: str,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    path = resolve_config_path(config_path, logger=logger)

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
        "url_resolution_enabled": _normalize_bool(
            config.get("url_resolution_enabled"),
            True,
        ),
        "url_resolve_timeout_seconds": _normalize_timeout(
            config.get("url_resolve_timeout_seconds"),
            5.0,
        ),
        "url_resolve_max_hops": _normalize_retry(
            config.get("url_resolve_max_hops"),
            5,
        ),
        "url_resolve_retry_attempts": _normalize_retry(
            config.get("url_resolve_retry_attempts"),
            2,
        ),
        "affiliate_enabled": _normalize_bool(
            config.get("affiliate_enabled"),
            False,
        ),
        "affiliate_api_url": (config.get("affiliate_api_url") or "").strip(),
        "affiliate_api_token": (config.get("affiliate_api_token") or "").strip(),
        "affiliate_timeout_seconds": _normalize_timeout(
            config.get("affiliate_timeout_seconds"),
            5.0,
        ),
        "affiliate_retry_attempts": _normalize_retry(
            config.get("affiliate_retry_attempts"),
            2,
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

    config_path = resolve_config_path(
        os.getenv("DEALSCOUT_CONFIG", "channels.json")
    )
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
            fresh_config = load_config(str(config_path), logger=logger)
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
            await client.connect()
            if not await client.is_user_authorized():
                if _can_use_interactive_auth():
                    logger.warning(
                        "No authorized Telegram session found; starting interactive login"
                    )
                    await _interactive_telegram_login(client, phone, logger)
                else:
                    raise FatalListenerError(
                        "Telegram session is not authorized. Mount a pre-authenticated "
                        "session file in /data, or run the listener attached to a terminal "
                        "for the first login so Telethon can prompt for the code."
                    )

            if not await client.is_user_authorized():
                raise FatalListenerError(
                    "Telegram session is not authorized. "
                    "Interactive login did not complete successfully."
                )

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

    config = load_config(config_path, logger=logger)

    env_webhook_enabled = os.getenv("DEALSCOUT_ENABLE_WEBHOOK")
    if env_webhook_enabled is not None:
        config["webhook_enabled"] = _normalize_bool(
            env_webhook_enabled,
            config.get("webhook_enabled", False),
        )

    env_url_resolution_enabled = os.getenv("DEALSCOUT_ENABLE_URL_RESOLUTION")
    if env_url_resolution_enabled is not None:
        config["url_resolution_enabled"] = _normalize_bool(
            env_url_resolution_enabled,
            config.get("url_resolution_enabled", True),
        )

    env_url_resolve_timeout = os.getenv("DEALSCOUT_URL_RESOLVE_TIMEOUT_SECONDS")
    if env_url_resolve_timeout is not None:
        config["url_resolve_timeout_seconds"] = _normalize_timeout(
            env_url_resolve_timeout,
            config.get("url_resolve_timeout_seconds", 5.0),
        )

    env_url_resolve_max_hops = os.getenv("DEALSCOUT_URL_RESOLVE_MAX_HOPS")
    if env_url_resolve_max_hops is not None:
        config["url_resolve_max_hops"] = _normalize_retry(
            env_url_resolve_max_hops,
            config.get("url_resolve_max_hops", 5),
        )

    env_url_resolve_retry = os.getenv("DEALSCOUT_URL_RESOLVE_RETRY_ATTEMPTS")
    if env_url_resolve_retry is not None:
        config["url_resolve_retry_attempts"] = _normalize_retry(
            env_url_resolve_retry,
            config.get("url_resolve_retry_attempts", 2),
        )

    env_affiliate_enabled = os.getenv("DEALSCOUT_ENABLE_AFFILIATE")
    if env_affiliate_enabled is not None:
        config["affiliate_enabled"] = _normalize_bool(
            env_affiliate_enabled,
            config.get("affiliate_enabled", False),
        )

    env_affiliate_api_url = os.getenv("DEALSCOUT_AFFILIATE_API_URL")
    if env_affiliate_api_url is not None:
        config["affiliate_api_url"] = env_affiliate_api_url.strip()

    env_affiliate_api_token = os.getenv("DEALSCOUT_AFFILIATE_API_TOKEN")
    if env_affiliate_api_token is not None:
        config["affiliate_api_token"] = env_affiliate_api_token.strip()

    env_affiliate_timeout = os.getenv("DEALSCOUT_AFFILIATE_TIMEOUT_SECONDS")
    if env_affiliate_timeout is not None:
        config["affiliate_timeout_seconds"] = _normalize_timeout(
            env_affiliate_timeout,
            config.get("affiliate_timeout_seconds", 5.0),
        )

    env_affiliate_retry_attempts = os.getenv(
        "DEALSCOUT_AFFILIATE_RETRY_ATTEMPTS"
    )
    if env_affiliate_retry_attempts is not None:
        config["affiliate_retry_attempts"] = _normalize_retry(
            env_affiliate_retry_attempts,
            config.get("affiliate_retry_attempts", 2),
        )

    auto_restart = _normalize_bool(
        os.getenv("DEALSCOUT_AUTO_RESTART"),
        True,
    )
    max_restarts_raw = os.getenv("DEALSCOUT_MAX_RESTARTS")
    try:
        max_restarts = int(max_restarts_raw) if max_restarts_raw is not None else 0
    except ValueError:
        max_restarts = 0

    base_delay = _normalize_retry(os.getenv("DEALSCOUT_RESTART_DELAY_SECONDS"), 1)
    max_delay = _normalize_retry(os.getenv("DEALSCOUT_MAX_RESTART_DELAY_SECONDS"), 300)

    attempt = 0
    while True:
        try:
            attempt += 1
            logger.info("Starting listener (attempt=%s)", attempt)
            asyncio.run(
                start_listener(api_id, api_hash, phone, session_name, config)
            )

            # If start_listener returns normally, only restart if auto_restart
            if not auto_restart:
                logger.info("Listener exited normally and auto-restart is disabled")
                break

            if max_restarts and attempt >= max_restarts:
                logger.error(
                    "Max restart attempts reached (%s); not restarting",
                    max_restarts,
                )
                break

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            logger.warning(
                "Listener exited; will restart after %s seconds (attempt=%s)",
                delay,
                attempt + 1,
            )
            time.sleep(delay)

        except FatalListenerError as exc:
            logger.error("Listener cannot start: %s", exc)
            break
        except KeyboardInterrupt:
            logger.info("Listener stopped by user")
            break
        except Exception:
            logger.exception("Listener crashed unexpectedly")
            if not auto_restart:
                break

            if max_restarts and attempt >= max_restarts:
                logger.error(
                    "Max restart attempts reached (%s) after crash; giving up",
                    max_restarts,
                )
                break

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            logger.info("Restarting listener after %s seconds", delay)
            time.sleep(delay)


if __name__ == "__main__":
    main()

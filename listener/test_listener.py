import logging
import csv
import base64
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from message_parser import parse_structured_message

from listener import (
    CSV_FIELDNAMES,
    ConfigFileChangeHandler,
    RuntimeConfig,
    archive_message_to_csv,
    build_archive_record,
    extract_image_base64,
    extract_urls,
    load_config,
    message_contains_url,
    log_visible_monitored_channels,
    process_monitored_message,
    start_listener,
    send_webhook,
)


@pytest.mark.parametrize(
    "message,expected",
    [
        ("http://example.com/deal", True),
        ("https://example.com/deal", True),
        ("www.example.com", True),
        ("Best deal at domain.DEALS now", True),
        ("visit offer.io for more", True),
        ("nothing to see here", False),
        ("", False),
        (None, False),
    ],
)
def test_message_contains_url(message, expected):
    assert message_contains_url(message) is expected


def test_load_config_json(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(
        '{"webhook_url":"http://localhost/webhook","retry_attempts":4,'
        '"retry_delay_seconds":2,"channels":[{"id":-1001,"name":"Deals"}]}',
        encoding="utf-8",
    )

    config = load_config(str(path))

    assert config["webhook_url"] == "http://localhost/webhook"
    assert config["retry_attempts"] == 4
    assert config["retry_delay_seconds"] == 2
    assert config["channels"][0]["id"] == -1001


def test_load_config_yaml(tmp_path: Path):
    path = tmp_path / "channels.yaml"
    path.write_text(
        """
webhook_url: http://localhost/webhook
channels:
  - id: -1002
    name: Deals YAML
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(path))

    assert config["webhook_url"] == "http://localhost/webhook"
    assert config["retry_attempts"] == 3
    assert config["retry_delay_seconds"] == 5
    assert config["channels"][0]["id"] == -1002


def test_load_config_defaults_and_missing_channels(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(
        '{"webhook_url":"http://localhost/webhook"}',
        encoding="utf-8",
    )

    config = load_config(str(path))

    assert config["channels"] == []
    assert config["retry_attempts"] == 3
    assert config["retry_delay_seconds"] == 5
    assert config["webhook_enabled"] is False


def test_load_config_missing_channel_id(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(
        (
            '{"webhook_url":"http://localhost/webhook",'
            '"channels":[{"name":"Deals"}]}'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing 'id'"):
        load_config(str(path))


def test_load_config_bad_json(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text('{"webhook_url":', encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid JSON"):
        load_config(str(path))


def test_load_config_missing_file(tmp_path: Path):
    config_path = tmp_path / "missing.json"

    config = load_config(str(config_path))

    assert config["channels"]
    assert config_path.exists()


def test_can_use_interactive_auth_with_tty_access():
    with patch("listener.sys.stdin.isatty", return_value=False):
        with patch("listener.sys.stdout.isatty", return_value=False):
            with patch("builtins.open", create=True) as open_mock:
                open_mock.return_value.__enter__.return_value = MagicMock()
                from listener import _can_use_interactive_auth

                assert _can_use_interactive_auth() is True


def test_runtime_config_updates_snapshot():
    runtime = RuntimeConfig(
        {
            "webhook_url": "http://localhost/webhook",
            "retry_attempts": 3,
            "retry_delay_seconds": 1,
            "webhook_enabled": False,
            "channels": [{"id": -1001}],
        }
    )

    runtime.update(
        {
            "webhook_url": "http://localhost/updated",
            "retry_attempts": 5,
            "retry_delay_seconds": 2,
            "webhook_enabled": True,
            "channels": [{"id": -2002}],
        }
    )
    snapshot = runtime.snapshot()

    assert snapshot["webhook_url"] == "http://localhost/updated"
    assert snapshot["retry_attempts"] == 5
    assert snapshot["retry_delay_seconds"] == 2
    assert snapshot["channel_ids"] == {-2002}
    assert snapshot["webhook_enabled"] is True


@pytest.mark.asyncio
async def test_log_visible_monitored_channels_reports_visible_and_missing(
    caplog,
):
    monitored_ids = {-1001, -2002}
    dialogs = [
        SimpleNamespace(entity=SimpleNamespace(title="Deals A")),
        SimpleNamespace(entity=SimpleNamespace(title="Deals B")),
    ]
    client = MagicMock()
    client.get_dialogs = AsyncMock(return_value=dialogs)

    with patch("listener.utils.get_peer_id", side_effect=[-1001, -3003]):
        with caplog.at_level(logging.INFO):
            await log_visible_monitored_channels(
                client=client,
                monitored_channel_ids=monitored_ids,
                logger=logging.getLogger("test"),
            )

    assert (
        "Verbose startup: monitored chats visible in this session"
        in caplog.text
    )
    assert "-1001 (Deals A)" in caplog.text
    assert (
        "Verbose startup: monitored chat ids not visible in this session: "
        "[-2002]"
        in caplog.text
    )


def test_extract_urls_returns_all_matches():
    message = (
        "Deal at https://example.com/a and also www.test.com "
        "plus offer.io"
    )

    assert extract_urls(message) == [
        "https://example.com/a",
        "www.test.com",
        "offer.io",
    ]


def test_parse_structured_message_extracts_primary_fields():
    message = (
        "🔥 Fone Bluetooth JBL Tune\n"
        "De R$ 299,90 por R$ 199,90\n"
        "Use cupom: SOM10\n"
        "https://amzn.to/abcd123?tag=aff"
    )

    parsed = parse_structured_message(message)

    assert parsed["product_url"] == "https://amzn.to/abcd123?tag=aff"
    assert parsed["product_domain"] == "amzn.to"
    assert parsed["is_affiliate_url"] is True
    assert parsed["product_price"] == 199.9
    assert parsed["original_price"] == 299.9
    assert parsed["price_currency"] == "BRL"
    assert parsed["coupon_code"] == "SOM10"
    assert "cupom" in (parsed["coupon_text"] or "").lower()
    assert parsed["product_description"] == "🔥 Fone Bluetooth JBL Tune"
    assert parsed["parse_status"] == "ok"
    assert parsed["schema_version"] == "v2"


def test_build_archive_record_includes_message_and_entity_metadata():
    event = SimpleNamespace(
        chat_id=-1001,
        sender_id=777,
        id=42,
        date=SimpleNamespace(isoformat=lambda: "2026-05-24T12:00:00+00:00"),
    )
    chat = SimpleNamespace(title="Deals Channel", username="dealschannel")
    sender = SimpleNamespace(
        first_name="Ana",
        last_name="Silva",
        username="ana",
    )

    record = build_archive_record(
        event=event,
        message_text="Promo at https://example.com/deal",
        contains_url=True,
        webhook_status="sent",
        image_base64=None,
        has_image=False,
        chat=chat,
        sender=sender,
    )

    assert record["source_channel_title"] == "Deals Channel"
    assert record["source_channel_username"] == "dealschannel"
    assert record["source_channel_type"] == "SimpleNamespace"
    assert record["sender_id"] == 777
    assert record["sender_name"] == "Ana Silva"
    assert record["sender_username"] == "ana"
    assert record["message_length"] == len("Promo at https://example.com/deal")
    assert record["schema_version"] == "v2"
    assert record["normalized_message"] == "Promo at https://example.com/deal"
    assert record["contains_url"] is True
    assert record["product_url"] == "https://example.com/deal"
    assert record["product_domain"] == "example.com"
    assert record["extracted_urls"] == "https://example.com/deal"
    assert record["all_urls"] == "https://example.com/deal"
    assert record["url_count"] == 1
    assert record["has_image"] is False
    assert record["image_base64"] is None
    assert record["webhook_status"] == "sent"


@pytest.mark.asyncio
async def test_extract_image_base64_returns_encoded_bytes_for_photo_message():
    message = SimpleNamespace(photo=SimpleNamespace())
    event = SimpleNamespace(message=message, photo=message.photo)
    client = MagicMock()
    client.download_media = AsyncMock(return_value=b"fake-image-bytes")

    encoded, has_image = await extract_image_base64(client, event)

    assert has_image is True
    assert encoded == base64.b64encode(b"fake-image-bytes").decode("ascii")


@pytest.mark.asyncio
async def test_extract_image_base64_returns_none_when_no_photo():
    event = SimpleNamespace(message=SimpleNamespace(photo=None), photo=None)
    client = MagicMock()
    client.download_media = AsyncMock()

    encoded, has_image = await extract_image_base64(client, event)

    assert has_image is False
    assert encoded is None


@pytest.mark.asyncio
async def test_process_monitored_message_with_fake_telegram_event(
    tmp_path: Path,
    caplog,
):
    archive_path = tmp_path / "archive" / "messages.csv"
    runtime_config = {
        "webhook_url": "http://localhost/webhook",
        "retry_attempts": 3,
        "retry_delay_seconds": 1,
        "webhook_enabled": True,
    }
    chat = SimpleNamespace(title="Deals Channel", username="dealschannel")
    sender = SimpleNamespace(
        first_name="Ana",
        last_name="Silva",
        username="ana",
    )
    event = SimpleNamespace(
        chat_id=-1001,
        sender_id=777,
        id=42,
        raw_text="Promo https://example.com/deal",
        date=SimpleNamespace(isoformat=lambda: "2026-05-24T12:00:00+00:00"),
        message=SimpleNamespace(photo=SimpleNamespace()),
        photo=SimpleNamespace(),
        get_chat=AsyncMock(return_value=chat),
        get_sender=AsyncMock(return_value=sender),
    )
    client = MagicMock()
    client.download_media = AsyncMock(return_value=b"fake-image-bytes")
    http_session = MagicMock()

    with patch(
        "listener.send_webhook",
        new=AsyncMock(return_value=True),
    ) as webhook_mock:
        with caplog.at_level(logging.INFO):
            archive_record = await process_monitored_message(
                event=event,
                client=client,
                runtime_config=runtime_config,
                http_session=http_session,
                archive_path=archive_path,
                logger=logging.getLogger("test"),
            )

    assert webhook_mock.await_count == 1
    sent_payload = webhook_mock.await_args.kwargs["payload"]
    assert sent_payload["message_id"] == 42
    assert sent_payload["product_url"] == "https://example.com/deal"
    assert sent_payload["structured_data"]["schema_version"] == "v2"
    assert archive_path.exists()

    with archive_path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert rows[0]["message_id"] == "42"
    assert rows[0]["webhook_status"] == "sent"
    assert rows[0]["schema_version"] == "v2"
    assert rows[0]["product_url"] == "https://example.com/deal"
    assert rows[0]["product_domain"] == "example.com"
    assert rows[0]["has_image"] == "True"
    assert rows[0]["image_base64"] == base64.b64encode(
        b"fake-image-bytes"
    ).decode("ascii")
    assert archive_record["webhook_status"] == "sent"
    assert archive_record["url_count"] == 1
    assert "New message event received" in caplog.text
    assert "URL detected for message_id=42; sending webhook" in caplog.text
    assert "Message archived successfully" in caplog.text


@pytest.mark.asyncio
async def test_process_monitored_message_skips_webhook_when_disabled(
    tmp_path: Path,
    caplog,
):
    archive_path = tmp_path / "archive" / "messages.csv"
    runtime_config = {
        "webhook_url": "http://localhost/webhook",
        "retry_attempts": 3,
        "retry_delay_seconds": 1,
        "webhook_enabled": False,
    }
    chat = SimpleNamespace(title="Deals Channel", username="dealschannel")
    sender = SimpleNamespace(
        first_name="Ana",
        last_name="Silva",
        username="ana",
    )
    event = SimpleNamespace(
        chat_id=-1001,
        sender_id=777,
        id=44,
        raw_text="Promo https://example.com/deal",
        date=SimpleNamespace(isoformat=lambda: "2026-05-24T12:10:00+00:00"),
        message=SimpleNamespace(photo=None),
        photo=None,
        get_chat=AsyncMock(return_value=chat),
        get_sender=AsyncMock(return_value=sender),
    )
    client = MagicMock()
    client.download_media = AsyncMock()
    http_session = MagicMock()

    with patch("listener.send_webhook", new=AsyncMock()) as webhook_mock:
        with caplog.at_level(logging.INFO):
            archive_record = await process_monitored_message(
                event=event,
                client=client,
                runtime_config=runtime_config,
                http_session=http_session,
                archive_path=archive_path,
                logger=logging.getLogger("test"),
            )

    assert webhook_mock.await_count == 0
    assert archive_record["webhook_status"] == "skipped_webhook_disabled"
    assert "webhook is disabled" in caplog.text


@pytest.mark.asyncio
async def test_start_listener_ignores_non_monitored_channel(
    tmp_path: Path,
    caplog,
):
    archive_path = tmp_path / "archive" / "messages.csv"
    runtime_config = {
        "webhook_url": "http://localhost/webhook",
        "retry_attempts": 3,
        "retry_delay_seconds": 1,
        "webhook_enabled": False,
        "channels": [{"id": -2002}],
    }
    event = SimpleNamespace(
        chat_id=-1001,
        sender_id=777,
        id=43,
        raw_text="Promo https://example.com/deal",
        date=SimpleNamespace(
            isoformat=lambda: "2026-05-24T12:05:00+00:00"
        ),
        get_chat=AsyncMock(side_effect=AssertionError("should not load chat")),
        get_sender=AsyncMock(
            side_effect=AssertionError("should not load sender")
        ),
    )
    client = MagicMock()
    client.connect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)
    client.get_me = AsyncMock(
        return_value=SimpleNamespace(id=123, username="listener", phone="55")
    )
    client.on = MagicMock()
    client.run_until_disconnected = AsyncMock()
    http_session = MagicMock()

    captured_handler = {}

    def on_new_message(_event_filter):
        def decorator(handler):
            captured_handler["handler"] = handler
            return handler

        return decorator

    client.on.side_effect = on_new_message

    async def run_until_disconnected():
        await captured_handler["handler"](event)

    client.run_until_disconnected.side_effect = run_until_disconnected

    http_session_context = MagicMock()
    http_session_context.__aenter__ = AsyncMock(return_value=http_session)
    http_session_context.__aexit__ = AsyncMock(return_value=None)
    config_path = tmp_path / "channels.json"
    config_path.write_text(
        '{"webhook_enabled":false,"channels":[{"id":-2002}]}',
        encoding="utf-8",
    )

    with patch.dict(os.environ, {"DEALSCOUT_CONFIG": str(config_path)}):
        with patch("listener.TelegramClient", return_value=client):
            with patch(
                "listener.aiohttp.ClientSession",
                return_value=http_session_context,
            ):
                with patch(
                    "listener.process_monitored_message",
                    new=AsyncMock(),
                ) as process_mock:
                    with caplog.at_level(logging.INFO):
                        await start_listener(
                            api_id=123,
                            api_hash="hash",
                            phone="+551199999999",
                            session_name="test-session",
                            config=runtime_config,
                            archive_csv_path=archive_path,
                        )

    assert process_mock.await_count == 0
    assert not archive_path.exists()
    assert "is not in the monitored channel list" in caplog.text


def test_archive_message_to_csv_appends_rows_and_keeps_header_once(
    tmp_path: Path,
):
    archive_path = tmp_path / "data" / "messages.csv"

    archive_message_to_csv(
        archive_path,
        {
            "schema_version": "v2",
            "archived_at": "2026-05-24T12:00:00+00:00",
            "processed_at": "2026-05-24T12:00:00+00:00",
            "source_channel_id": -1001,
            "source_channel_title": "Deals Channel",
            "source_channel_username": "dealschannel",
            "source_channel_type": "SimpleNamespace",
            "sender_id": 777,
            "sender_name": "Ana Silva",
            "sender_username": "ana",
            "message_id": 10,
            "message_date": "2026-05-24T11:59:00+00:00",
            "message_length": 16,
            "message_text": "Deal, with comma",
            "normalized_message": "Deal, with comma",
            "contains_url": True,
            "product_url": "https://example.com/deal",
            "product_domain": "example.com",
            "is_affiliate_url": False,
            "product_price": 199.9,
            "product_price_raw": "R$ 199,90",
            "original_price": 299.9,
            "original_price_raw": "R$ 299,90",
            "price_currency": "BRL",
            "coupon_code": "SAVE10",
            "coupon_text": "Use cupom SAVE10",
            "product_description": "Mouse Gamer RGB",
            "parse_status": "ok",
            "parse_confidence": 0.95,
            "extracted_urls": "https://example.com/deal",
            "all_urls": "https://example.com/deal",
            "url_count": 1,
            "has_image": True,
            "image_base64": "ZmFrZS1pbWFnZS1ieXRlcw==",
            "webhook_status": "sent",
        },
    )
    archive_message_to_csv(
        archive_path,
        {
            "schema_version": "v2",
            "archived_at": "2026-05-24T12:01:00+00:00",
            "processed_at": "2026-05-24T12:01:00+00:00",
            "source_channel_id": -1002,
            "source_channel_title": "Deals Channel 2",
            "source_channel_username": "deals2",
            "source_channel_type": "SimpleNamespace",
            "sender_id": 778,
            "sender_name": "Bruno Silva",
            "sender_username": "bruno",
            "message_id": 11,
            "message_date": "2026-05-24T12:00:30+00:00",
            "message_length": 13,
            "message_text": "No link here",
            "normalized_message": "No link here",
            "contains_url": False,
            "product_url": None,
            "product_domain": None,
            "is_affiliate_url": False,
            "product_price": None,
            "product_price_raw": None,
            "original_price": None,
            "original_price_raw": None,
            "price_currency": None,
            "coupon_code": None,
            "coupon_text": None,
            "product_description": "No link here",
            "parse_status": "partial_no_url",
            "parse_confidence": 0.3,
            "extracted_urls": "",
            "all_urls": "",
            "url_count": 0,
            "has_image": False,
            "image_base64": None,
            "webhook_status": "skipped_no_url",
        },
    )

    with archive_path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    assert archive_path.exists()
    assert len(rows) == 2
    assert rows[0]["schema_version"] == "v2"
    assert rows[0]["message_text"] == "Deal, with comma"
    assert rows[0]["source_channel_title"] == "Deals Channel"
    assert rows[0]["product_price"] == "199.9"
    assert rows[0]["webhook_status"] == "sent"
    assert rows[1]["contains_url"] == "False"
    assert rows[1]["webhook_status"] == "skipped_no_url"


def test_archive_message_to_csv_creates_missing_file_and_parent_directory(
    tmp_path: Path,
):
    archive_path = tmp_path / "nested" / "archive" / "messages.csv"

    archive_message_to_csv(
        archive_path,
        {
            "schema_version": "v2",
            "archived_at": "2026-05-24T12:30:00+00:00",
            "processed_at": "2026-05-24T12:30:00+00:00",
            "source_channel_id": -1003,
            "source_channel_title": "Deals Channel 3",
            "source_channel_username": "deals3",
            "source_channel_type": "SimpleNamespace",
            "sender_id": 779,
            "sender_name": "Carla Lima",
            "sender_username": "carla",
            "message_id": 12,
            "message_date": "2026-05-24T12:29:30+00:00",
            "message_length": 20,
            "message_text": "Fresh deal message",
            "normalized_message": "Fresh deal message",
            "contains_url": False,
            "product_url": None,
            "product_domain": None,
            "is_affiliate_url": False,
            "product_price": None,
            "product_price_raw": None,
            "original_price": None,
            "original_price_raw": None,
            "price_currency": None,
            "coupon_code": None,
            "coupon_text": None,
            "product_description": "Fresh deal message",
            "parse_status": "partial_no_url",
            "parse_confidence": 0.3,
            "extracted_urls": "",
            "all_urls": "",
            "url_count": 0,
            "has_image": False,
            "image_base64": None,
            "webhook_status": "skipped_no_url",
        },
    )

    assert archive_path.exists()
    assert archive_path.parent.exists()

    with archive_path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert set(rows[0].keys()) == set(CSV_FIELDNAMES)
    assert rows[0]["message_text"] == "Fresh deal message"


def test_archive_message_to_csv_keeps_legacy_header_when_file_exists(
    tmp_path: Path,
):
    archive_path = tmp_path / "legacy" / "messages.csv"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(
        "message_id,message_text,webhook_status\n"
        "1,legacy row,sent\n",
        encoding="utf-8",
    )

    archive_message_to_csv(
        archive_path,
        {
            "message_id": 2,
            "message_text": "new row",
            "webhook_status": "skipped_no_url",
            "product_url": "https://example.com/deal",
            "schema_version": "v2",
        },
    )

    with archive_path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 2
    assert list(rows[1].keys()) == ["message_id", "message_text", "webhook_status"]
    assert rows[1]["message_id"] == "2"
    assert rows[1]["message_text"] == "new row"
    assert rows[1]["webhook_status"] == "skipped_no_url"


def test_config_file_change_handler_only_calls_callback_for_target_file(
    tmp_path: Path,
):
    config_path = (tmp_path / "channels.json").resolve()
    callback = MagicMock()
    handler = ConfigFileChangeHandler(
        config_path,
        callback,
        logging.getLogger("test"),
    )

    handler.on_modified(
        SimpleNamespace(
            is_directory=False,
            src_path=str(tmp_path / "other.json"),
        )
    )
    callback.assert_not_called()

    handler.on_modified(
        SimpleNamespace(is_directory=False, src_path=str(config_path))
    )
    callback.assert_called_once()


def test_config_file_change_handler_handles_move_events(tmp_path: Path):
    config_path = (tmp_path / "channels.json").resolve()
    callback = MagicMock()
    handler = ConfigFileChangeHandler(
        config_path,
        callback,
        logging.getLogger("test"),
    )

    handler.on_moved(
        SimpleNamespace(
            is_directory=False,
            src_path=str(tmp_path / "temp.json"),
            dest_path=str(config_path),
        )
    )

    callback.assert_called_once()


def _response_context(status: int):
    response = MagicMock()
    response.status = status
    response.text = AsyncMock(return_value="response")

    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=response)
    context_manager.__aexit__ = AsyncMock(return_value=None)
    return context_manager


@pytest.mark.asyncio
async def test_send_webhook_success():
    session = MagicMock()
    session.post.return_value = _response_context(200)

    success = await send_webhook(
        session=session,
        webhook_url="http://localhost/webhook",
        payload={"message_id": 1},
        retry_attempts=3,
        retry_delay_seconds=1,
        logger=logging.getLogger("test"),
    )

    assert success is True
    assert session.post.call_count == 1


@pytest.mark.asyncio
async def test_send_webhook_retry_then_success():
    session = MagicMock()
    session.post.side_effect = [_response_context(500), _response_context(201)]

    with patch("listener.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        success = await send_webhook(
            session=session,
            webhook_url="http://localhost/webhook",
            payload={"message_id": 2},
            retry_attempts=3,
            retry_delay_seconds=1,
            logger=logging.getLogger("test"),
        )

    assert success is True
    assert session.post.call_count == 2
    sleep_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_webhook_all_retries_exhausted():
    session = MagicMock()
    session.post.side_effect = [
        _response_context(500),
        _response_context(500),
        _response_context(500),
    ]

    with patch("listener.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        success = await send_webhook(
            session=session,
            webhook_url="http://localhost/webhook",
            payload={"message_id": 3},
            retry_attempts=3,
            retry_delay_seconds=1,
            logger=logging.getLogger("test"),
        )

    assert success is False
    assert session.post.call_count == 3
    assert sleep_mock.await_count == 2


@pytest.mark.asyncio
async def test_send_webhook_network_errors():
    session = MagicMock()
    session.post.side_effect = aiohttp.ClientError("network down")

    with patch("listener.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        success = await send_webhook(
            session=session,
            webhook_url="http://localhost/webhook",
            payload={"message_id": 4},
            retry_attempts=3,
            retry_delay_seconds=1,
            logger=logging.getLogger("test"),
        )

    assert success is False
    assert session.post.call_count == 3
    assert sleep_mock.await_count == 2

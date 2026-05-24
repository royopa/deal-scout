import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from listener import load_config, message_contains_url, send_webhook


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
    path.write_text('{"webhook_url":"http://localhost/webhook"}', encoding="utf-8")

    config = load_config(str(path))

    assert config["channels"] == []
    assert config["retry_attempts"] == 3
    assert config["retry_delay_seconds"] == 5


def test_load_config_missing_channel_id(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(
        '{"webhook_url":"http://localhost/webhook","channels":[{"name":"Deals"}]}',
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
    with pytest.raises(FileNotFoundError):
        load_config(str(tmp_path / "missing.json"))


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
    session.post.side_effect = [_response_context(500), _response_context(500), _response_context(500)]

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

"""Tests for the Xiaohongshu channel (auto-reply via xiaohongshu-mcp HTTP API)."""

from __future__ import annotations

import asyncio
import json
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nanobot.bus.queue import MessageBus
from nanobot.channels.xiaohongshu import (
    XHS_MAX_MESSAGE_LEN,
    XiaohongshuChannel,
    XiaohongshuConfig,
    _ReplyContext,
)
from nanobot.cli.commands import _onboard_plugins


def _make_channel(**overrides) -> tuple[XiaohongshuChannel, MessageBus]:
    base = dict(
        enabled=True,
        allow_from=["*"],
        state_dir=tempfile.mkdtemp(prefix="nanobot-xhs-test-"),
        poll_interval_s=1,
    )
    base.update(overrides)
    bus = MessageBus()
    return XiaohongshuChannel(XiaohongshuConfig(**base), bus), bus


def test_onboard_plugins_seeds_xiaohongshu_channel_and_mcp_server(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({}))

    _onboard_plugins(config_path)

    saved = json.loads(config_path.read_text())
    # Channel block is seeded.
    assert "xiaohongshu" in saved["channels"]
    assert saved["channels"]["xiaohongshu"]["enabled"] is False
    # MCP server block is also seeded, pointing at the same backend.
    mcp = saved["tools"]["mcpServers"]["xiaohongshu"]
    assert mcp["url"] == "http://localhost:18060/mcp"
    assert mcp["enabled"] is False


def test_onboard_plugins_preserves_user_edits_to_mcp_entry(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    # User has already edited the MCP entry: enabled + different URL.
    config_path.write_text(
        json.dumps(
            {
                "tools": {
                    "mcpServers": {
                        "xiaohongshu": {
                            "enabled": True,
                            "url": "http://192.168.1.10:18060/mcp",
                        }
                    }
                }
            }
        )
    )

    _onboard_plugins(config_path)

    saved = json.loads(config_path.read_text())
    mcp = saved["tools"]["mcpServers"]["xiaohongshu"]
    # Existing user values are preserved …
    assert mcp["enabled"] is True
    assert mcp["url"] == "http://192.168.1.10:18060/mcp"
    # … and missing defaults are filled in.
    assert mcp["type"] == "streamableHttp"
    assert mcp["enabledTools"] == ["*"]


def test_default_mcp_servers_points_at_backend_mcp_endpoint() -> None:
    servers = XiaohongshuChannel.default_mcp_servers()
    assert "xiaohongshu" in servers
    entry = servers["xiaohongshu"]
    # Must align with the channel's default base_url so the same running
    # xiaohongshu-mcp process serves both REST and MCP.
    assert entry["url"] == "http://localhost:18060/mcp"
    assert entry["type"] == "streamableHttp"
    # Disabled by default so an unreachable backend doesn't error on gateway start.
    assert entry["enabled"] is False


def test_default_config_round_trips_through_dict() -> None:
    cfg = XiaohongshuChannel.default_config()
    assert cfg["enabled"] is False
    assert cfg["mode"] == "messages"
    # Plugin-style instantiation: dict in, validated config out.
    bus = MessageBus()
    channel = XiaohongshuChannel({**cfg, "enabled": True, "allowFrom": ["*"]}, bus)
    assert isinstance(channel.config, XiaohongshuConfig)
    assert channel.config.allow_from == ["*"]


def test_url_helper_strips_double_slashes() -> None:
    channel, _ = _make_channel(base_url="http://localhost:18060/")
    assert channel._url("/api/v1/messages/list") == "http://localhost:18060/api/v1/messages/list"
    assert channel._url("api/v1/messages/list") == "http://localhost:18060/api/v1/messages/list"


def test_payload_unwraps_data_envelope() -> None:
    assert XiaohongshuChannel._payload({"data": {"messages": []}}) == {"messages": []}
    assert XiaohongshuChannel._payload({"messages": []}) == {"messages": []}
    # Non-dict envelope falls back to raw resp (defensive against odd backends).
    assert XiaohongshuChannel._payload({"data": None}) == {"data": None}


def test_remember_processed_dedupes_and_caps() -> None:
    channel, _ = _make_channel()
    assert channel._remember_processed("a") is True
    assert channel._remember_processed("a") is False
    assert channel._remember_processed("") is False


@pytest.mark.asyncio
async def test_poll_messages_forwards_only_new_inbound() -> None:
    channel, bus = _make_channel()
    channel._get = AsyncMock(
        return_value={
            "data": {
                "next_cursor": "cursor-1",
                "messages": [
                    {
                        "message_id": "m1",
                        "conversation_id": "c1",
                        "sender_id": "u1",
                        "sender_name": "Alice",
                        "content": "hi",
                    },
                    # Bot's own message — must be skipped.
                    {
                        "message_id": "m2",
                        "conversation_id": "c1",
                        "is_from_me": True,
                        "content": "ignored",
                    },
                ],
            }
        }
    )

    await channel._poll_messages()

    inbound = await asyncio.wait_for(bus.consume_inbound(), timeout=1.0)
    assert inbound.sender_id == "u1"
    assert inbound.chat_id == "c1"
    assert inbound.content == "hi"
    assert inbound.metadata["sender_name"] == "Alice"
    assert inbound.metadata["xhs_mode"] == "messages"
    assert channel._messages_cursor == "cursor-1"
    assert "c1" in channel._reply_ctx
    assert bus.inbound_size == 0

    # Re-polling the same message_id is a no-op.
    await channel._poll_messages()
    assert bus.inbound_size == 0


@pytest.mark.asyncio
async def test_send_routes_to_messages_endpoint_after_inbound() -> None:
    channel, _ = _make_channel()
    channel._client = object()  # only checked for None
    channel._get = AsyncMock(
        return_value={
            "data": {
                "messages": [
                    {
                        "message_id": "m1",
                        "conversation_id": "c-42",
                        "sender_id": "u1",
                        "content": "hello",
                    }
                ],
            }
        }
    )
    await channel._poll_messages()
    channel._send_message = AsyncMock()

    msg = SimpleNamespace(chat_id="c-42", content="thanks!", media=[], metadata={})
    await channel.send(msg)
    channel._send_message.assert_awaited_once_with("c-42", "thanks!")


@pytest.mark.asyncio
async def test_send_drops_progress_chunks() -> None:
    channel, _ = _make_channel()
    channel._client = object()
    channel._send_message = AsyncMock()

    await channel.send(
        SimpleNamespace(chat_id="c1", content="partial", media=[], metadata={"_progress": True})
    )
    channel._send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_splits_long_content_into_chunks() -> None:
    channel, _ = _make_channel()
    channel._client = object()
    channel._reply_ctx["c1"] = _ReplyContext(mode="messages", conversation_id="c1")
    channel._send_message = AsyncMock()

    long = "a" * (XHS_MAX_MESSAGE_LEN * 2 + 50)
    await channel.send(SimpleNamespace(chat_id="c1", content=long, media=[], metadata={}))

    assert channel._send_message.await_count >= 2


@pytest.mark.asyncio
async def test_send_skips_when_reply_context_missing_in_comments_mode() -> None:
    channel, _ = _make_channel(mode="comments")
    channel._client = object()
    channel._send_message = AsyncMock()
    channel._send_comment_reply = AsyncMock()

    # No reply ctx for "feed1:c1" → should not attempt either path.
    await channel.send(
        SimpleNamespace(chat_id="feed1:c1", content="hello", media=[], metadata={})
    )
    channel._send_message.assert_not_awaited()
    channel._send_comment_reply.assert_not_awaited()


@pytest.mark.asyncio
async def test_poll_comments_warm_start_does_not_forward_existing() -> None:
    channel, bus = _make_channel(mode="comments")
    channel._resolve_comment_targets = AsyncMock(
        return_value=[{"feed_id": "f1", "xsec_token": "tok"}]
    )
    channel._post = AsyncMock(
        return_value={
            "data": {
                "comments": [
                    {"comment_id": "c1", "user_id": "u1", "content": "old"},
                ]
            }
        }
    )

    await channel._poll_comments()
    assert bus.inbound_size == 0
    assert channel._comments_warmed is True
    assert "c1" in channel._processed_ids


@pytest.mark.asyncio
async def test_poll_comments_forwards_new_comment_after_warmup() -> None:
    channel, bus = _make_channel(mode="comments")
    channel._comments_warmed = True
    channel._resolve_comment_targets = AsyncMock(
        return_value=[{"feed_id": "f1", "xsec_token": "tok"}]
    )
    channel._post = AsyncMock(
        return_value={
            "data": {
                "comments": [
                    {
                        "comment_id": "c-new",
                        "user_id": "u-new",
                        "user_name": "Bob",
                        "content": "love it",
                    }
                ]
            }
        }
    )

    await channel._poll_comments()
    inbound = await asyncio.wait_for(bus.consume_inbound(), timeout=1.0)
    assert inbound.chat_id == "f1:c-new"
    assert inbound.metadata["xhs_mode"] == "comments"
    ctx = channel._reply_ctx["f1:c-new"]
    assert ctx.feed_id == "f1"
    assert ctx.comment_id == "c-new"
    assert ctx.xsec_token == "tok"


@pytest.mark.asyncio
async def test_send_in_comments_mode_routes_to_reply_endpoint() -> None:
    channel, _ = _make_channel(mode="comments")
    channel._client = object()
    channel._reply_ctx["f1:c1"] = _ReplyContext(
        mode="comments",
        feed_id="f1",
        xsec_token="tok",
        comment_id="c1",
        user_id="u1",
    )
    channel._send_comment_reply = AsyncMock()

    await channel.send(SimpleNamespace(chat_id="f1:c1", content="thx", media=[], metadata={}))
    channel._send_comment_reply.assert_awaited_once()
    args, _ = channel._send_comment_reply.call_args
    ctx, content = args
    assert ctx.feed_id == "f1"
    assert ctx.comment_id == "c1"
    assert content == "thx"


@pytest.mark.asyncio
async def test_send_message_raises_on_nonzero_code() -> None:
    channel, _ = _make_channel()
    channel._post = AsyncMock(return_value={"code": 1, "msg": "boom"})

    with pytest.raises(RuntimeError, match="code=1"):
        await channel._send_message("c1", "hello")


@pytest.mark.asyncio
async def test_state_persistence_round_trip(tmp_path) -> None:
    bus = MessageBus()
    channel = XiaohongshuChannel(
        XiaohongshuConfig(enabled=True, allow_from=["*"], state_dir=str(tmp_path)),
        bus,
    )
    channel._messages_cursor = "cursor-x"
    channel._processed_ids["m1"] = None
    channel._comments_warmed = True
    channel._save_state()

    saved = json.loads((tmp_path / "state.json").read_text())
    assert saved["messages_cursor"] == "cursor-x"
    assert "m1" in saved["processed_ids"]

    restored = XiaohongshuChannel(
        XiaohongshuConfig(enabled=True, allow_from=["*"], state_dir=str(tmp_path)),
        bus,
    )
    restored._load_state()
    assert restored._messages_cursor == "cursor-x"
    assert "m1" in restored._processed_ids
    assert restored._comments_warmed is True

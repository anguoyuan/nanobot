"""Xiaohongshu (小红书 / RedNote) channel — auto-reply for private messages and comments.

Bridges nanobot to Xiaohongshu via the ``xpzouying/xiaohongshu-mcp`` HTTP API
(https://github.com/xpzouying/xiaohongshu-mcp). Covers both directions:

* **Passive (auto-reply)** — this module polls the backend's REST endpoints and
  forwards inbound 私信 / comments to the agent, then ships the agent's replies
  back out.
* **Active (agent-initiated)** — :meth:`XiaohongshuChannel.default_mcp_servers`
  seeds ``tools.mcpServers.xiaohongshu`` during ``nanobot onboard`` so the same
  backend's MCP endpoint (``{base_url}/mcp``) is registered as a tool provider.
  The agent then gets ``publish_content`` / ``search_feeds`` /
  ``post_comment_to_feed`` / … alongside auto-reply.

Two operating modes (``mode`` config):

* ``messages`` *(default)* — polls ``GET /api/v1/messages/list`` for incoming
  private messages (私信) and replies via ``POST /api/v1/messages/send``. The
  vanilla ``xpzouying/xiaohongshu-mcp`` build does not yet ship 私信 endpoints,
  so this mode targets a fork/extension that adds them. The expected JSON
  contract is documented in :class:`XiaohongshuConfig`.

* ``comments`` — polls comments on the user's recent published feeds via the
  endpoints already shipped by upstream (``/api/v1/feeds/list`` +
  ``/api/v1/feeds/detail``) and replies via ``/api/v1/feeds/comment/reply``.
  This works with vanilla xiaohongshu-mcp and is a useful fallback.

The channel never blocks on login: if ``/api/v1/login/status`` reports the
backend is logged out, polling is paused (with a warning) until the user
completes login through the xiaohongshu-mcp UI.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx
from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_runtime_subdir
from nanobot.config.schema import Base
from nanobot.utils.helpers import split_message

XHS_MAX_MESSAGE_LEN = 1000  # Xiaohongshu private-message hard limit observed in the web client
DEFAULT_BASE_URL = "http://localhost:18060"
DEFAULT_POLL_INTERVAL_S = 15
DEFAULT_REQUEST_TIMEOUT_S = 30
LOGIN_PAUSE_S = 60  # back-off when backend reports logged-out state
PROCESSED_ID_CAP = 2000


class XiaohongshuConfig(Base):
    """Xiaohongshu channel configuration.

    The channel talks HTTP to a running ``xpzouying/xiaohongshu-mcp`` instance.
    Endpoint paths are configurable so forks / extensions that move them around
    can be supported without code changes.

    Expected ``messages`` mode contract (when implementing the upstream side):

    * ``GET {messages_list_path}?cursor=<opaque>`` →
      ``{"data": {"next_cursor": "...", "messages": [
            {"message_id": str, "conversation_id": str, "sender_id": str,
             "sender_name": str, "content": str, "timestamp": int,
             "is_from_me": bool, "media": [{"type": str, "url": str}]}
        ]}}``
    * ``POST {messages_send_path}`` body
      ``{"conversation_id": str, "content": str}`` → ``{"code": 0}``
    """

    enabled: bool = False
    allow_from: list[str] = Field(default_factory=list)

    base_url: str = DEFAULT_BASE_URL
    headers: dict[str, str] = Field(default_factory=dict)
    request_timeout_s: int = DEFAULT_REQUEST_TIMEOUT_S
    poll_interval_s: int = DEFAULT_POLL_INTERVAL_S
    state_dir: str = ""

    mode: Literal["messages", "comments"] = "messages"

    # --- messages mode ---
    messages_list_path: str = "/api/v1/messages/list"
    messages_send_path: str = "/api/v1/messages/send"

    # --- comments mode ---
    feed_ids: list[str] = Field(default_factory=list)  # explicit feeds; empty → discover via my profile
    comments_per_feed: int = 20
    feeds_list_path: str = "/api/v1/feeds/list"
    feed_detail_path: str = "/api/v1/feeds/detail"
    comment_reply_path: str = "/api/v1/feeds/comment/reply"
    my_profile_path: str = "/api/v1/user/me"

    # --- common ---
    login_status_path: str = "/api/v1/login/status"


@dataclass
class _ReplyContext:
    """Per-conversation reply routing info, captured on inbound and consumed by send()."""

    mode: Literal["messages", "comments"]
    conversation_id: str = ""  # messages mode
    feed_id: str = ""          # comments mode
    xsec_token: str = ""       # comments mode
    comment_id: str = ""       # comments mode
    user_id: str = ""          # comments mode


class XiaohongshuChannel(BaseChannel):
    """Xiaohongshu channel that polls xiaohongshu-mcp HTTP and auto-replies."""

    name = "xiaohongshu"
    display_name = "Xiaohongshu"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return XiaohongshuConfig().model_dump(by_alias=True)

    @classmethod
    def default_mcp_servers(cls) -> dict[str, dict[str, Any]]:
        """Seed ``tools.mcpServers.xiaohongshu`` so the agent can publish/search.

        The same ``xpzouying/xiaohongshu-mcp`` backend the channel polls over
        REST also exposes the MCP protocol at ``{base_url}/mcp``. Wiring it up
        here gives the agent active capabilities (``publish_content``,
        ``search_feeds``, ``post_comment_to_feed`` …) to complement the
        channel's passive auto-reply role.

        Disabled by default so an unreachable backend doesn't error on every
        ``nanobot gateway`` startup — flip ``enabled`` to ``true`` once
        xiaohongshu-mcp is running.
        """
        base_url = XiaohongshuConfig().base_url.rstrip("/")
        return {
            "xiaohongshu": {
                "enabled": False,
                "type": "streamableHttp",
                "url": f"{base_url}/mcp",
                "enabledTools": ["*"],
                "toolTimeout": 60,
            }
        }

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = XiaohongshuConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: XiaohongshuConfig = config

        self._client: httpx.AsyncClient | None = None
        self._state_dir: Path | None = None
        self._messages_cursor: str = ""
        self._processed_ids: OrderedDict[str, None] = OrderedDict()
        self._reply_ctx: dict[str, _ReplyContext] = {}
        self._login_pause_until: float = 0.0
        # Warm-start flag for comments mode: on the very first poll cycle we
        # record existing comment IDs without forwarding them, so the agent
        # doesn't get spammed by historical chatter on an established account.
        self._comments_warmed: bool = False

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _get_state_dir(self) -> Path:
        if self._state_dir is not None:
            return self._state_dir
        if self.config.state_dir:
            d = Path(self.config.state_dir).expanduser()
            d.mkdir(parents=True, exist_ok=True)
        else:
            d = get_runtime_subdir("xiaohongshu")
        self._state_dir = d
        return d

    def _state_file(self) -> Path:
        return self._get_state_dir() / "state.json"

    def _load_state(self) -> None:
        path = self._state_file()
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            logger.warning("Xiaohongshu: failed to read state file {}: {}", path, e)
            return
        cursor = data.get("messages_cursor", "")
        if isinstance(cursor, str):
            self._messages_cursor = cursor
        processed = data.get("processed_ids", [])
        if isinstance(processed, list):
            for mid in processed[-PROCESSED_ID_CAP:]:
                if isinstance(mid, str):
                    self._processed_ids[mid] = None
        self._comments_warmed = bool(data.get("comments_warmed", False))

    def _save_state(self) -> None:
        try:
            payload = {
                "messages_cursor": self._messages_cursor,
                "processed_ids": list(self._processed_ids.keys()),
                "comments_warmed": self._comments_warmed,
            }
            self._state_file().write_text(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.warning("Xiaohongshu: failed to persist state: {}", e)

    def _remember_processed(self, msg_id: str) -> bool:
        """Record *msg_id*; return True if it had not been seen before."""
        if not msg_id or msg_id in self._processed_ids:
            return False
        self._processed_ids[msg_id] = None
        while len(self._processed_ids) > PROCESSED_ID_CAP:
            self._processed_ids.popitem(last=False)
        return True

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _merged_headers(self) -> dict[str, str]:
        return {"Accept": "application/json", **(self.config.headers or {})}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        assert self._client is not None
        resp = await self._client.get(self._url(path), params=params, headers=self._merged_headers())
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        assert self._client is not None
        resp = await self._client.post(
            self._url(path),
            json=body or {},
            headers={"Content-Type": "application/json", **self._merged_headers()},
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _payload(resp: dict[str, Any]) -> dict[str, Any]:
        """Unwrap ``{"data": {...}}`` envelopes used by xiaohongshu-mcp."""
        data = resp.get("data") if isinstance(resp, dict) else None
        return data if isinstance(data, dict) else (resp or {})

    async def _is_logged_in(self) -> bool:
        try:
            resp = await self._get(self.config.login_status_path)
        except Exception as e:
            logger.warning("Xiaohongshu: login status check failed: {}", e)
            return False
        data = self._payload(resp)
        return bool(data.get("is_logged_in") or data.get("logged_in") or data.get("status") == "ok")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._load_state()
        timeout = httpx.Timeout(self.config.request_timeout_s, connect=10)
        self._client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)

        logger.info(
            "Xiaohongshu channel starting: base_url={} mode={} interval={}s",
            self.config.base_url,
            self.config.mode,
            self.config.poll_interval_s,
        )

        try:
            while self._running:
                if time.time() < self._login_pause_until:
                    await asyncio.sleep(min(self.config.poll_interval_s, 5))
                    continue

                if not await self._is_logged_in():
                    self._login_pause_until = time.time() + LOGIN_PAUSE_S
                    logger.warning(
                        "Xiaohongshu backend reports logged-out; pausing {}s. "
                        "Run xiaohongshu-mcp's login flow to authenticate.",
                        LOGIN_PAUSE_S,
                    )
                    continue

                try:
                    if self.config.mode == "messages":
                        await self._poll_messages()
                    else:
                        await self._poll_comments()
                except Exception as e:
                    logger.warning("Xiaohongshu poll cycle failed: {}", e)

                await asyncio.sleep(self.config.poll_interval_s)
        finally:
            self._running = False
            if self._client is not None:
                await self._client.aclose()
                self._client = None
            self._save_state()

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Messages mode (私信)
    # ------------------------------------------------------------------

    async def _poll_messages(self) -> None:
        params: dict[str, Any] = {}
        if self._messages_cursor:
            params["cursor"] = self._messages_cursor

        resp = await self._get(self.config.messages_list_path, params=params)
        data = self._payload(resp)

        messages = data.get("messages")
        if not isinstance(messages, list):
            return

        next_cursor = data.get("next_cursor")
        if isinstance(next_cursor, str) and next_cursor:
            self._messages_cursor = next_cursor

        forwarded = 0
        for raw in messages:
            if not isinstance(raw, dict):
                continue
            if raw.get("is_from_me"):
                continue
            msg_id = str(raw.get("message_id") or raw.get("id") or "")
            if not msg_id or not self._remember_processed(msg_id):
                continue

            sender_id = str(raw.get("sender_id") or raw.get("from_user_id") or "")
            conv_id = str(raw.get("conversation_id") or raw.get("conv_id") or sender_id)
            content = str(raw.get("content") or "").strip()
            sender_name = str(raw.get("sender_name") or "")

            media_urls: list[str] = []
            for item in raw.get("media") or []:
                if isinstance(item, dict):
                    url = str(item.get("url") or "").strip()
                    if url:
                        media_urls.append(url)

            display_content = content
            if not display_content and media_urls:
                display_content = "[private message attachment]"
            if not display_content:
                continue

            self._reply_ctx[conv_id] = _ReplyContext(mode="messages", conversation_id=conv_id)

            await self._handle_message(
                sender_id=sender_id or conv_id,
                chat_id=conv_id,
                content=display_content,
                media=media_urls or None,
                metadata={
                    "message_id": msg_id,
                    "sender_name": sender_name,
                    "xhs_mode": "messages",
                },
            )
            forwarded += 1

        if forwarded:
            self._save_state()

    async def _send_message(self, conversation_id: str, content: str) -> None:
        body = {"conversation_id": conversation_id, "content": content}
        resp = await self._post(self.config.messages_send_path, body)
        if isinstance(resp, dict):
            code = resp.get("code")
            if code is not None and code != 0:
                raise RuntimeError(
                    f"Xiaohongshu messages/send returned code={code} msg={resp.get('msg', '')}"
                )

    # ------------------------------------------------------------------
    # Comments mode
    # ------------------------------------------------------------------

    async def _poll_comments(self) -> None:
        targets = await self._resolve_comment_targets()
        if not targets:
            return

        new_inbound: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for feed in targets:
            feed_id = str(feed.get("feed_id") or feed.get("id") or "")
            xsec = str(feed.get("xsec_token") or feed.get("xsecToken") or "")
            if not feed_id or not xsec:
                continue
            try:
                detail = await self._post(
                    self.config.feed_detail_path,
                    {"feed_id": feed_id, "xsec_token": xsec},
                )
            except Exception as e:
                logger.debug("Xiaohongshu: feed detail failed for {}: {}", feed_id, e)
                continue

            payload = self._payload(detail)
            comments = payload.get("comments") or payload.get("comment_list") or []
            if not isinstance(comments, list):
                continue

            for raw in comments[: self.config.comments_per_feed]:
                if not isinstance(raw, dict):
                    continue
                comment_id = str(raw.get("comment_id") or raw.get("id") or "")
                if not comment_id:
                    continue
                # Warm-start: record existing comments without forwarding them.
                if not self._comments_warmed:
                    self._processed_ids[comment_id] = None
                    continue
                if not self._remember_processed(comment_id):
                    continue
                new_inbound.append((raw, {"feed_id": feed_id, "xsec_token": xsec}))

        if not self._comments_warmed:
            self._comments_warmed = True
            self._save_state()
            return

        for raw, feed_ctx in new_inbound:
            user_id = str(raw.get("user_id") or raw.get("author_id") or "")
            sender_name = str(raw.get("user_name") or raw.get("author_name") or "")
            content = str(raw.get("content") or "").strip()
            comment_id = str(raw.get("comment_id") or raw.get("id") or "")
            chat_id = f"{feed_ctx['feed_id']}:{comment_id}"

            if not content:
                continue

            self._reply_ctx[chat_id] = _ReplyContext(
                mode="comments",
                feed_id=feed_ctx["feed_id"],
                xsec_token=feed_ctx["xsec_token"],
                comment_id=comment_id,
                user_id=user_id,
            )

            await self._handle_message(
                sender_id=user_id or chat_id,
                chat_id=chat_id,
                content=content,
                metadata={
                    "comment_id": comment_id,
                    "sender_name": sender_name,
                    "feed_id": feed_ctx["feed_id"],
                    "xhs_mode": "comments",
                },
            )

        if new_inbound:
            self._save_state()

    async def _resolve_comment_targets(self) -> list[dict[str, Any]]:
        """Resolve the list of feeds to monitor for new comments."""
        if self.config.feed_ids:
            # Explicit feed list — caller must pre-supply xsec tokens via the
            # ``feed_ids`` config as ``"<feed_id>:<xsec_token>"`` pairs.
            out: list[dict[str, Any]] = []
            for entry in self.config.feed_ids:
                if not isinstance(entry, str) or ":" not in entry:
                    continue
                feed_id, xsec = entry.split(":", 1)
                out.append({"feed_id": feed_id, "xsec_token": xsec})
            return out

        try:
            resp = await self._get(self.config.feeds_list_path)
        except Exception as e:
            logger.warning("Xiaohongshu: feeds/list failed: {}", e)
            return []

        payload = self._payload(resp)
        feeds = payload.get("feeds") or payload.get("items") or []
        return feeds if isinstance(feeds, list) else []

    async def _send_comment_reply(self, ctx: _ReplyContext, content: str) -> None:
        body = {
            "feed_id": ctx.feed_id,
            "xsec_token": ctx.xsec_token,
            "comment_id": ctx.comment_id,
            "user_id": ctx.user_id,
            "content": content,
        }
        resp = await self._post(self.config.comment_reply_path, body)
        if isinstance(resp, dict):
            code = resp.get("code")
            if code is not None and code != 0:
                raise RuntimeError(
                    f"Xiaohongshu comment/reply returned code={code} msg={resp.get('msg', '')}"
                )

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def send(self, msg: OutboundMessage) -> None:
        if self._client is None:
            logger.warning("Xiaohongshu: client not initialized; dropping outbound to {}", msg.chat_id)
            return

        # Progress-streaming chunks should not flood Xiaohongshu (no edit support).
        if (msg.metadata or {}).get("_progress"):
            return

        content = (msg.content or "").strip()
        if not content:
            return

        ctx = self._reply_ctx.get(msg.chat_id)
        if ctx is None:
            # Fall back to inferring from the chat_id format used in comments mode.
            if self.config.mode == "comments" and ":" in msg.chat_id:
                logger.warning(
                    "Xiaohongshu: no reply context for {} (lost across restart); skipping",
                    msg.chat_id,
                )
                return
            ctx = _ReplyContext(mode="messages", conversation_id=msg.chat_id)

        for chunk in split_message(content, XHS_MAX_MESSAGE_LEN):
            try:
                if ctx.mode == "messages":
                    await self._send_message(ctx.conversation_id or msg.chat_id, chunk)
                else:
                    await self._send_comment_reply(ctx, chunk)
            except Exception as e:
                logger.error("Xiaohongshu send failed for {}: {}", msg.chat_id, e)
                raise

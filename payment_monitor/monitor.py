"""IMAP poller that hands DBS receipt emails to the matcher.

Config is read from environment variables so credentials never live in the
repo:

    IMAP_HOST       e.g. imap.gmail.com
    IMAP_PORT       default 993
    IMAP_USERNAME
    IMAP_PASSWORD
    IMAP_MAILBOX    default INBOX
    IMAP_POLL_SECS  default 30
    IMAP_SENDER     optional, filter by sender address (e.g. dbs.com)
"""

from __future__ import annotations

import email
import imaplib
import os
import time
from email.message import Message

from .matcher import handle_payment
from .parser import ParseError, parse


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        raise SystemExit(f"missing required env var: {name}")
    return val or ""


def _extract_body(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True) or b""
                return payload.decode(part.get_content_charset() or "utf-8", "replace")
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True) or b""
                return payload.decode(part.get_content_charset() or "utf-8", "replace")
        return ""
    payload = msg.get_payload(decode=True) or b""
    return payload.decode(msg.get_content_charset() or "utf-8", "replace")


def _search_criteria() -> list[str]:
    criteria = ["UNSEEN"]
    sender = _env("IMAP_SENDER")
    if sender:
        criteria.extend(["FROM", sender])
    return criteria


def process_message(raw: bytes) -> None:
    msg = email.message_from_bytes(raw)
    body = _extract_body(msg)
    try:
        payment = parse(body)
    except ParseError as e:
        print(f"[SKIP] {msg.get('Subject', '')!r}: {e}")
        return
    handle_payment(payment)


def poll_once(conn: imaplib.IMAP4) -> None:
    conn.select(_env("IMAP_MAILBOX", "INBOX"))
    status, data = conn.search(None, *_search_criteria())
    if status != "OK":
        print(f"[ERROR] IMAP search failed: {status}")
        return
    for uid in data[0].split():
        status, msg_data = conn.fetch(uid, "(RFC822)")
        if status != "OK" or not msg_data or not msg_data[0]:
            continue
        process_message(msg_data[0][1])
        conn.store(uid, "+FLAGS", "\\Seen")


def run() -> None:
    host = _env("IMAP_HOST", required=True)
    port = int(_env("IMAP_PORT", "993"))
    user = _env("IMAP_USERNAME", required=True)
    pwd = _env("IMAP_PASSWORD", required=True)
    interval = int(_env("IMAP_POLL_SECS", "30"))

    while True:
        try:
            with imaplib.IMAP4_SSL(host, port) as conn:
                conn.login(user, pwd)
                poll_once(conn)
        except (imaplib.IMAP4.error, OSError) as e:
            print(f"[ERROR] IMAP session failed: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    run()

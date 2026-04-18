"""Parse DBS/POSB PayNow receipt emails."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

AMOUNT_RE = re.compile(
    r"received\s+(?P<currency>[A-Z]{3})\s+(?P<amount>[\d,]+\.\d{2})",
    re.IGNORECASE,
)
SENDER_RE = re.compile(r"^\s*From:\s*(?P<name>[^\n\r]+?)\s*$", re.MULTILINE)
TIMESTAMP_RE = re.compile(
    r"on\s+(?P<ts>\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}\s+[A-Z]{3})"
)
DBS_MARKERS = ("PayNow", "DBS", "POSB")


@dataclass(frozen=True)
class Payment:
    amount: Decimal
    currency: str
    sender: str
    timestamp: str | None


class ParseError(ValueError):
    pass


def is_dbs_receipt(text: str) -> bool:
    return "received" in text.lower() and any(m in text for m in DBS_MARKERS)


def parse(text: str) -> Payment:
    """Extract payment details from a DBS receipt email body.

    Raises ParseError if the text doesn't look like a DBS receipt or if
    required fields are missing.
    """
    if not is_dbs_receipt(text):
        raise ParseError("not a DBS/POSB PayNow receipt")

    amount_match = AMOUNT_RE.search(text)
    if not amount_match:
        raise ParseError("amount not found")

    sender_match = SENDER_RE.search(text)
    if not sender_match:
        raise ParseError("sender not found")

    ts_match = TIMESTAMP_RE.search(text)

    amount = Decimal(amount_match.group("amount").replace(",", ""))
    return Payment(
        amount=amount,
        currency=amount_match.group("currency").upper(),
        sender=sender_match.group("name").strip(),
        timestamp=ts_match.group("ts") if ts_match else None,
    )

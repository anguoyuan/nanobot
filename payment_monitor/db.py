"""SQLite helpers for the pending-payments ledger."""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Iterable

DB_PATH = Path(__file__).parent / "payments.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS pending_payments (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL,
    amount_cents  INTEGER NOT NULL,
    currency      TEXT NOT NULL DEFAULT 'SGD',
    status        TEXT NOT NULL DEFAULT 'pending',
    paid_at       TEXT,
    paid_by       TEXT
);
"""

MOCK_ROWS = [
    ("ORD-1001", "CUST-001", 850,   "SGD", "pending"),   # 8.50
    ("ORD-1002", "CUST-002", 2499,  "SGD", "pending"),   # 24.99
    ("ORD-1003", "CUST-003", 12000, "SGD", "pending"),   # 120.00
    ("ORD-1004", "CUST-004", 5000,  "SGD", "pending"),   # 50.00  (dup amount)
    ("ORD-1005", "CUST-005", 5000,  "SGD", "pending"),   # 50.00  (dup amount)
    ("ORD-1006", "CUST-006", 999,   "SGD", "pending"),   # 9.99
]


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path | None = None, rows: Iterable[tuple] = MOCK_ROWS) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute("DELETE FROM pending_payments")
        conn.executemany(
            "INSERT INTO pending_payments "
            "(order_id, customer_id, amount_cents, currency, status) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def to_cents(amount: Decimal) -> int:
    return int((amount * 100).to_integral_value())


def find_pending_by_amount(
    amount: Decimal,
    currency: str = "SGD",
    db_path: Path | None = None,
) -> list[sqlite3.Row]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM pending_payments "
            "WHERE status = 'pending' AND currency = ? AND amount_cents = ?",
            (currency, to_cents(amount)),
        )
        return cur.fetchall()


def mark_paid(
    order_id: str,
    paid_by: str,
    paid_at: str,
    db_path: Path | None = None,
) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE pending_payments "
            "SET status = 'paid', paid_at = ?, paid_by = ? "
            "WHERE order_id = ? AND status = 'pending'",
            (paid_at, paid_by, order_id),
        )
        conn.commit()


if __name__ == "__main__":
    init_db()
    with connect() as conn:
        for row in conn.execute("SELECT * FROM pending_payments"):
            print(dict(row))

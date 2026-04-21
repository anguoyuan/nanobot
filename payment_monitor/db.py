"""MySQL helpers for the orders payment ledger.

Connects directly to the tea-order-system MySQL database
and operates on the `orders` table's `payment_status` column.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pymysql
import pymysql.cursors

# MySQL connection config — reuse the same credentials as the Node backend.
# Password must be provided via DB_PASSWORD env var (no default), matching the
# policy in the Node backend's .env. See .env.example for the full list.
_DB_PASSWORD = os.environ.get("DB_PASSWORD")
if not _DB_PASSWORD:
    raise SystemExit(
        "DB_PASSWORD env var is required. Copy .env.example to .env and fill it in."
    )

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": _DB_PASSWORD,
    "database": os.getenv("DB_NAME", "tea_order_system"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def connect() -> pymysql.Connection:
    return pymysql.connect(**DB_CONFIG)


def to_cents(amount: Decimal) -> int:
    return int((amount * 100).to_integral_value())


def find_pending_by_amount(
    amount: Decimal,
    currency: str = "SGD",
) -> list[dict]:
    """Find unpaid orders whose total_price matches the payment amount."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, order_no, user_id, total_price, payment_status "
                "FROM orders "
                "WHERE payment_status = 'unpaid' "
                "AND ROUND(total_price * 100) = %s",
                (to_cents(amount),),
            )
            return cur.fetchall()
    finally:
        conn.close()


def mark_paid(
    order_no: str,
    paid_by: str,
    paid_at: str,
) -> None:
    """Set payment_status = 'paid' for the given order."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders "
                "SET payment_status = 'paid' "
                "WHERE order_no = %s AND payment_status = 'unpaid'",
                (order_no,),
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, order_no, user_id, total_price, payment_status "
                "FROM orders ORDER BY create_time DESC LIMIT 20"
            )
            for row in cur.fetchall():
                print(row)
    finally:
        conn.close()

"""Match a received payment against the MySQL orders table."""

from __future__ import annotations

from datetime import datetime, timezone

from . import db
from .parser import Payment


def handle_payment(payment: Payment) -> None:
    """Match a parsed payment against unpaid orders and update payment_status.

    Behaviour:
      * 0 matches -> log and ignore (not for any known order).
      * 1 match   -> mark as paid, print a confirmation.
      * >1 match  -> do nothing, print an alert for the back office.
    """
    rows = db.find_pending_by_amount(payment.amount, payment.currency)

    if not rows:
        print(
            f"[INFO] received {payment.currency} {payment.amount} from "
            f"{payment.sender}: no unpaid order with this amount, ignoring."
        )
        return

    if len(rows) > 1:
        order_nos = ", ".join(r["order_no"] for r in rows)
        print(
            f"[ALERT] received {payment.currency} {payment.amount} from "
            f"{payment.sender} but {len(rows)} unpaid orders match "
            f"({order_nos}). Manual review required; no action taken."
        )
        return

    row = rows[0]
    paid_at = payment.timestamp or datetime.now(timezone.utc).isoformat()
    db.mark_paid(row["order_no"], payment.sender, paid_at)
    print(
        f"[PAID] order {row['order_no']} (user_id {row['user_id']}, "
        f"SGD {row['total_price']}) marked as paid — received from "
        f"{payment.sender} at {paid_at}."
    )

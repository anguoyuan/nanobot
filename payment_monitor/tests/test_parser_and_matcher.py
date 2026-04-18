"""End-to-end tests for parser + matcher using a temp SQLite DB."""

from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from payment_monitor import db, matcher  # noqa: E402
from payment_monitor.parser import ParseError, parse  # noqa: E402

SAMPLE = """Transaction Ref: PIB2603189654865075 C100512515969


Dear Customer,

You have received SGD 8.50 via PayNow on 18 Mar 2026 20:38 SGT.

From: YANG QU
To: Your DBS/ POSB account ending 6337

Didn't expect these funds? If this is a joint account, it may be for your
joint account holder. Otherwise, please call our DBS hotline.

Thank you for banking with us.

Yours faithfully
DBS Bank Ltd
"""


class ParserTests(unittest.TestCase):
    def test_parses_amount_sender_timestamp(self):
        p = parse(SAMPLE)
        self.assertEqual(p.amount, Decimal("8.50"))
        self.assertEqual(p.currency, "SGD")
        self.assertEqual(p.sender, "YANG QU")
        self.assertEqual(p.timestamp, "18 Mar 2026 20:38 SGT")

    def test_rejects_non_dbs_text(self):
        with self.assertRaises(ParseError):
            parse("hello world, just some random email")


class MatcherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.__class__.__name__ + ".db")
        if self.tmp.exists():
            self.tmp.unlink()
        db.init_db(self.tmp)
        self._patched = db.DB_PATH
        db.DB_PATH = self.tmp

    def tearDown(self):
        db.DB_PATH = self._patched
        if self.tmp.exists():
            self.tmp.unlink()

    def _run(self, text: str) -> str:
        buf = io.StringIO()
        with redirect_stdout(buf):
            matcher.handle_payment(parse(text))
        return buf.getvalue()

    def test_single_match_marks_paid(self):
        out = self._run(SAMPLE)
        self.assertIn("[PAID] order ORD-1001", out)
        with db.connect(self.tmp) as conn:
            row = conn.execute(
                "SELECT status, paid_by FROM pending_payments WHERE order_id = 'ORD-1001'"
            ).fetchone()
        self.assertEqual(row["status"], "paid")
        self.assertEqual(row["paid_by"], "YANG QU")

    def test_multiple_matches_alerts_and_does_nothing(self):
        text = SAMPLE.replace("SGD 8.50", "SGD 50.00")
        out = self._run(text)
        self.assertIn("[ALERT]", out)
        self.assertIn("ORD-1004", out)
        self.assertIn("ORD-1005", out)
        with db.connect(self.tmp) as conn:
            statuses = [
                r["status"]
                for r in conn.execute(
                    "SELECT status FROM pending_payments WHERE amount_cents = 5000"
                )
            ]
        self.assertEqual(statuses, ["pending", "pending"])

    def test_no_match_ignored(self):
        text = SAMPLE.replace("SGD 8.50", "SGD 77.77")
        out = self._run(text)
        self.assertIn("[INFO]", out)
        self.assertIn("no pending order", out)


if __name__ == "__main__":
    unittest.main()

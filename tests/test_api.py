"""HTTP API 端到端测试（仅标准库 urllib）。"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer

from emergency_connectivity.api import create_handler
from emergency_connectivity.store import EventStore


def iso(m: int) -> str:
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(minutes=m)).isoformat()


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(EventStore()))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_full_lifecycle_and_explain(self):
        status, _ = self.call("POST", "/offers", {
            "offer_code": "O1", "operator": "甲", "area_code": "A",
            "link_code": "L1", "capacity_mbps": 100,
            "start": iso(0), "end": iso(120), "at": iso(0),
        })
        self.assertEqual(status, 200)

        status, body = self.call("POST", "/requests", {
            "request_code": "R1", "area_code": "A", "priority_level": 1,
            "min_capacity_mbps": 40, "start": iso(1), "end": iso(90),
            "fallback_areas": [], "at": iso(1),
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["decisions"][0]["outcome"], "allocated")

        # 跨渠道重复送达：幂等
        status, dup = self.call("POST", "/requests", {
            "request_code": "R1", "area_code": "A", "priority_level": 1,
            "min_capacity_mbps": 40, "start": iso(1), "end": iso(90),
            "fallback_areas": [], "at": iso(1),
        })
        self.assertTrue(dup["duplicate"])

        status, _ = self.call("POST", "/usage", {
            "request_code": "R1", "bytes_transmitted": 999, "at": iso(5),
        })
        self.assertEqual(status, 200)

        status, body = self.call("POST", "/links/L1/interrupt", {"at": iso(10)})
        self.assertEqual(status, 200)
        self.assertEqual(body["decisions"][0]["outcome"], "waitlisted")

        status, report = self.call("GET", "/requests/R1/explain")
        self.assertEqual(status, 200)
        self.assertEqual([r["outcome"] for r in report["timeline"]], ["allocated", "waitlisted"])
        self.assertEqual(report["total_bytes_transmitted"], 999)

    def test_validation_error_returns_422(self):
        status, body = self.call("POST", "/requests", {"request_code": "X"})
        self.assertEqual(status, 422)
        self.assertIn("error", body)

    def test_override_flow_and_events_audit(self):
        self.call("POST", "/offers", {
            "offer_code": "O1", "operator": "甲", "area_code": "A",
            "link_code": "L1", "capacity_mbps": 100,
            "start": iso(0), "end": iso(120), "at": iso(0),
        })
        self.call("POST", "/requests", {
            "request_code": "R1", "area_code": "A", "priority_level": 5,
            "min_capacity_mbps": 40, "start": iso(1), "end": iso(90),
            "at": iso(1),
        })
        status, body = self.call("POST", "/overrides", {
            "override_code": "OV1", "request_code": "R1", "area_code": "A",
            "link_code": "L1", "capacity_mbps": 40,
            "start": iso(10), "end": iso(30),
            "owner": "张指挥", "reason": "视频会诊", "at": iso(10),
        })
        self.assertEqual(status, 200)
        # 规则授权被特批授权替换：记为 switched
        self.assertEqual(body["decisions"][0]["outcome"], "switched")

        status, events = self.call("GET", "/events")
        self.assertEqual(status, 200)
        types = [e["type"] for e in events["events"]]
        self.assertIn("override.granted", types)
        self.assertIn("authorization.granted", types)
        # 审计流严格有序
        seqs = [e["seq"] for e in events["events"]]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))


if __name__ == "__main__":
    unittest.main()

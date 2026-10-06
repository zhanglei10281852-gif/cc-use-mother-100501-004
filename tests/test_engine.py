"""编排引擎规则测试。"""

from __future__ import annotations

import threading
import unittest
from datetime import datetime, timedelta, timezone

from emergency_connectivity.engine import (
    ALLOCATED,
    DOWNGRADED,
    REJECTED,
    REINSTATED,
    RETAINED,
    SWITCHED,
    TERMINATED,
    WAITLISTED,
    EngineError,
    Orchestrator,
)
from emergency_connectivity.store import EventStore, fold

T0 = datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)


def t(m: int) -> datetime:
    return T0 + timedelta(minutes=m)


class EngineTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Orchestrator(EventStore())

    def offer(self, code="O-A", area="A", link="L1", capacity=100, start=0, end=120, at=0):
        self.engine.publish_offer(code, "op", area, link, capacity, t(start), t(end), at=t(at))
        return code

    def request(self, code, area="A", priority=3, need=40, start=1, end=90, fallback=(), at=1):
        return self.engine.submit_request(
            code, area, priority, need, t(start), t(end), fallback, at=t(at)
        )

    def outcomes(self, result):
        return {d["request_code"]: d["outcome"] for d in result.get("decisions", [])}


class TestAllocation(EngineTestBase):
    def test_allocates_within_capacity(self):
        self.offer(capacity=100)
        r = self.request("R1")
        self.assertEqual(self.outcomes(r)["R1"], ALLOCATED)

    def test_capacity_never_double_booked(self):
        self.offer(capacity=100)
        self.request("R1", priority=1, need=40)
        self.request("R2", priority=2, need=40)
        r = self.request("R3", priority=5, need=40)
        # 100Mbps 放不下第三个 40Mbps
        self.assertEqual(self.outcomes(r)["R3"], WAITLISTED)
        state = self.engine.state()
        active = [h for h in state.auths.values() if not h.closed]
        self.assertLessEqual(sum(a.auth.capacity_mbps for a in active), 100)

    def test_higher_priority_preempts_lower(self):
        self.offer(capacity=100)
        self.request("LOW", priority=5, need=60)
        self.request("MID", priority=4, need=40)  # LOW 60 + MID 40 = 100
        r = self.request("HIGH", priority=1, need=60)
        out = self.outcomes(r)
        self.assertEqual(out["HIGH"], ALLOCATED)
        self.assertEqual(out["LOW"], WAITLISTED)  # 同级容量中最低优先级被抢占

    def test_equal_priority_protects_earlier_request(self):
        self.offer(capacity=100)
        self.request("EARLY", priority=2, need=60, at=1)
        self.request("LATER", priority=2, need=40, at=2)  # 占满
        # 同优先级后来者不能驱逐更早的请求
        r = self.request("ANOTHER", priority=2, need=60, at=3)
        self.assertEqual(self.outcomes(r)["ANOTHER"], WAITLISTED)
        self.assertIsNotNone(self.engine.state().active_auth_for("EARLY", t(3)))

    def test_structural_rejection_when_no_offer_anywhere(self):
        self.offer(area="A")
        r = self.request("R1", area="C", fallback=())
        self.assertEqual(self.outcomes(r)["R1"], REJECTED)

    def test_waitlist_reinstated_when_future_offer_arrives(self):
        # 只有 t=20 才开始有能力
        self.offer(start=20, end=120)
        r = self.request("R1", start=1, end=90)
        self.assertEqual(self.outcomes(r)["R1"], WAITLISTED)
        r = self.engine.sweep(at=t(20))
        self.assertEqual(self.outcomes(r)["R1"], REINSTATED)


class TestFailover(EngineTestBase):
    def test_link_interruption_downgrades_to_fallback(self):
        self.offer(code="O-A", area="A", link="L1", capacity=100)
        self.offer(code="O-B", area="B", link="L2", capacity=100)
        self.request("R1", area="A", fallback=("B",), need=40)
        r = self.engine.interrupt_link("L1", at=t(10))
        self.assertEqual(self.outcomes(r)["R1"], DOWNGRADED)
        auth = self.engine.state().active_auth_for("R1", t(10))
        self.assertEqual(auth.auth.area_code, "B")

    def test_link_interruption_without_fallback_waitlists(self):
        self.offer(area="A", link="L1")
        self.request("R1", area="A", fallback=())
        r = self.engine.interrupt_link("L1", at=t(10))
        self.assertEqual(self.outcomes(r)["R1"], WAITLISTED)

    def test_restore_switches_back_to_preferred_area(self):
        self.offer(code="O-A", area="A", link="L1", capacity=100)
        self.offer(code="O-B", area="B", link="L2", capacity=100)
        self.request("R1", area="A", fallback=("B",), need=40)
        self.engine.interrupt_link("L1", at=t(10))
        r = self.engine.restore_link("L1", at=t(20))
        self.assertEqual(self.outcomes(r)["R1"], SWITCHED)
        auth = self.engine.state().active_auth_for("R1", t(20))
        self.assertEqual(auth.auth.area_code, "A")

    def test_offer_withdraw_releases_capacity(self):
        self.offer(code="O-A", area="A", link="L1")
        self.request("R1", area="A")
        r = self.engine.withdraw_offer("O-A", at=t(10))
        # 唯一能力被撤回、可接受区域内再无能力：结构性拒绝
        self.assertEqual(self.outcomes(r)["R1"], REJECTED)


class TestIdempotency(EngineTestBase):
    def test_duplicate_submit_from_another_channel_is_idempotent(self):
        self.offer()
        self.request("R1")
        dup = self.request("R1")
        self.assertTrue(dup["duplicate"])
        self.assertEqual(len([e for e in self.engine.store.events() if e.type == "request.submitted"]), 1)
        # 没有双占：只有一条授权
        auths = [h for h in self.engine.state().auths.values()]
        self.assertEqual(len(auths), 1)

    def test_same_request_code_with_conflicting_payload_rejected(self):
        self.offer()
        self.request("R1", need=40)
        with self.assertRaises(EngineError):
            self.request("R1", need=80)

    def test_explicit_idem_key_dedupes(self):
        self.offer()
        kwargs = dict(area_code="A", priority_level=2, min_capacity_mbps=40,
                      start=t(1), end=t(90), fallback_areas=(), at=t(1))
        self.engine.submit_request("R9", idem="msg-123", **kwargs)
        again = self.engine.submit_request("R9", idem="msg-123", **kwargs)
        self.assertTrue(again["duplicate"])

    def test_concurrent_duplicate_submits_allocate_once(self):
        self.offer(capacity=100)
        errors: list[Exception] = []

        def fire():
            try:
                self.request("DUP", priority=2, need=40)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=fire) for _ in range(10)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])
        auths = [h for h in self.engine.state().auths.values()]
        self.assertEqual(len(auths), 1)

    def test_concurrent_mixed_requests_respect_capacity(self):
        self.offer(capacity=100)
        errors: list[Exception] = []

        def fire(code, priority):
            try:
                self.request(code, priority=priority, need=40, at=1)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=fire, args=(f"R{i}", (i % 5) + 1))
            for i in range(12)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])
        active = [h for h in self.engine.state().auths.values() if not h.closed]
        self.assertLessEqual(sum(a.auth.capacity_mbps for a in active), 100)
        self.assertLessEqual(len(active), 2)

    def test_cancel_is_idempotent_under_race(self):
        self.offer()
        self.request("R1")
        results = []

        def fire():
            results.append(self.engine.cancel_request("R1", at=t(5)))

        threads = [threading.Thread(target=fire) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(sum(1 for r in results if not r.get("duplicate")), 1)


class TestOverride(EngineTestBase):
    def _preempted_setup(self):
        self.offer(capacity=100)
        self.request("R1", priority=1, need=60)
        self.request("R2", priority=2, need=40)  # 占满
        self.request("LOW", priority=5, need=40)  # 候补

    def test_override_requires_owner_reason_and_period(self):
        self.offer()
        self.request("R1")
        with self.assertRaises(ValueError):
            self.engine.grant_override(
                "OV", "R1", "A", "L1", 40, t(5), t(5), owner="", reason="x", at=t(5)
            )
        with self.assertRaises(ValueError):
            self.engine.grant_override(
                "OV", "R1", "A", "L1", 40, t(5), t(20), owner="张指挥", reason=" ", at=t(5)
            )

    def test_override_locks_resource_and_expires_with_owner(self):
        self._preempted_setup()
        r = self.engine.grant_override(
            "OV-1", "LOW", "A", "L1", 40, t(10), t(30),
            owner="张指挥", reason="视频会诊", at=t(10),
        )
        out = self.outcomes(r)
        self.assertEqual(out["LOW"], REINSTATED)
        self.assertIn(out["R2"], (DOWNGRADED, WAITLISTED))
        # 特批授权带责任人来源
        state = self.engine.state()
        ov_auth = next(
            h.auth for h in state.auths.values()
            if h.auth.request_code == "LOW" and not h.closed and h.auth.source == "override"
        )
        self.assertEqual(ov_auth.area_code, "A")

        # 到期自动回到规则编排
        r = self.engine.sweep(at=t(30))
        out = self.outcomes(r)
        self.assertEqual(out["LOW"], WAITLISTED)
        self.assertIn(out["R2"], (REINSTATED, SWITCHED))

    def test_two_active_overrides_same_request_rejected(self):
        self.offer()
        self.request("R1")
        self.engine.grant_override("OV1", "R1", "A", "L1", 40, t(5), t(30),
                                   owner="甲", reason="r", at=t(5))
        with self.assertRaises(EngineError):
            self.engine.grant_override("OV2", "R1", "A", "L1", 40, t(6), t(30),
                                       owner="乙", reason="r", at=t(6))


class TestRenewalAndSweep(EngineTestBase):
    def test_expired_auth_auto_reclaimed(self):
        self.offer(start=0, end=120)
        self.request("R1", start=1, end=30)
        self.assertIsNotNone(self.engine.state().active_auth_for("R1", t(10)))
        r = self.engine.sweep(at=t(31))
        self.assertEqual(self.outcomes(r)["R1"], TERMINATED)
        self.assertTrue(all(h.closed for h in self.engine.state().auths.values()))

    def test_renew_extends_window_without_touching_history(self):
        self.offer(start=0, end=120)
        self.request("R1", start=1, end=30)
        self.engine.report_usage("R1", 1000, at=t(10))
        before = list(self.engine.state().auths.values())[0].auth.auth_code

        self.engine.renew_request("R1", t(10), t(80), at=t(15))
        r = self.engine.sweep(at=t(31))
        self.assertNotIn("R1", self.outcomes(r))  # 续期后未到期，无终止决策
        self.assertIsNotNone(self.engine.state().active_auth_for("R1", t(40)))
        self.assertEqual(self.engine.state().usage_total("R1"), 1000)
        # 历史授权未被改写
        self.assertIn(before, self.engine.state().auths)

    def test_cannot_renew_cancelled(self):
        self.offer()
        self.request("R1", end=30)
        self.engine.cancel_request("R1", at=t(5))
        with self.assertRaises(EngineError):
            self.engine.renew_request("R1", t(5), t(80), at=t(6))


class TestUsage(EngineTestBase):
    def test_usage_append_only_after_termination(self):
        self.offer()
        self.request("R1", end=30)
        self.engine.report_usage("R1", 1000, at=t(5))
        self.engine.report_usage("R1", 2000, at=t(8))
        self.engine.cancel_request("R1", at=t(10))
        self.assertEqual(self.engine.state().usage_total("R1"), 3000)
        events = [e for e in self.engine.store.events() if e.type == "usage.reported"]
        self.assertEqual(len(events), 2)  # 没有任何改写

    def test_usage_outside_auth_window_rejected(self):
        self.offer()
        self.request("R1", start=1, end=10)
        with self.assertRaises(EngineError):
            self.engine.report_usage("R1", 1, at=t(20))

    def test_usage_auth_mismatch_rejected(self):
        self.offer()
        self.request("R1", end=30)
        self.request("R2", end=30, at=2)
        auth1 = self.engine.state().active_auth_for("R1", t(3)).auth.auth_code
        with self.assertRaises(EngineError):
            self.engine.report_usage("R2", 1, at=t(3), auth_code=auth1)


class TestExplain(EngineTestBase):
    def test_timeline_explains_each_change(self):
        self.offer(code="O-A", area="A", link="L1", capacity=100)
        self.offer(code="O-B", area="B", link="L2", capacity=100)
        self.request("R1", area="A", fallback=("B",), need=40)
        self.engine.interrupt_link("L1", at=t(10))
        self.engine.restore_link("L1", at=t(20))
        report = self.engine.explain("R1", at=t(25))
        outcomes = [row["outcome"] for row in report["timeline"]]
        self.assertEqual(outcomes, [ALLOCATED, DOWNGRADED, SWITCHED])
        for row in report["timeline"]:
            self.assertTrue(row["reason"])
        self.assertEqual(report["current_status"], "active:A/L1")


class TestReplay(unittest.TestCase):
    def test_rebuild_from_journal_matches(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            engine = Orchestrator(EventStore(path))
            engine.publish_offer("O1", "op", "A", "L1", 100, t(0), t(120), at=t(0))
            engine.submit_request("R1", "A", 1, 40, t(1), t(90), ("B",), at=t(1))
            engine.interrupt_link("L1", at=t(10))

            replayed = fold(list(EventStore(path).events()))
            live = engine.state()
            self.assertEqual(
                sorted(replayed.auths), sorted(live.auths)
            )
            self.assertEqual(
                [d.outcome for d in replayed.decisions],
                [d.outcome for d in live.decisions],
            )
            self.assertEqual(replayed.interrupted_links, {"L1"})

    def test_repeated_sweep_writes_no_duplicate_events(self):
        engine = Orchestrator(EventStore())
        engine.publish_offer("O1", "op", "A", "L1", 100, t(0), t(120), at=t(0))
        engine.submit_request("R1", "A", 1, 40, t(1), t(90), (), at=t(1))
        n1 = len(engine.store.events())
        engine.sweep(at=t(5))
        engine.sweep(at=t(6))
        n2 = len(engine.store.events())
        self.assertEqual(n1, n2)


if __name__ == "__main__":
    unittest.main()

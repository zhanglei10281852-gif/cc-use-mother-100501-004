"""端到端剧情演示：用确定性时间线走通全部编排规则。

时间线（UTC，2026-07-15）
-------------------------
00:00 运营商发布 A 区 SAT-1（100Mbps）与 B 区 SAT-2（60Mbps）
00:01 医院(1级,40M,主A备B)、安置点(5级,40M,主A备B)、
      观测站(5级,30M,仅C区) 提交；医院请求经另一渠道重复送达
00:02 后勤供电点(3级,60M,仅B区) 占满备用链路
00:05 医院登记用量 12MB
00:10 重症转运队(1级,40M,主A备B) 到达：抢占安置点，安置点候补
00:20 主链路 SAT-1 中断：医院降级到备用区，转运队/供电点候补
00:30 SAT-1 恢复：医院切回主区，转运队、供电点恢复，安置点仍候补
00:35 人工特批安置点锁定 SAT-1（35~50 分，责任人张指挥）：
      转运队临时降级到 B 区
00:40 安置点在特批授权下传输 5MB
00:50 扫描：特批到期自动回收，转运队回主链路
01:10 安置点主动取消 → 授权回收，用量保留
01:31 扫描：保障窗口到期，授权全部回收，历史用量不变
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from .engine import Orchestrator
from .store import EventStore

T0 = datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)


def t(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def run() -> dict:
    engine = Orchestrator(EventStore())
    trace: list[dict] = []

    def step(title: str, result: dict) -> None:
        trace.append({
            "step": title,
            "duplicate": result.get("duplicate", False),
            "decisions": [
                {"request": d["request_code"], "outcome": d["outcome"], "reason": d["reason"]}
                for d in result.get("decisions", [])
            ],
        })

    engine.publish_offer("offer-A1", "运营商甲", "AREA-A", "SAT-1", 100, t(0), t(120), at=t(0))
    engine.publish_offer("offer-B1", "运营商乙", "AREA-B", "SAT-2", 60, t(0), t(120), at=t(0))

    r = engine.submit_request("REQ-HOSP", "AREA-A", 1, 40, t(1), t(90), ("AREA-B",), at=t(1))
    step("医院请求 40Mbps（1 级，主 A 备 B）", r)
    engine.submit_request("REQ-SHELTER", "AREA-A", 5, 40, t(1), t(90), ("AREA-B",), at=t(1))
    r = engine.submit_request("REQ-OBS", "AREA-C", 5, 30, t(1), t(90), (), at=t(1))
    step("观测站仅接受 C 区（无能力，结构性拒绝）", r)
    r = engine.submit_request("REQ-HOSP", "AREA-A", 1, 40, t(1), t(90), ("AREA-B",), at=t(1))
    step("另一渠道重复送达医院请求（幂等，不双占）", r)

    engine.submit_request("REQ-POWER", "AREA-B", 3, 60, t(2), t(60), (), at=t(2))
    engine.report_usage("REQ-HOSP", 12_000_000, at=t(5))

    r = engine.submit_request("REQ-ICU", "AREA-A", 1, 40, t(10), t(90), ("AREA-B",), at=t(10))
    step("重症转运队 1 级到达，抢占安置点", r)

    r = engine.interrupt_link("SAT-1", at=t(20))
    step("主链路 SAT-1 中断", r)

    r = engine.restore_link("SAT-1", at=t(30))
    step("SAT-1 恢复，重新评估未结束任务", r)

    r = engine.grant_override(
        "OV-001", "REQ-SHELTER", "AREA-A", "SAT-1", 40, t(35), t(50),
        owner="张指挥", reason="安置点临时收治伤员，需视频会诊", at=t(35),
    )
    step("人工特批安置点锁定 SAT-1（35~50 分，责任人：张指挥）", r)

    engine.report_usage("REQ-SHELTER", 5_000_000, at=t(40))

    r = engine.sweep(at=t(50))
    step("50 分扫描：特批到期自动回收，回到规则编排", r)

    r = engine.cancel_request("REQ-SHELTER", at=t(70))
    step("安置点主动取消，授权回收", r)

    r = engine.sweep(at=t(91))
    step("91 分扫描：保障窗口到期，授权自动回收", r)

    shelter = engine.explain("REQ-SHELTER", at=t(95))
    hospital = engine.explain("REQ-HOSP", at=t(95))
    icu = engine.explain("REQ-ICU", at=t(95))
    return {
        "trace": trace,
        "shelter_explanation": shelter,
        "hospital_final": {
            "status": hospital["current_status"],
            "total_bytes_transmitted": hospital["total_bytes_transmitted"],
            "authorization_versions": len(hospital["authorizations"]),
        },
        "icu_final": {
            "status": icu["current_status"],
            "total_bytes_transmitted": icu["total_bytes_transmitted"],
        },
        "invariants": {
            "用量在取消/到期后保持不变": (
                hospital["total_bytes_transmitted"] == 12_000_000
                and shelter["total_bytes_transmitted"] == 5_000_000
            ),
            "所有授权均已关闭": all(
                a["status"] == "closed"
                for a in hospital["authorizations"]
                + shelter["authorizations"]
                + icu["authorizations"]
            ),
        },
    }


def main() -> None:
    print(json.dumps(run(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

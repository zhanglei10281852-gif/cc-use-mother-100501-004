"""追加式事件日志。

所有状态变化都是事件：业务命令产生领域事件，引擎在结算时派生
授权/关闭/决策事件。事件一旦写入即不可修改；重放事件即可重建全部状态，
从而保证「已传输用量与当时授权不能被改写」。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from typing import Any

# 领域事件（由命令产生）
REQUEST_SUBMITTED = "request.submitted"
REQUEST_CANCELLED = "request.cancelled"
REQUEST_RENEWED = "request.renewed"
PRIORITY_CHANGED = "request.priority_changed"
OFFER_PUBLISHED = "offer.published"
OFFER_WITHDRAWN = "offer.withdrawn"
LINK_INTERRUPTED = "link.interrupted"
LINK_RESTORED = "link.restored"
OVERRIDE_GRANTED = "override.granted"
OVERRIDE_REVOKED = "override.revoked"
USAGE_REPORTED = "usage.reported"

# 派生事件（引擎结算时确定性产生）
AUTH_GRANTED = "authorization.granted"
AUTH_CLOSED = "authorization.closed"
DECISION_RECORDED = "decision.recorded"


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    event_id: str
    idem: str
    type: str
    at: datetime
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "idem": self.idem,
            "type": self.type,
            "at": self.at.isoformat(),
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Event":
        return cls(
            seq=int(raw["seq"]),
            event_id=str(raw["event_id"]),
            idem=str(raw["idem"]),
            type=str(raw["type"]),
            at=datetime.fromisoformat(raw["at"]),
            payload=dict(raw["payload"]),
        )


def derived_idem(kind: str, *parts: object) -> str:
    """为派生事件生成确定性幂等键，保证重放不会重复落账。"""
    body = json.dumps([kind, *parts], ensure_ascii=False, sort_keys=True, default=str)
    return "derived:" + sha256(body.encode("utf-8")).hexdigest()[:32]

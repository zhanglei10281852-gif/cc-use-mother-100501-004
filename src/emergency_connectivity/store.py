"""事件存储与状态折叠。

存储是一个可持久化到 JSONL 的追加日志；``fold`` 通过重放事件重建当前状态。
所有写入在同一把锁内串行完成，命令与结算之间不会出现中间态。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import threading
from pathlib import Path
from typing import Any

from .events import (
    AUTH_CLOSED,
    AUTH_GRANTED,
    DECISION_RECORDED,
    Event,
    LINK_INTERRUPTED,
    LINK_RESTORED,
    OFFER_PUBLISHED,
    OFFER_WITHDRAWN,
    OVERRIDE_GRANTED,
    OVERRIDE_REVOKED,
    PRIORITY_CHANGED,
    REQUEST_CANCELLED,
    REQUEST_RENEWED,
    REQUEST_SUBMITTED,
    USAGE_REPORTED,
)
from .model import Authorization, CapabilityOffer, Decision, ManualOverride, RequestSpec, UsageRecord, Window


def _parse_at(raw: str | datetime) -> datetime:
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raise ValueError("时间必须携带时区")
        return raw
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        raise ValueError("时间必须携带时区")
    return parsed


class EventStore:
    """线程安全的追加日志；幂等键相同的重复写入返回原事件。"""

    def __init__(self, path: str | Path | None = None) -> None:
        self._lock = threading.RLock()
        self._events: list[Event] = []
        self._idem: dict[str, int] = {}
        self._path = Path(path) if path else None
        if self._path and self._path.exists():
            self._load()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def _load(self) -> None:
        assert self._path is not None
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = Event.from_dict(json.loads(line))
            self._idem[event.idem] = event.seq
            self._events.append(event)

    def append(self, type_: str, at: datetime, payload: dict[str, Any], idem: str) -> tuple[Event, bool]:
        """写入事件；返回 (事件, 是否为重复投递)。重复时返回首个事件。"""
        with self._lock:
            if idem in self._idem:
                return self._events[self._idem[idem]], True
            event = Event(
                seq=len(self._events) + 1,
                event_id=f"evt-{len(self._events) + 1:08d}",
                idem=idem,
                type=type_,
                at=at,
                payload=payload,
            )
            self._events.append(event)
            self._idem[idem] = event.seq
            if self._path is not None:
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
            return event, False

    def events(self) -> list[Event]:
        with self._lock:
            return list(self._events)


@dataclass
class AuthState:
    auth: Authorization
    closed: bool = False
    closed_at: datetime | None = None
    close_reason: str | None = None


@dataclass
class RequestState:
    spec: RequestSpec
    submitted_at: datetime
    cancelled: bool = False
    last_change_at: datetime | None = None


@dataclass
class State:
    """重放事件得到的完整快照。"""

    requests: dict[str, RequestState] = field(default_factory=dict)
    offers: dict[str, CapabilityOffer] = field(default_factory=dict)
    withdrawn_offers: set[str] = field(default_factory=set)
    interrupted_links: set[str] = field(default_factory=set)
    overrides: dict[str, ManualOverride] = field(default_factory=dict)
    revoked_overrides: set[str] = field(default_factory=set)
    auths: dict[str, AuthState] = field(default_factory=dict)
    usage: list[UsageRecord] = field(default_factory=list)
    decisions: list[Decision] = field(default_factory=list)
    seq: int = 0

    def active_auth_for(self, request_code: str, at: datetime) -> AuthState | None:
        for state in self.auths.values():
            if state.auth.request_code != request_code or state.closed:
                continue
            if state.auth.window.start <= at < state.auth.window.end:
                return state
        return None

    def open_requests(self, at: datetime) -> list[RequestState]:
        out = []
        for rs in self.requests.values():
            if not rs.cancelled and rs.spec.window.end > at:
                out.append(rs)
        return out

    def usage_total(self, request_code: str) -> int:
        return sum(u.bytes_transmitted for u in self.usage if u.request_code == request_code)


def _window(payload: dict[str, Any]) -> Window:
    return Window(start=datetime.fromisoformat(payload["start"]), end=datetime.fromisoformat(payload["end"]))


def fold(events: list[Event]) -> State:
    """从事件流确定性重建状态。"""
    state = State(seq=len(events))
    for event in events:
        p = event.payload
        if event.type == REQUEST_SUBMITTED:
            spec = RequestSpec(
                request_code=p["request_code"],
                area_code=p["area_code"],
                priority_level=int(p["priority_level"]),
                min_capacity_mbps=int(p["min_capacity_mbps"]),
                window=Window(datetime.fromisoformat(p["start"]), datetime.fromisoformat(p["end"])),
                fallback_areas=tuple(p.get("fallback_areas", ())),
            )
            state.requests[spec.request_code] = RequestState(spec=spec, submitted_at=event.at)
        elif event.type == REQUEST_RENEWED:
            rs = state.requests[p["request_code"]]
            new_window = Window(datetime.fromisoformat(p["start"]), datetime.fromisoformat(p["end"]))
            rs.spec = RequestSpec(
                request_code=rs.spec.request_code,
                area_code=rs.spec.area_code,
                priority_level=rs.spec.priority_level,
                min_capacity_mbps=rs.spec.min_capacity_mbps,
                window=new_window,
                fallback_areas=rs.spec.fallback_areas,
            )
            rs.last_change_at = event.at
        elif event.type == PRIORITY_CHANGED:
            rs = state.requests[p["request_code"]]
            rs.spec = RequestSpec(
                request_code=rs.spec.request_code,
                area_code=rs.spec.area_code,
                priority_level=int(p["priority_level"]),
                min_capacity_mbps=rs.spec.min_capacity_mbps,
                window=rs.spec.window,
                fallback_areas=rs.spec.fallback_areas,
            )
            rs.last_change_at = event.at
        elif event.type == REQUEST_CANCELLED:
            state.requests[p["request_code"]].cancelled = True
        elif event.type == OFFER_PUBLISHED:
            offer = CapabilityOffer(
                offer_code=p["offer_code"],
                operator=p["operator"],
                area_code=p["area_code"],
                link_code=p["link_code"],
                window=_window(p),
                capacity_mbps=int(p["capacity_mbps"]),
            )
            state.offers[offer.offer_code] = offer
            state.withdrawn_offers.discard(offer.offer_code)
        elif event.type == OFFER_WITHDRAWN:
            state.withdrawn_offers.add(p["offer_code"])
        elif event.type == LINK_INTERRUPTED:
            state.interrupted_links.add(p["link_code"])
        elif event.type == LINK_RESTORED:
            state.interrupted_links.discard(p["link_code"])
        elif event.type == OVERRIDE_GRANTED:
            ov = ManualOverride(
                override_code=p["override_code"],
                request_code=p["request_code"],
                area_code=p["area_code"],
                link_code=p["link_code"],
                capacity_mbps=int(p["capacity_mbps"]),
                window=_window(p),
                owner=p["owner"],
                reason=p["reason"],
            )
            state.overrides[ov.override_code] = ov
            state.revoked_overrides.discard(ov.override_code)
        elif event.type == OVERRIDE_REVOKED:
            state.revoked_overrides.add(p["override_code"])
        elif event.type == USAGE_REPORTED:
            state.usage.append(
                UsageRecord(
                    request_code=p["request_code"],
                    at=event.at,
                    bytes_transmitted=int(p["bytes_transmitted"]),
                    auth_code=p.get("auth_code"),
                )
            )
        elif event.type == AUTH_GRANTED:
            auth = Authorization(
                auth_code=p["auth_code"],
                request_code=p["request_code"],
                offer_code=p["offer_code"],
                link_code=p["link_code"],
                area_code=p["area_code"],
                capacity_mbps=int(p["capacity_mbps"]),
                window=_window(p),
                source=p["source"],
                decided_at=event.at,
            )
            state.auths[auth.auth_code] = AuthState(auth=auth)
        elif event.type == AUTH_CLOSED:
            holder = state.auths[p["auth_code"]]
            holder.closed = True
            holder.closed_at = event.at
            holder.close_reason = p["reason"]
        elif event.type == DECISION_RECORDED:
            state.decisions.append(
                Decision(
                    request_code=p["request_code"],
                    at=event.at,
                    outcome=p["outcome"],
                    reason=p["reason"],
                    area_code=p.get("area_code"),
                    link_code=p.get("link_code"),
                    offer_code=p.get("offer_code"),
                )
            )
    return state

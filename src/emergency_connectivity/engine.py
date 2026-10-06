"""通信保障编排引擎。

核心策略
--------
1. 把所有未结束请求、有效能力发布与人工特批按时间边界切成互不重叠的时段；
2. 每个时段独立结算：人工特批优先预留，普通请求按任务等级（1 最高）、
   提交时间排序，在主区域与替代区域中寻找能力；高优先级可抢占低优先级，
   被抢占者按替代区域重新安置，否则进入候补；
3. 结算结果与既有授权对账：仍然成立的授权原样保留（用量与授权不可改写），
   消失/被抢占的授权按原因关闭，新分配以不可变授权事件落账；同一请求在
   同一时段被再次分配时产生新版本授权，绝不复用已关闭授权；
4. 灾情升级（优先级变化）、覆盖变化（发布/撤回/特批）、链路中断/恢复、
   到期扫描都会触发重评估。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Iterable, Optional

from .events import (
    AUTH_CLOSED,
    AUTH_GRANTED,
    DECISION_RECORDED,
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
    derived_idem,
)
from .model import Authorization, CapabilityOffer, Decision, ManualOverride, RequestSpec, Window
from .store import EventStore, State, fold

UTC = timezone.utc

# 决策结果
ALLOCATED = "allocated"
WAITLISTED = "waitlisted"
REJECTED = "rejected"
RETAINED = "retained"
SWITCHED = "switched"
DOWNGRADED = "downgraded"
REINSTATED = "reinstated"
TERMINATED = "terminated"


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _idem(prefix: str, payload: dict[str, Any]) -> str:
    body = json.dumps([prefix, payload], ensure_ascii=False, sort_keys=True, default=str)
    return f"{prefix}:" + sha256(body.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class Placement:
    request_code: str
    offer: CapabilityOffer
    source: str  # "rule" | "override"
    override_code: str | None


@dataclass
class _Segment:
    window: Window
    request_codes: list[str]
    offer_codes: list[str]
    overrides: dict[str, str]  # request_code -> override_code


@dataclass
class _Solution:
    segment: _Segment
    placements: list[Placement]
    waitlisted: set[str]
    rejected: set[str]


class EngineError(ValueError):
    """命令违反业务规则。"""


class Orchestrator:
    """对外门面：所有命令都在事件存储的锁内完成「写命令 + 结算」。"""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def state(self) -> State:
        return fold(self.store.events())

    # ================================================================ 命令

    def submit_request(
        self,
        request_code: str,
        area_code: str,
        priority_level: int,
        min_capacity_mbps: int,
        start: datetime,
        end: datetime,
        fallback_areas: Iterable[str] = (),
        *,
        at: datetime | None = None,
        idem: str | None = None,
    ) -> dict[str, Any]:
        at = at or utcnow()
        Window(start, end)
        payload = {
            "request_code": request_code,
            "area_code": area_code,
            "priority_level": priority_level,
            "min_capacity_mbps": min_capacity_mbps,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "fallback_areas": tuple(fallback_areas),
        }
        dup = self._repeated_command(REQUEST_SUBMITTED, "request_code", request_code, payload, strict=True)
        if dup is not None:
            return dup
        event, duplicate = self.store.append(
            REQUEST_SUBMITTED, at, payload, idem or _idem("submit", payload)
        )
        if duplicate:
            return self._duplicate_result(event)
        return self._settle(at, {request_code}, trigger="submit")

    def cancel_request(
        self, request_code: str, *, at: datetime | None = None, idem: str | None = None
    ) -> dict[str, Any]:
        at = at or utcnow()
        rs = self.state().requests.get(request_code)
        if rs is None:
            raise EngineError(f"未知请求 {request_code}")
        if rs.cancelled:
            # 取消与取消并发/多渠道重复：幂等返回，不再结算
            for event in reversed(self.store.events()):
                if event.type == REQUEST_CANCELLED and event.payload.get("request_code") == request_code:
                    return self._duplicate_result(event)
        payload = {"request_code": request_code}
        event, duplicate = self.store.append(
            REQUEST_CANCELLED, at, payload, idem or _idem("cancel", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        return self._settle(at, {request_code}, trigger="cancel")

    def renew_request(
        self,
        request_code: str,
        start: datetime,
        end: datetime,
        *,
        at: datetime | None = None,
        idem: str | None = None,
    ) -> dict[str, Any]:
        """续期：把保障窗口延长/平移到新区间；已发生的授权与用量不动。"""
        at = at or utcnow()
        Window(start, end)
        rs = self.state().requests.get(request_code)
        if rs is None:
            raise EngineError(f"未知请求 {request_code}")
        if rs.cancelled:
            raise EngineError(f"请求 {request_code} 已取消，不能续期")
        payload = {"request_code": request_code, "start": start.isoformat(), "end": end.isoformat()}
        event, duplicate = self.store.append(
            REQUEST_RENEWED, at, payload, idem or _idem("renew", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        return self._settle(at, {request_code}, trigger="renew")

    def change_priority(
        self,
        request_code: str,
        priority_level: int,
        *,
        at: datetime | None = None,
        idem: str | None = None,
    ) -> dict[str, Any]:
        """灾情升级/降级：调整任务等级后立即重新竞争资源。"""
        at = at or utcnow()
        if not 1 <= priority_level <= 5:
            raise EngineError("priority_level 必须在 1..5 之间")
        if request_code not in self.state().requests:
            raise EngineError(f"未知请求 {request_code}")
        payload = {"request_code": request_code, "priority_level": priority_level}
        dup = self._repeated_command(
            PRIORITY_CHANGED, "request_code", request_code, payload, ignore_keys=("at",)
        )
        if dup is not None:
            return dup
        event, duplicate = self.store.append(
            PRIORITY_CHANGED, at, payload, idem or _idem("prio", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        return self._settle(at, {request_code}, trigger="priority")

    def publish_offer(
        self,
        offer_code: str,
        operator: str,
        area_code: str,
        link_code: str,
        capacity_mbps: int,
        start: datetime,
        end: datetime,
        *,
        at: datetime | None = None,
        idem: str | None = None,
    ) -> dict[str, Any]:
        at = at or utcnow()
        Window(start, end)
        payload = {
            "offer_code": offer_code,
            "operator": operator,
            "area_code": area_code,
            "link_code": link_code,
            "capacity_mbps": capacity_mbps,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
        dup = self._repeated_command(OFFER_PUBLISHED, "offer_code", offer_code, payload, strict=True)
        if dup is not None:
            return dup
        event, duplicate = self.store.append(
            OFFER_PUBLISHED, at, payload, idem or _idem("offer", payload)
        )
        if duplicate:
            return self._duplicate_result(event)
        state = self.state()
        affected = {
            code
            for code, rs in state.requests.items()
            if not rs.cancelled and area_code in (rs.spec.area_code, *rs.spec.fallback_areas)
        }
        return self._settle(at, affected, trigger="offer_published")

    def withdraw_offer(
        self, offer_code: str, *, at: datetime | None = None, idem: str | None = None
    ) -> dict[str, Any]:
        at = at or utcnow()
        if offer_code not in self.state().offers:
            raise EngineError(f"未知能力发布 {offer_code}")
        payload = {"offer_code": offer_code}
        event, duplicate = self.store.append(
            OFFER_WITHDRAWN, at, payload, idem or _idem("withdraw", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        state = self.state()
        affected = {
            holder.auth.request_code
            for holder in state.auths.values()
            if not holder.closed and holder.auth.offer_code == offer_code
        }
        return self._settle(at, affected, trigger="offer_withdrawn")

    def interrupt_link(
        self, link_code: str, *, at: datetime | None = None, idem: str | None = None
    ) -> dict[str, Any]:
        at = at or utcnow()
        payload = {"link_code": link_code}
        event, duplicate = self.store.append(
            LINK_INTERRUPTED, at, payload, idem or _idem("linkdown", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        state = self.state()
        affected = {
            holder.auth.request_code
            for holder in state.auths.values()
            if not holder.closed and holder.auth.link_code == link_code
        }
        return self._settle(at, affected, trigger="link_interrupted")

    def restore_link(
        self, link_code: str, *, at: datetime | None = None, idem: str | None = None
    ) -> dict[str, Any]:
        at = at or utcnow()
        payload = {"link_code": link_code}
        event, duplicate = self.store.append(
            LINK_RESTORED, at, payload, idem or _idem("linkup", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        state = self.state()
        affected = {
            code
            for code, rs in state.requests.items()
            if not rs.cancelled
            and any(
                o.link_code == link_code and o.area_code in (rs.spec.area_code, *rs.spec.fallback_areas)
                for o in state.offers.values()
            )
        }
        return self._settle(at, affected, trigger="link_restored")

    def grant_override(
        self,
        override_code: str,
        request_code: str,
        area_code: str,
        link_code: str,
        capacity_mbps: int,
        start: datetime,
        end: datetime,
        owner: str,
        reason: str,
        *,
        at: datetime | None = None,
        idem: str | None = None,
    ) -> dict[str, Any]:
        """人工特批：必须带明确期限与责任人；到期自动回到规则编排。"""
        at = at or utcnow()
        Window(start, end)
        state = self.state()
        if request_code not in state.requests:
            raise EngineError(f"未知请求 {request_code}")
        for ov in state.overrides.values():
            if (
                ov.request_code == request_code
                and ov.override_code not in state.revoked_overrides
                and ov.window.end > at
            ):
                raise EngineError(f"请求 {request_code} 已存在未结束的特批 {ov.override_code}，请先撤销")
        payload = {
            "override_code": override_code,
            "request_code": request_code,
            "area_code": area_code,
            "link_code": link_code,
            "capacity_mbps": capacity_mbps,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "owner": owner,
            "reason": reason,
        }
        event, duplicate = self.store.append(
            OVERRIDE_GRANTED, at, payload, idem or _idem("override", payload)
        )
        if duplicate:
            return self._duplicate_result(event)
        return self._settle(at, {request_code}, trigger="override_granted")

    def revoke_override(
        self, override_code: str, *, at: datetime | None = None, idem: str | None = None
    ) -> dict[str, Any]:
        at = at or utcnow()
        if override_code not in self.state().overrides:
            raise EngineError(f"未知特批 {override_code}")
        payload = {"override_code": override_code}
        event, duplicate = self.store.append(
            OVERRIDE_REVOKED, at, payload, idem or _idem("revokeov", {**payload, "at": at.isoformat()})
        )
        if duplicate:
            return self._duplicate_result(event)
        target = self.state().overrides[override_code].request_code
        return self._settle(at, {target}, trigger="override_revoked")

    def report_usage(
        self,
        request_code: str,
        bytes_transmitted: int,
        at: datetime | None = None,
        auth_code: str | None = None,
        *,
        idem: str | None = None,
    ) -> dict[str, Any]:
        """登记已传输用量。用量只追加，任何后续决定都不能改写。"""
        at = at or utcnow()
        if bytes_transmitted < 0:
            raise EngineError("用量不能为负")
        state = self.state()
        if request_code not in state.requests:
            raise EngineError(f"未知请求 {request_code}")
        if auth_code is None:
            holder = state.active_auth_for(request_code, at)
            if holder is None:
                raise EngineError("该时刻没有生效授权，用量无法对号入座（拒绝挂账）")
            auth_code = holder.auth.auth_code
        else:
            holder = state.auths.get(auth_code)
            if holder is None or holder.auth.request_code != request_code:
                raise EngineError("用量凭证与请求不匹配")
            if not (holder.auth.window.start <= at < holder.auth.window.end):
                raise EngineError("用量时间超出授权窗口")
        payload = {
            "request_code": request_code,
            "bytes_transmitted": bytes_transmitted,
            "auth_code": auth_code,
        }
        event, duplicate = self.store.append(
            USAGE_REPORTED, at, payload, idem or _idem("usage", {**payload, "at": at.isoformat()})
        )
        return {"duplicate": duplicate, "event": event.to_dict()}

    def sweep(self, at: datetime | None = None) -> dict[str, Any]:
        """到期自动回收：关闭过期授权/特批，结束到期请求，并补位候补。"""
        at = at or utcnow()
        return self._settle(at, set(), trigger="sweep")

    # ================================================================ 结算

    def _repeated_command(
        self,
        type_: str,
        id_field: str,
        id_value: str,
        payload: dict[str, Any],
        ignore_keys: tuple[str, ...] = (),
        *,
        strict: bool = False,
    ) -> dict[str, Any] | None:
        """多渠道重复送达处理。

        strict=True（提交/发布类不可变资源）：同标识同内容幂等返回，
        同标识不同内容直接判冲突；strict=False（可连续变更的命令）只与
        最近一条同标识命令比较，允许 3→1→2 这样的连续变更。
        """
        def norm(data: dict[str, Any]) -> str:
            kept = {k: v for k, v in data.items() if k not in ignore_keys}
            return json.dumps(kept, ensure_ascii=False, sort_keys=True, default=str)

        normalized = norm(payload)
        for event in reversed(self.store.events()):
            if event.type != type_ or event.payload.get(id_field) != id_value:
                continue
            if norm(event.payload) == normalized:
                return self._duplicate_result(event)
            if strict:
                raise EngineError(f"{id_field}={id_value} 已存在但内容不同，拒绝冲突命令")
            break
        return None

    def _duplicate_result(self, event) -> dict[str, Any]:  # type: ignore[no-untyped-def]
        return {"duplicate": True, "event": event.to_dict(), "decisions": []}

    def _build_segments(self, state: State, at: datetime) -> list[_Segment]:
        open_requests = [
            rs for rs in state.requests.values() if not rs.cancelled and rs.spec.window.end > at
        ]
        active_offers = [
            o
            for o in state.offers.values()
            if o.offer_code not in state.withdrawn_offers and o.window.end > at
        ]
        active_overrides = [
            o
            for o in state.overrides.values()
            if o.override_code not in state.revoked_overrides and o.window.end > at
        ]

        # 边界只取领域边界（未来的开始/结束）；首段从评估时刻 at 起，
        # 这样已授权区间不会因为再次评估而整体重建。
        points: set[datetime] = set()
        for rs in open_requests:
            if rs.spec.window.start > at:
                points.add(rs.spec.window.start)
            points.add(rs.spec.window.end)
        for offer in active_offers:
            if offer.window.start > at:
                points.add(offer.window.start)
            points.add(offer.window.end)
        for ov in active_overrides:
            if ov.window.start > at:
                points.add(ov.window.start)
            points.add(ov.window.end)
        bounds = [at, *sorted(points)]

        segments: list[_Segment] = []
        for s, e in zip(bounds, bounds[1:]):
            window = Window(start=s, end=e)
            seg = _Segment(window=window, request_codes=[], offer_codes=[], overrides={})
            for rs in open_requests:
                if rs.spec.window.contains(window):
                    seg.request_codes.append(rs.spec.request_code)
            for offer in active_offers:
                if offer.window.contains(window) and offer.link_code not in state.interrupted_links:
                    seg.offer_codes.append(offer.offer_code)
            for ov in active_overrides:
                rs = state.requests.get(ov.request_code)
                if rs is not None and not rs.cancelled and ov.window.contains(window):
                    seg.overrides[ov.request_code] = ov.override_code
            segments.append(seg)
        return segments

    def _solve(self, state: State, seg: _Segment) -> _Solution:
        """单时段求解：返回分配、候补、结构性拒绝。"""
        offers = {code: state.offers[code] for code in seg.offer_codes}
        remaining = {code: offer.capacity_mbps for code, offer in offers.items()}
        placements: list[Placement] = []
        evicted: list[str] = []

        def areas_of(spec: RequestSpec) -> list[str]:
            return [spec.area_code, *spec.fallback_areas]

        def last_link(request_code: str) -> str | None:
            links = [
                h.auth.link_code
                for h in state.auths.values()
                if h.auth.request_code == request_code and not h.closed
            ]
            return links[-1] if links else None

        def free_capacity(request_code: str, need: int, areas: list[str], override: bool) -> bool:
            """通过抢占为请求腾出能力；override=True 时可让任何人让位，否则只让低优先级。"""
            while True:
                victims = [
                    p
                    for p in placements
                    if p.request_code != request_code
                    and p.source == "rule"
                    and p.offer.area_code in areas
                    and p.offer.capacity_mbps >= need
                    and (
                        override
                        or state.requests[p.request_code].spec.priority_level
                        > state.requests[request_code].spec.priority_level
                    )
                ]
                if not victims:
                    return False
                if any(remaining[p.offer.offer_code] >= need for p in victims):
                    return True
                # 同优先级时驱逐后来者，保护更早获批的请求
                victims.sort(
                    key=lambda p: (
                        -state.requests[p.request_code].spec.priority_level,
                        -state.requests[p.request_code].submitted_at.timestamp(),
                        p.request_code,
                    )
                )
                victim = victims[0]
                placements.remove(victim)
                remaining[victim.offer.offer_code] += state.requests[
                    victim.request_code
                ].spec.min_capacity_mbps
                evicted.append(victim.request_code)

        def try_place(
            request_code: str, areas: list[str], prefer_link: str | None
        ) -> Placement | None:
            spec = state.requests[request_code].spec
            need = spec.min_capacity_mbps
            candidates = [
                o for o in offers.values() if o.area_code in areas and o.capacity_mbps >= need
            ]
            candidates.sort(
                key=lambda o: (areas.index(o.area_code), o.link_code != prefer_link, o.link_code, o.offer_code)
            )
            for offer in candidates:
                if remaining[offer.offer_code] >= need:
                    remaining[offer.offer_code] -= need
                    placed = Placement(request_code, offer, "rule", None)
                    placements.append(placed)
                    return placed
            return None

        order = sorted(
            seg.request_codes,
            key=lambda c: (
                0 if c in seg.overrides else 1,
                state.requests[c].spec.priority_level,
                state.requests[c].submitted_at,
                c,
            ),
        )
        waitlisted: set[str] = set()
        for code in order:
            spec = state.requests[code].spec
            need = spec.min_capacity_mbps
            prefer = last_link(code)
            ov_code = seg.overrides.get(code)

            if ov_code is not None:
                ov = state.overrides[ov_code]
                if ov.capacity_mbps >= need:
                    targets = sorted(
                        [
                            o
                            for o in offers.values()
                            if o.link_code == ov.link_code
                            and o.area_code == ov.area_code
                            and o.capacity_mbps >= need
                        ],
                        key=lambda o: o.offer_code,
                    )
                    for offer in targets:
                        if remaining[offer.offer_code] < need:
                            free_capacity(code, need, [ov.area_code], override=True)
                        if remaining[offer.offer_code] >= need:
                            remaining[offer.offer_code] -= need
                            placements.append(Placement(code, offer, "override", ov_code))
                            break
                    if any(p.request_code == code for p in placements):
                        continue

            # 规则分配；不足时抢占低优先级后重试
            if try_place(code, areas_of(spec), prefer) is None:
                if free_capacity(code, need, areas_of(spec), override=False):
                    try_place(code, areas_of(spec), prefer)
            if not any(p.request_code == code for p in placements) and ov_code is not None:
                # 特批链路暂不可用：尝试替代区域的规则能力
                try_place(code, list(spec.fallback_areas), prefer)
            if not any(p.request_code == code for p in placements):
                waitlisted.add(code)

        # 被抢占者按优先级重新安置
        for code in sorted(evicted, key=lambda c: (state.requests[c].spec.priority_level, c)):
            if any(p.request_code == code for p in placements) or code in waitlisted:
                continue
            spec = state.requests[code].spec
            if try_place(code, areas_of(spec), last_link(code)) is None:
                waitlisted.add(code)

        # 结构性拒绝：剩余保障窗口内，任何可接受区域都不存在足够大的物理能力
        rejected: set[str] = set()
        for code in set(waitlisted):
            spec = state.requests[code].spec
            horizon = Window(start=max(seg.window.start, spec.window.start), end=spec.window.end)
            possible = any(
                o.offer_code not in state.withdrawn_offers
                and o.area_code in areas_of(spec)
                and o.capacity_mbps >= spec.min_capacity_mbps
                and o.window.intersection(horizon) is not None
                for o in state.offers.values()
            )
            if not possible:
                rejected.add(code)
        waitlisted -= rejected
        return _Solution(segment=seg, placements=placements, waitlisted=waitlisted, rejected=rejected)

    def _settle(self, at: datetime, targets: set[str], trigger: str) -> dict[str, Any]:
        with self.store.lock:
            state = fold(self.store.events())

            # 1) 自然到期的授权先行关闭
            closes = self._close_expired(state, at)

            # 2) 切时段并求解；授权只物化「当前时段」[at, 下一领域边界)，
            #    未来时段仅用于候补/拒绝判断，边界到达时由 sweep 激活。
            segments = self._build_segments(state, at)
            solutions = [self._solve(state, seg) for seg in segments]
            current = solutions[0] if solutions else None

            # 3) 对账：当前时段不再被期望的开放授权关闭
            closes.update(self._reconcile(state, at, current))

            # 4) 为当前时段新分配落账授权
            state = fold(self.store.events())
            known_auths = set(state.auths)
            grants = self._grant_current(state, at, current)

            # 5) 汇总候补/拒绝并写决策事件（状态未变化的请求不重复出决策）
            state = fold(self.store.events())
            fresh_codes = set(state.auths) - known_auths
            wait_counts: dict[str, int] = {}
            rejected_now: set[str] = set()
            for sol in solutions:
                for code in sol.waitlisted:
                    wait_counts[code] = wait_counts.get(code, 0) + 1
            if current is not None:
                rejected_now = set(current.rejected)
            decisions = self._record_decisions(
                state, at, targets, trigger, closes, grants, fresh_codes,
                wait_counts, rejected_now, current,
            )
            return {
                "duplicate": False,
                "at": at.isoformat(),
                "trigger": trigger,
                "decisions": [_decision_dict(d) for d in decisions],
            }

    def _close_expired(self, state: State, at: datetime) -> dict[str, list[tuple[Authorization, str]]]:
        closes: dict[str, list[tuple[Authorization, str]]] = {}
        for holder in list(state.auths.values()):
            if holder.closed or holder.auth.window.end > at:
                continue
            reason = "override_expired" if holder.auth.source == "override" else "expired"
            idem = derived_idem(AUTH_CLOSED, holder.auth.auth_code, reason)
            self.store.append(
                AUTH_CLOSED, at, {"auth_code": holder.auth.auth_code, "reason": reason}, idem
            )
            closes.setdefault(holder.auth.request_code, []).append((holder.auth, reason))
        return closes

    def _reconcile(self, state: State, at: datetime, current: _Solution | None) -> dict[str, list[tuple[Authorization, str]]]:
        """关闭当前时段不再被期望的开放授权。"""
        wanted: dict[str, tuple[Placement, Window]] = {}
        if current is not None:
            for p in current.placements:
                wanted[p.request_code] = (p, current.segment.window)

        closes: dict[str, list[tuple[Authorization, str]]] = {}
        for holder in list(state.auths.values()):
            auth = holder.auth
            if holder.closed or auth.window.end <= at:
                continue
            if not (auth.window.start <= at < auth.window.end):
                continue  # 尚未生效的未来授权不在本次对账范围
            match = wanted.get(auth.request_code)
            if (
                match is not None
                and match[0].offer.offer_code == auth.offer_code
                and match[0].source == auth.source
            ):
                continue
            reason = self._close_reason(state, auth)
            idem = derived_idem(AUTH_CLOSED, auth.auth_code, reason)
            self.store.append(AUTH_CLOSED, at, {"auth_code": auth.auth_code, "reason": reason}, idem)
            closes.setdefault(auth.request_code, []).append((auth, reason))
        return closes

    def _grant_current(
        self, state: State, at: datetime, current: _Solution | None
    ) -> dict[str, list[Authorization]]:
        """只为当前时段的期望分配物化授权；同一区间再次分配使用新版本号。"""
        grants: dict[str, list[Authorization]] = {}
        if current is None:
            return grants
        for p in sorted(current.placements, key=lambda x: x.request_code):
            covered = next(
                (
                    h
                    for h in state.auths.values()
                    if not h.closed
                    and h.auth.request_code == p.request_code
                    and h.auth.offer_code == p.offer.offer_code
                    and h.auth.source == p.source
                    and h.auth.window.start <= at < h.auth.window.end
                ),
                None,
            )
            if covered is not None:
                grants.setdefault(p.request_code, []).append(covered.auth)
                continue
            version = (
                sum(
                    1
                    for h in state.auths.values()
                    if h.auth.request_code == p.request_code
                    and h.auth.offer_code == p.offer.offer_code
                    and h.auth.source == p.source
                    and h.auth.window.start == at
                )
                + 1
            )
            raw = f"{p.request_code}|{p.offer.offer_code}|{at.isoformat()}|{p.source}|v{version}"
            auth_code = "auth-" + sha256(raw.encode("utf-8")).hexdigest()[:16]
            payload = {
                "auth_code": auth_code,
                "request_code": p.request_code,
                "offer_code": p.offer.offer_code,
                "link_code": p.offer.link_code,
                "area_code": p.offer.area_code,
                "capacity_mbps": state.requests[p.request_code].spec.min_capacity_mbps,
                "start": at.isoformat(),
                "end": current.segment.window.end.isoformat(),
                "source": p.source,
                "override_code": p.override_code,
            }
            self.store.append(
                AUTH_GRANTED, at, payload, derived_idem(AUTH_GRANTED, auth_code)
            )
            holder = fold(self.store.events()).auths[auth_code]
            state.auths[auth_code] = holder
            grants.setdefault(p.request_code, []).append(holder.auth)
        return grants

    def _close_reason(self, state: State, auth: Authorization) -> str:
        rs = state.requests.get(auth.request_code)
        if rs is not None and rs.cancelled:
            return "cancelled"
        if auth.offer_code in state.withdrawn_offers:
            return "offer_withdrawn"
        if auth.link_code in state.interrupted_links:
            return "link_interrupted"
        if auth.source == "override":
            return "override_ended"
        return "preempted"

    def _record_decisions(
        self,
        state: State,
        at: datetime,
        targets: set[str],
        trigger: str,
        closes: dict[str, list[tuple[Authorization, str]]],
        grants: dict[str, list[Authorization]],
        fresh_codes: set[str],
        wait_counts: dict[str, int],
        rejected_now: set[str],
        current: _Solution | None,
    ) -> list[Decision]:
        """只在请求状态真正变化（分配/候补/拒绝/终止及其相互转移）时落一条决策。"""
        last_decision: dict[str, Decision] = {}
        for d in state.decisions:
            last_decision[d.request_code] = d

        active_outcomes = {ALLOCATED, RETAINED, SWITCHED, DOWNGRADED, REINSTATED}
        decisions: list[Decision] = []
        candidates = set(state.requests)
        for code in sorted(candidates):
            rs = state.requests[code]
            prev = last_decision.get(code)
            prev_kind = prev.outcome if prev else None

            # ---- 结算后的当前状态 ----
            if rs.cancelled:
                new_kind, auth = TERMINATED, None
            elif rs.spec.window.end <= at:
                new_kind, auth = TERMINATED, None
            else:
                holder = state.active_auth_for(code, at)
                if holder is not None:
                    new_kind, auth = "active", holder.auth
                elif code in rejected_now:
                    new_kind, auth = REJECTED, None
                else:
                    new_kind, auth = WAITLISTED, None

            fresh = [
                state.auths[c].auth for c in fresh_codes if state.auths[c].auth.request_code == code
            ]
            closed_here = closes.get(code, [])
            outcome, reason, latest = self._transition(
                state, code, at, trigger, prev_kind, prev,
                new_kind, auth, fresh, closed_here, wait_counts.get(code, 0),
                code in targets,
            )
            if outcome is None:
                continue
            payload = {
                "request_code": code,
                "outcome": outcome,
                "reason": reason,
                "area_code": latest.area_code if latest else None,
                "link_code": latest.link_code if latest else None,
                "offer_code": latest.offer_code if latest else None,
            }
            self.store.append(
                DECISION_RECORDED, at, payload,
                derived_idem(DECISION_RECORDED, code, outcome, at, reason),
            )
            decisions.append(
                Decision(
                    request_code=code, at=at, outcome=outcome, reason=reason,
                    area_code=payload["area_code"], link_code=payload["link_code"],
                    offer_code=payload["offer_code"],
                )
            )
        return decisions

    def _override_owner(self, state: State, auth: Authorization) -> str:
        for ov in state.overrides.values():
            if (
                ov.request_code == auth.request_code
                and ov.link_code == auth.link_code
                and ov.window.start <= auth.window.start
                and auth.window.end <= ov.window.end
            ):
                return ov.owner
        return "未知责任人"

    def _transition(
        self,
        state: State,
        code: str,
        at: datetime,
        trigger: str,
        prev_kind: str | None,
        prev: Decision | None,
        new_kind: str,
        auth: Authorization | None,
        fresh: list[Authorization],
        closed: list[tuple[Authorization, str]],
        wait_segments: int,
        is_target: bool,
    ) -> tuple[Optional[str], str, Authorization | None]:
        rs = state.requests[code]
        prev_active = prev_kind in {ALLOCATED, RETAINED, SWITCHED, DOWNGRADED, REINSTATED}
        now_active = new_kind == "active"

        if new_kind == TERMINATED:
            if rs.cancelled:
                if prev_kind == TERMINATED:
                    return None, "", None
                return TERMINATED, "请求方已取消；授权全部回收，已登记的传输用量保持不变", None
            if prev_kind != TERMINATED:
                return TERMINATED, f"保障窗口已于 {rs.spec.window.end.isoformat()} 到期，授权自动回收", None
            return None, "", None

        if now_active:
            newest = sorted(fresh, key=lambda a: a.window.start)[-1] if fresh else None
            if not prev_active and newest is not None:
                if prev_kind in (WAITLISTED, REJECTED):
                    return (
                        REINSTATED,
                        f"因 {trigger} 获得 {newest.area_code}/{newest.link_code}，从候补恢复分配",
                        newest,
                    )
                source = "人工特批" if newest.source == "override" else "应急规则"
                return (
                    ALLOCATED,
                    f"按{source}分配 {newest.area_code}/{newest.link_code}，"
                    f"保障 {rs.spec.min_capacity_mbps}Mbps，"
                    f"覆盖 {newest.window.start.isoformat()} 至 {newest.window.end.isoformat()}",
                    newest,
                )
            if prev_active and newest is not None and closed:
                old, old_reason = closed[0]
                # 规则↔特批来源变化是有意义的切换，优先判定
                if newest.source != old.source:
                    if newest.source == "override":
                        return (
                            SWITCHED,
                            f"人工特批生效（责任人：{self._override_owner(state, newest)}，"
                            f"至 {newest.window.end.isoformat()} 到期），资源锁定到 "
                            f"{newest.area_code}/{newest.link_code}",
                            newest,
                        )
                    return (
                        SWITCHED,
                        f"人工特批到期/撤销后回到规则编排，"
                        f"恢复 {newest.area_code}/{newest.link_code}（触发：{trigger}）",
                        newest,
                    )
                # 同一资源的授权只是跨过时段边界续上：对外状态未变化，不出决策
                if (
                    old.offer_code == newest.offer_code
                    and old.link_code == newest.link_code
                    and old.area_code == newest.area_code
                ):
                    return None, "", newest
                if newest.area_code != old.area_code and newest.area_code in rs.spec.fallback_areas:
                    return (
                        DOWNGRADED,
                        f"原 {old.area_code}/{old.link_code} 因 {old_reason} 释放，"
                        f"改用替代区域 {newest.area_code}/{newest.link_code}（触发：{trigger}）",
                        newest,
                    )
                return (
                    SWITCHED,
                    f"原 {old.area_code}/{old.link_code} 因 {old_reason} 释放，"
                    f"切换到 {newest.area_code}/{newest.link_code}（触发：{trigger}）",
                    newest,
                )
            # 持续占用、本次无新授权：仅当该请求是本次事件的直接受影响对象时，
            # 显式记录一条「保留」及其原因；否则状态未变化不重复记录。
            if is_target and auth is not None:
                return (
                    RETAINED,
                    f"重评估后继续保留在 {auth.area_code}/{auth.link_code}（触发：{trigger}）",
                    auth,
                )
            return None, "", auth

        if new_kind == REJECTED:
            if prev_kind == REJECTED:
                return None, "", None
            areas = "、".join([rs.spec.area_code, *rs.spec.fallback_areas])
            return (
                REJECTED,
                f"可接受区域（{areas}）在剩余保障窗口内都不存在 ≥{rs.spec.min_capacity_mbps}Mbps 的能力",
                None,
            )

        # new_kind == WAITLISTED
        if prev_active and closed:
            old, old_reason = closed[0]
            return (
                WAITLISTED,
                f"{old.area_code}/{old.link_code} 因 {old_reason} 释放；"
                f"当前时段能力不足，进入候补（触发：{trigger}）",
                None,
            )
        if prev_kind in (None, REJECTED):
            return WAITLISTED, f"当前时段能力被更高优先级占用，进入候补（触发：{trigger}）", None
        # 候补→候补：状态未变化
        return None, "", None

    # ================================================================ 解释

    def explain(self, request_code: str, at: datetime | None = None) -> dict[str, Any]:
        """输出某请求在每次变化后被保留/降级/切换/终止的完整事实链。"""
        at = at or utcnow()
        state = self.state()
        rs = state.requests.get(request_code)
        if rs is None:
            raise EngineError(f"未知请求 {request_code}")
        timeline = [
            {
                "seq": i + 1,
                "at": d.at.isoformat(),
                "outcome": d.outcome,
                "area_code": d.area_code,
                "link_code": d.link_code,
                "reason": d.reason,
            }
            for i, d in enumerate(d for d in state.decisions if d.request_code == request_code)
        ]
        auths = sorted(
            [
                {
                    "auth_code": h.auth.auth_code,
                    "offer_code": h.auth.offer_code,
                    "area_code": h.auth.area_code,
                    "link_code": h.auth.link_code,
                    "capacity_mbps": h.auth.capacity_mbps,
                    "start": h.auth.window.start.isoformat(),
                    "end": h.auth.window.end.isoformat(),
                    "source": h.auth.source,
                    "status": "closed" if h.closed else "active",
                    "closed_reason": h.close_reason,
                }
                for h in state.auths.values()
                if h.auth.request_code == request_code
            ],
            key=lambda a: a["start"],
        )
        overrides = [
            {
                "override_code": ov.override_code,
                "link_code": ov.link_code,
                "area_code": ov.area_code,
                "capacity_mbps": ov.capacity_mbps,
                "owner": ov.owner,
                "reason": ov.reason,
                "start": ov.window.start.isoformat(),
                "end": ov.window.end.isoformat(),
                "revoked": ov.override_code in state.revoked_overrides,
            }
            for ov in state.overrides.values()
            if ov.request_code == request_code
        ]
        usage = [
            {"at": u.at.isoformat(), "bytes": u.bytes_transmitted, "auth_code": u.auth_code}
            for u in state.usage
            if u.request_code == request_code
        ]
        active = state.active_auth_for(request_code, at)
        if rs.cancelled or rs.spec.window.end <= at:
            current = TERMINATED
        elif active is not None:
            current = f"active:{active.auth.area_code}/{active.auth.link_code}"
        elif timeline and timeline[-1]["outcome"] == REJECTED:
            current = REJECTED
        else:
            current = WAITLISTED
        return {
            "request_code": request_code,
            "as_of": at.isoformat(),
            "spec": {
                "area_code": rs.spec.area_code,
                "fallback_areas": list(rs.spec.fallback_areas),
                "priority_level": rs.spec.priority_level,
                "min_capacity_mbps": rs.spec.min_capacity_mbps,
                "start": rs.spec.window.start.isoformat(),
                "end": rs.spec.window.end.isoformat(),
                "cancelled": rs.cancelled,
            },
            "current_status": current,
            "total_bytes_transmitted": state.usage_total(request_code),
            "timeline": timeline,
            "authorizations": auths,
            "overrides": overrides,
            "usage": usage,
        }


def _decision_dict(d: Decision) -> dict[str, Any]:
    return {
        "request_code": d.request_code,
        "at": d.at.isoformat(),
        "outcome": d.outcome,
        "reason": d.reason,
        "area_code": d.area_code,
        "link_code": d.link_code,
        "offer_code": d.offer_code,
    }

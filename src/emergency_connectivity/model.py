"""应急通信保障编排的领域值对象。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True, slots=True)
class Window:
    """半开时间区间 [start, end)，统一使用带时区的 UTC 时间。"""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("时间必须携带时区")
        if self.end <= self.start:
            raise ValueError("结束时间必须晚于开始时间")

    @property
    def duration_seconds(self) -> int:
        return int((self.end - self.start).total_seconds())

    def contains(self, other: "Window") -> bool:
        return self.start <= other.start and other.end <= self.end

    def intersection(self, other: "Window") -> Optional["Window"]:
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        if end <= start:
            return None
        return Window(start=start, end=end)


@dataclass(frozen=True, slots=True)
class CapabilityOffer:
    """运营方按覆盖区域与时段发布的一段卫星链路能力。"""

    offer_code: str
    operator: str
    area_code: str
    link_code: str
    window: Window
    capacity_mbps: int

    def __post_init__(self) -> None:
        for key in ("offer_code", "operator", "area_code", "link_code"):
            value = getattr(self, key)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{key} 不能为空")
        if self.capacity_mbps < 1:
            raise ValueError("capacity_mbps 必须大于零")


@dataclass(frozen=True, slots=True)
class RequestSpec:
    """请求方提交的保障请求：地点、等级、最低能力、时长与替代范围。

    priority_level 取 1..5，1 为最高（对应急救护/指挥），5 为普通安置点。
    fallback_areas 为按意愿排序的备用覆盖区域。
    """

    request_code: str
    area_code: str
    priority_level: int
    min_capacity_mbps: int
    window: Window
    fallback_areas: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.request_code.strip() or not self.area_code.strip():
            raise ValueError("请求编号与区域编号不能为空")
        if not 1 <= self.priority_level <= 5:
            raise ValueError("priority_level 必须在 1..5 之间")
        if self.min_capacity_mbps < 1:
            raise ValueError("min_capacity_mbps 必须大于零")


@dataclass(frozen=True, slots=True)
class ManualOverride:
    """人工特批：必须有明确期限与责任人，到期自动回到规则编排。"""

    override_code: str
    request_code: str
    area_code: str
    link_code: str
    capacity_mbps: int
    window: Window
    owner: str
    reason: str

    def __post_init__(self) -> None:
        for key in ("override_code", "request_code", "area_code", "link_code", "owner"):
            if not isinstance(getattr(self, key), str) or not getattr(self, key).strip():
                raise ValueError(f"{key} 不能为空")
        if self.capacity_mbps < 1:
            raise ValueError("capacity_mbps 必须大于零")
        if not self.reason.strip():
            raise ValueError("特批必须写明原因")


@dataclass(frozen=True, slots=True)
class Authorization:
    """一次资源授权的不可变记录；重评估只会新增/关闭授权，绝不修改它。"""

    auth_code: str
    request_code: str
    offer_code: str
    link_code: str
    area_code: str
    capacity_mbps: int
    window: Window
    source: str  # "rule" 或 "override"
    decided_at: datetime


@dataclass(frozen=True, slots=True)
class Decision:
    """某次变化后针对单个请求的评估结论与事实依据。"""

    request_code: str
    at: datetime
    outcome: str  # allocated/waitlisted/rejected/retained/switched/downgraded/reinstated/terminated
    reason: str
    area_code: str | None = None
    link_code: str | None = None
    offer_code: str | None = None


@dataclass(frozen=True, slots=True)
class UsageRecord:
    """已传输用量凭证，只追加；任何后续终止/切换都不得改写。"""

    request_code: str
    at: datetime
    bytes_transmitted: int
    auth_code: str | None

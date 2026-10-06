"""应急通信保障编排 HTTP API（仅依赖标准库）。

资源与动作
----------
POST   /requests                          提交保障请求
POST   /requests/{code}/cancel            取消
POST   /requests/{code}/renew             续期（新保障窗口）
POST   /requests/{code}/priority          灾情升级/降级
POST   /offers                            运营方发布能力
POST   /offers/{code}/withdraw            撤回能力
POST   /links/{code}/interrupt            链路中断
POST   /links/{code}/restore              链路恢复
POST   /overrides                         人工特批（期限+责任人）
POST   /overrides/{code}/revoke           撤销特批
POST   /usage                             登记已传输用量
POST   /sweep                             到期回收/候补补位
GET    /requests/{code}/explain           解释每次变化的原因
GET    /events                            追加事件审计流

所有写操作接受 ``idem`` 字段用于跨渠道幂等；时间统一为带时区 ISO-8601。
"""

from __future__ import annotations

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

from .engine import EngineError, Orchestrator
from .store import EventStore, fold


def _dt(raw: str, field: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw)
    except (TypeError, ValueError) as exc:
        raise EngineError(f"{field} 必须是 ISO-8601 时间") from exc
    if value.tzinfo is None:
        raise EngineError(f"{field} 必须携带时区")
    return value


def _require(body: dict[str, Any], *keys: str) -> tuple:
    missing = [key for key in keys if key not in body]
    if missing:
        raise EngineError(f"缺少必填字段：{', '.join(missing)}")
    return tuple(body[key] for key in keys)


def create_handler(store: EventStore) -> type[BaseHTTPRequestHandler]:
    engine = Orchestrator(store)

    class Handler(BaseHTTPRequestHandler):
        server_version = "EmergencyConnectivity/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默访问日志
            return

        def _send(self, status: int, data: Any) -> None:
            raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            try:
                data = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise EngineError("请求体不是合法 JSON") from exc
            if not isinstance(data, dict):
                raise EngineError("请求体必须是 JSON 对象")
            return data

        def _handle(self, fn: Callable[[], Any]) -> None:
            try:
                self._send(200, fn())
            except EngineError as exc:
                self._send(422, {"error": str(exc)})
            except KeyError as exc:
                self._send(404, {"error": f"资源不存在：{exc.args[0]}"})

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.rstrip("/") or "/"
            if path == "/events":
                self._handle(lambda: {"events": [e.to_dict() for e in store.events()]})
                return
            if path.startswith("/requests/") and path.endswith("/explain"):
                code = path[len("/requests/") : -len("/explain")]
                self._handle(lambda: engine.explain(code))
                return
            if path == "/state":
                state = engine.state()
                self._handle(
                    lambda: {
                        "requests": {
                            c: {
                                "area_code": rs.spec.area_code,
                                "priority_level": rs.spec.priority_level,
                                "window": [rs.spec.window.start.isoformat(), rs.spec.window.end.isoformat()],
                                "cancelled": rs.cancelled,
                            }
                            for c, rs in state.requests.items()
                        },
                        "active_authorizations": [
                            {
                                "auth_code": h.auth.auth_code,
                                "request_code": h.auth.request_code,
                                "area_code": h.auth.area_code,
                                "link_code": h.auth.link_code,
                                "end": h.auth.window.end.isoformat(),
                                "source": h.auth.source,
                            }
                            for h in state.auths.values()
                            if not h.closed
                        ],
                        "interrupted_links": sorted(state.interrupted_links),
                    }
                )
                return
            self._send(404, {"error": f"未知路径 {path}"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.rstrip("/") or "/"
            routes: dict[str, Callable[[dict[str, Any]], Any]] = {
                "/requests": self._submit,
                "/offers": self._publish,
                "/usage": self._usage,
                "/sweep": lambda b: engine.sweep(_dt(b["at"], "at") if b.get("at") else None),
            }
            if path in routes:
                self._handle(lambda: routes[path](self._body()))
                return

            suffixes = [
                ("/requests/", "/cancel", self._cancel),
                ("/requests/", "/renew", self._renew),
                ("/requests/", "/priority", self._priority),
                ("/offers/", "/withdraw", self._withdraw),
                ("/links/", "/interrupt", self._interrupt),
                ("/links/", "/restore", self._restore),
                ("/overrides/", "/revoke", self._revoke_override),
            ]
            for prefix, suffix, fn in suffixes:
                if path.startswith(prefix) and path.endswith(suffix):
                    code = path[len(prefix) : -len(suffix)]
                    self._handle(lambda fn=fn, code=code: fn(code, self._body()))
                    return
            if path == "/overrides":
                self._handle(lambda: self._override(self._body()))
                return
            self._send(404, {"error": f"未知路径 {path}"})

        # ------------------------------------------------------------ 动作
        def _at(self, body: dict[str, Any]):
            return _dt(body["at"], "at") if body.get("at") else None

        def _submit(self, body: dict[str, Any]) -> Any:
            _require(
                body, "request_code", "area_code", "priority_level",
                "min_capacity_mbps", "start", "end",
            )
            return engine.submit_request(
                body["request_code"], body["area_code"], int(body["priority_level"]),
                int(body["min_capacity_mbps"]),
                _dt(body["start"], "start"), _dt(body["end"], "end"),
                tuple(body.get("fallback_areas", ())),
                at=self._at(body), idem=body.get("idem"),
            )

        def _cancel(self, code: str, body: dict[str, Any]) -> Any:
            return engine.cancel_request(code, at=self._at(body), idem=body.get("idem"))

        def _renew(self, code: str, body: dict[str, Any]) -> Any:
            _require(body, "start", "end")
            return engine.renew_request(
                code, _dt(body["start"], "start"), _dt(body["end"], "end"),
                at=self._at(body), idem=body.get("idem"),
            )

        def _priority(self, code: str, body: dict[str, Any]) -> Any:
            _require(body, "priority_level")
            return engine.change_priority(
                code, int(body["priority_level"]), at=self._at(body), idem=body.get("idem"),
            )

        def _publish(self, body: dict[str, Any]) -> Any:
            _require(
                body, "offer_code", "operator", "area_code", "link_code",
                "capacity_mbps", "start", "end",
            )
            return engine.publish_offer(
                body["offer_code"], body["operator"], body["area_code"], body["link_code"],
                int(body["capacity_mbps"]),
                _dt(body["start"], "start"), _dt(body["end"], "end"),
                at=self._at(body), idem=body.get("idem"),
            )

        def _withdraw(self, code: str, body: dict[str, Any]) -> Any:
            return engine.withdraw_offer(code, at=self._at(body), idem=body.get("idem"))

        def _interrupt(self, code: str, body: dict[str, Any]) -> Any:
            return engine.interrupt_link(code, at=self._at(body), idem=body.get("idem"))

        def _restore(self, code: str, body: dict[str, Any]) -> Any:
            return engine.restore_link(code, at=self._at(body), idem=body.get("idem"))

        def _override(self, body: dict[str, Any]) -> Any:
            _require(
                body, "override_code", "request_code", "area_code", "link_code",
                "capacity_mbps", "start", "end", "owner", "reason",
            )
            return engine.grant_override(
                body["override_code"], body["request_code"], body["area_code"],
                body["link_code"], int(body["capacity_mbps"]),
                _dt(body["start"], "start"), _dt(body["end"], "end"),
                body["owner"], body["reason"],
                at=self._at(body), idem=body.get("idem"),
            )

        def _revoke_override(self, code: str, body: dict[str, Any]) -> Any:
            return engine.revoke_override(code, at=self._at(body), idem=body.get("idem"))

        def _usage(self, body: dict[str, Any]) -> Any:
            _require(body, "request_code", "bytes_transmitted")
            return engine.report_usage(
                body["request_code"], int(body["bytes_transmitted"]),
                at=self._at(body), auth_code=body.get("auth_code"), idem=body.get("idem"),
            )

    return Handler


def serve(host: str = "127.0.0.1", port: int = 8080, store: EventStore | None = None) -> None:
    store = store or EventStore()
    httpd = ThreadingHTTPServer((host, port), create_handler(store))
    print(f"应急通信保障编排服务监听 http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

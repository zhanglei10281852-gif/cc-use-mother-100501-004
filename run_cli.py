"""应急通信保障编排命令行冒烟入口。"""

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from emergency_connectivity import ConnectivityRequest


def main() -> None:
    item = ConnectivityRequest(request_code='request-code-001', area_code='area-code-001', priority_level=1, state='state-001')
    print(json.dumps({"item": asdict(item), "fingerprint": item.fingerprint()}, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

"""应急通信保障编排命令行冒烟入口：运行确定性端到端剧情。"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from emergency_connectivity.demo import main  # noqa: E402

if __name__ == "__main__":
    main()

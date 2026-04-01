import argparse
import os
from types import SimpleNamespace

import turning_point_btc as base


DEFAULT_HORIZONS = [1, 6, 18]


def parse_args():
    parser = argparse.ArgumentParser(description="BTC 4h 拐点信号脚本")
    parser.add_argument("--mode", choices=["scan", "backtest", "monitor"], default="backtest")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--limit", type=int, default=300, help="scan/monitor 模式最近K线数量")
    parser.add_argument("--min-score", type=int, default=5, help="4h 拐点信号最低分")
    parser.add_argument("--cooldown", type=int, default=3, help="同方向信号冷却K线数")
    parser.add_argument("--recent", type=int, default=20, help="scan 模式输出最近信号数")
    parser.add_argument("--show-all", action="store_true", help="打印全部信号")
    parser.add_argument("--poll-seconds", type=int, default=900, help="monitor 模式轮询秒数")
    parser.add_argument("--once", action="store_true", help="monitor 模式只执行一次")
    parser.add_argument(
        "--state-file",
        default="turning_point_monitor_4h_state.json",
        help="monitor 模式本地状态文件",
    )
    parser.add_argument(
        "--webhook",
        default=os.getenv("DINGTALK_WEBHOOK", "").strip(),
        help="钉钉 webhook，默认读取 DINGTALK_WEBHOOK",
    )
    parser.add_argument(
        "--secret",
        default=os.getenv("DINGTALK_SECRET", "").strip(),
        help="钉钉签名 secret，默认读取 DINGTALK_SECRET",
    )
    parser.add_argument(
        "--horizons",
        default="1,6,18",
        help="回测前瞻K线步数，逗号分隔，默认 1,6,18；在 4h 周期下对应 4h/24h/72h",
    )
    return parser.parse_args()


def build_base_args(args):
    return SimpleNamespace(
        mode=args.mode,
        symbol=args.symbol,
        interval="4h",
        days=args.days,
        limit=args.limit,
        min_score=args.min_score,
        cooldown=args.cooldown,
        recent=args.recent,
        show_all=args.show_all,
        poll_seconds=args.poll_seconds,
        once=args.once,
        state_file=args.state_file,
        webhook=args.webhook,
        secret=args.secret,
    )


def main():
    args = parse_args()
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    if not horizons:
        horizons = DEFAULT_HORIZONS

    base_args = build_base_args(args)
    if args.mode == "scan":
        base.run_scan(base_args, horizons)
    elif args.mode == "monitor":
        base.run_monitor(base_args, horizons)
    else:
        base.run_backtest(base_args, horizons)


if __name__ == "__main__":
    main()


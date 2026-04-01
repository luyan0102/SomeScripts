import argparse
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import time
import urllib.parse

import numpy as np
import pandas as pd
import requests


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000
DEFAULT_HORIZONS = [4, 24, 72]


def parse_args():
    parser = argparse.ArgumentParser(description="BTC 拐点信号脚本")
    parser.add_argument("--mode", choices=["scan", "backtest", "monitor"], default="backtest")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--limit", type=int, default=300, help="scan 模式最近K线数量")
    parser.add_argument("--min-score", type=int, default=5, help="拐点信号最低分")
    parser.add_argument("--cooldown", type=int, default=6, help="同方向信号冷却K线数")
    parser.add_argument("--recent", type=int, default=20, help="scan 模式输出最近信号数")
    parser.add_argument("--show-all", action="store_true", help="打印全部信号")
    parser.add_argument("--poll-seconds", type=int, default=300, help="monitor 模式轮询秒数")
    parser.add_argument("--once", action="store_true", help="monitor 模式只执行一次")
    parser.add_argument(
        "--state-file",
        default="turning_point_monitor_state.json",
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
        default="4,24,72",
        help="回测前瞻K线步数，逗号分隔，默认 4,24,72",
    )
    return parser.parse_args()


def interval_to_milliseconds(interval):
    unit = interval[-1]
    value = int(interval[:-1])
    unit_map = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }
    if unit not in unit_map:
        raise ValueError(f"不支持的周期: {interval}")
    return value * unit_map[unit]


def fetch_recent_klines(symbol, interval, limit):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
    response.raise_for_status()
    return klines_to_dataframe(response.json())


def fetch_historical_klines(symbol, interval, start_ms, end_ms):
    rows = []
    step_ms = interval_to_milliseconds(interval)
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": MAX_LIMIT,
        }
        response = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        rows.extend(batch)
        last_open_time = int(batch[-1][0])
        current_start = last_open_time + step_ms

        if len(batch) < MAX_LIMIT:
            break

    if not rows:
        raise ValueError("未拉到任何K线数据")

    df = klines_to_dataframe(rows)
    df = df[df["close_time"] <= end_ms].reset_index(drop=True)
    if df.empty:
        raise ValueError("过滤未收盘K线后没有可用数据")
    return df


def klines_to_dataframe(rows):
    df = pd.DataFrame(
        rows,
        columns=[
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "qav",
            "trades",
            "tbbav",
            "tbqav",
            "ignore",
        ],
    )
    df = df.astype(float)
    df = df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df


def filter_closed_klines(df, now_ms=None):
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return df[df["close_time"] <= now_ms].reset_index(drop=True)


def prepare_indicators(df):
    df = df.copy()
    df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))
    df["rsi14"] = df["rsi14"].fillna(50)

    df["body"] = (df["close"] - df["open"]).abs()
    df["range"] = (df["high"] - df["low"]).replace(0, np.nan)
    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_wick"] = df[["open", "close"]].min(axis=1) - df["low"]
    df["recent_drop_pct"] = df["close"].pct_change(12)
    df["recent_rise_pct"] = df["close"].pct_change(12)

    return add_confirmed_swings(df)


def add_confirmed_swings(df, left=3, right=3):
    n = len(df)
    swing_high = np.zeros(n, dtype=bool)
    swing_low = np.zeros(n, dtype=bool)
    swing_high_confirmed_at = np.full(n, -1, dtype=int)
    swing_low_confirmed_at = np.full(n, -1, dtype=int)

    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()

    for i in range(left, n - right):
        high_window = highs[i - left : i + right + 1]
        low_window = lows[i - left : i + right + 1]

        if highs[i] == high_window.max():
            swing_high[i] = True
            swing_high_confirmed_at[i] = i + right

        if lows[i] == low_window.min():
            swing_low[i] = True
            swing_low_confirmed_at[i] = i + right

    df["swing_high"] = swing_high
    df["swing_low"] = swing_low
    df["swing_high_confirmed_at"] = swing_high_confirmed_at
    df["swing_low_confirmed_at"] = swing_low_confirmed_at
    return df


def latest_confirmed_swing_index(df, idx, side, max_age=120):
    swing_col = f"swing_{side}"
    confirmed_col = f"swing_{side}_confirmed_at"
    mask = (
        df[swing_col]
        & (df[confirmed_col] >= 0)
        & (df[confirmed_col] <= idx)
        & (df.index < idx)
        & (df.index >= max(0, idx - max_age))
    )
    candidates = df.index[mask]
    if len(candidates) == 0:
        return None
    return int(candidates[-1])


def detect_bullish_engulfing(df, idx):
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    curr = df.iloc[idx]
    return (
        prev["close"] < prev["open"]
        and curr["close"] > curr["open"]
        and curr["close"] > prev["open"]
        and curr["open"] < prev["close"]
    )


def detect_bearish_engulfing(df, idx):
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    curr = df.iloc[idx]
    return (
        prev["close"] > prev["open"]
        and curr["close"] < curr["open"]
        and curr["open"] > prev["close"]
        and curr["close"] < prev["open"]
    )


def detect_bullish_rejection(df, idx):
    row = df.iloc[idx]
    if pd.isna(row["range"]) or row["range"] == 0:
        return False
    return (
        row["lower_wick"] >= row["body"] * 1.5
        and row["lower_wick"] >= row["range"] * 0.35
        and row["close"] >= row["low"] + row["range"] * 0.55
    )


def detect_bearish_rejection(df, idx):
    row = df.iloc[idx]
    if pd.isna(row["range"]) or row["range"] == 0:
        return False
    return (
        row["upper_wick"] >= row["body"] * 1.5
        and row["upper_wick"] >= row["range"] * 0.35
        and row["close"] <= row["high"] - row["range"] * 0.55
    )


def analyze_turning_point_bar(df, idx, min_score):
    if idx < 20:
        return None

    row = df.iloc[idx]
    prev = df.iloc[idx - 1]
    atr = row["atr14"]
    if pd.isna(atr) or atr <= 0:
        return None

    last_low_idx = latest_confirmed_swing_index(df, idx, "low")
    last_high_idx = latest_confirmed_swing_index(df, idx, "high")

    last_low_price = df["low"].iloc[last_low_idx] if last_low_idx is not None else None
    last_high_price = df["high"].iloc[last_high_idx] if last_high_idx is not None else None

    stretch_down = row["close"] < row["ema20"] - 1.2 * atr
    stretch_up = row["close"] > row["ema20"] + 1.2 * atr
    oversold = row["rsi14"] < 35
    overbought = row["rsi14"] > 65

    near_support = (
        last_low_price is not None
        and row["low"] <= last_low_price + 0.6 * atr
        and row["close"] >= last_low_price - 0.8 * atr
    )
    near_resistance = (
        last_high_price is not None
        and row["high"] >= last_high_price - 0.6 * atr
        and row["close"] <= last_high_price + 0.8 * atr
    )

    bullish_divergence = (
        last_low_idx is not None
        and row["low"] < df["low"].iloc[last_low_idx] * 0.999
        and row["rsi14"] > df["rsi14"].iloc[last_low_idx] + 3
    )
    bearish_divergence = (
        last_high_idx is not None
        and row["high"] > df["high"].iloc[last_high_idx] * 1.001
        and row["rsi14"] < df["rsi14"].iloc[last_high_idx] - 3
    )

    bullish_rejection = detect_bullish_rejection(df, idx)
    bearish_rejection = detect_bearish_rejection(df, idx)
    bullish_engulfing = detect_bullish_engulfing(df, idx)
    bearish_engulfing = detect_bearish_engulfing(df, idx)
    bullish_confirm = row["close"] > prev["high"]
    bearish_confirm = row["close"] < prev["low"]

    long_score = 0
    long_reasons = []
    if stretch_down:
        long_score += 2
        long_reasons.append("ATR超跌")
    if near_support:
        long_score += 2
        long_reasons.append("接近结构低点")
    if bullish_divergence:
        long_score += 2
        long_reasons.append("RSI底背离")
    if oversold:
        long_score += 1
        long_reasons.append("RSI超卖")
    if bullish_rejection:
        long_score += 1
        long_reasons.append("下影拒绝")
    if bullish_engulfing:
        long_score += 1
        long_reasons.append("看涨吞没")
    if bullish_confirm:
        long_score += 1
        long_reasons.append("向上确认")
    if row["rsi14"] > prev["rsi14"]:
        long_score += 1
        long_reasons.append("RSI回升")

    short_score = 0
    short_reasons = []
    if stretch_up:
        short_score += 2
        short_reasons.append("ATR超涨")
    if near_resistance:
        short_score += 2
        short_reasons.append("接近结构高点")
    if bearish_divergence:
        short_score += 2
        short_reasons.append("RSI顶背离")
    if overbought:
        short_score += 1
        short_reasons.append("RSI超买")
    if bearish_rejection:
        short_score += 1
        short_reasons.append("上影拒绝")
    if bearish_engulfing:
        short_score += 1
        short_reasons.append("看跌吞没")
    if bearish_confirm:
        short_score += 1
        short_reasons.append("向下确认")
    if row["rsi14"] < prev["rsi14"]:
        short_score += 1
        short_reasons.append("RSI回落")

    long_trigger = bullish_rejection or bullish_engulfing or bullish_confirm
    short_trigger = bearish_rejection or bearish_engulfing or bearish_confirm
    long_context = stretch_down or near_support or bullish_divergence or oversold
    short_context = stretch_up or near_resistance or bearish_divergence or overbought

    long_signal = long_context and long_trigger and long_score >= min_score
    short_signal = short_context and short_trigger and short_score >= min_score

    if long_signal and short_signal:
        if long_score > short_score:
            short_signal = False
        elif short_score > long_score:
            long_signal = False
        else:
            long_signal = False
            short_signal = False

    return {
        "long_signal": long_signal,
        "short_signal": short_signal,
        "long_score": long_score,
        "short_score": short_score,
        "long_reasons": long_reasons,
        "short_reasons": short_reasons,
        "rsi14": float(row["rsi14"]),
        "atr14": float(atr),
        "ema20": float(row["ema20"]),
        "stretch_down": stretch_down,
        "stretch_up": stretch_up,
        "near_support": near_support,
        "near_resistance": near_resistance,
        "bullish_divergence": bullish_divergence,
        "bearish_divergence": bearish_divergence,
    }


def build_signal_records(df, min_score, cooldown, horizons):
    records = []
    last_signal_idx = {"long": -10_000, "short": -10_000}

    for idx in range(len(df)):
        analysis = analyze_turning_point_bar(df, idx, min_score)
        if analysis is None:
            continue

        candidates = []
        if analysis["long_signal"] and idx - last_signal_idx["long"] > cooldown:
            candidates.append(("long", analysis["long_score"], analysis["long_reasons"]))
        if analysis["short_signal"] and idx - last_signal_idx["short"] > cooldown:
            candidates.append(("short", analysis["short_score"], analysis["short_reasons"]))

        if not candidates:
            continue

        if len(candidates) == 2:
            candidates = [max(candidates, key=lambda item: item[1])]

        side, score, reasons = candidates[0]
        last_signal_idx[side] = idx
        records.append(build_signal_record(df, idx, side, score, reasons, horizons))

    return pd.DataFrame(records)


def build_signal_record(df, idx, side, score, reasons, horizons):
    row = df.iloc[idx]
    entry_price = row["close"]
    direction = 1 if side == "long" else -1
    record = {
        "time": row["dt"],
        "side": side,
        "score": score,
        "price": entry_price,
        "rsi14": float(row["rsi14"]),
        "reasons": " + ".join(reasons),
    }

    for horizon in horizons:
        future_idx = idx + horizon
        column = f"ret_{horizon}"
        if future_idx >= len(df):
            record[column] = None
            continue
        future_price = df.iloc[future_idx]["close"]
        raw_return = future_price / entry_price - 1
        record[column] = direction * raw_return

    return record


def summarize_backtest(signals_df, horizons):
    if signals_df.empty:
        return {
            "total": 0,
            "long_count": 0,
            "short_count": 0,
            "score_mix": pd.Series(dtype="int64"),
            "reason_mix": pd.Series(dtype="int64"),
            "per_side": {},
        }

    per_side = {}
    for side in ["long", "short"]:
        side_df = signals_df[signals_df["side"] == side]
        horizon_stats = {}
        for horizon in horizons:
            column = f"ret_{horizon}"
            valid = side_df[column].dropna()
            if valid.empty:
                horizon_stats[horizon] = {
                    "count": 0,
                    "avg_return": None,
                    "median_return": None,
                    "win_rate": None,
                }
                continue
            horizon_stats[horizon] = {
                "count": int(valid.count()),
                "avg_return": float(valid.mean()),
                "median_return": float(valid.median()),
                "win_rate": float((valid > 0).mean()),
            }
        per_side[side] = {
            "count": int(len(side_df)),
            "score_mix": side_df["score"].value_counts().sort_index(),
            "reason_mix": side_df["reasons"].value_counts(),
            "horizon_stats": horizon_stats,
        }

    return {
        "total": int(len(signals_df)),
        "long_count": int((signals_df["side"] == "long").sum()),
        "short_count": int((signals_df["side"] == "short").sum()),
        "score_mix": signals_df["score"].value_counts().sort_index(),
        "reason_mix": signals_df["reasons"].value_counts(),
        "per_side": per_side,
    }


def print_signal_table(signals_df, horizons, limit=None):
    if signals_df.empty:
        print("没有检测到满足条件的拐点信号。")
        return

    cols = ["time", "side", "score", "price", "rsi14", "reasons"] + [f"ret_{h}" for h in horizons]
    table = signals_df[cols].copy()
    table["time"] = table["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    table["price"] = table["price"].map(lambda value: f"{value:.2f}")
    table["rsi14"] = table["rsi14"].map(lambda value: f"{value:.2f}")

    for column in [f"ret_{h}" for h in horizons]:
        table[column] = table[column].apply(lambda value: "-" if pd.isna(value) else f"{value * 100:.2f}%")

    if limit is not None:
        table = table.tail(limit)

    print(table.to_string(index=False))


def load_state(state_file):
    if not os.path.exists(state_file):
        return {}
    try:
        with open(state_file, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(state_file, state):
    with open(state_file, "w", encoding="utf-8") as file_obj:
        json.dump(state, file_obj, ensure_ascii=False, indent=2)


def build_dingtalk_url(webhook, secret):
    if not secret:
        return webhook
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode("utf-8")
    string_to_sign = f"{timestamp}\n{secret}"
    string_to_sign_enc = string_to_sign.encode("utf-8")
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"{webhook}&timestamp={timestamp}&sign={sign}"


def send_dingtalk_message(webhook, secret, message):
    if not webhook:
        raise ValueError("未配置钉钉 webhook，请传 --webhook 或设置 DINGTALK_WEBHOOK")
    url = build_dingtalk_url(webhook, secret)
    response = requests.post(
        url,
        json={"msgtype": "text", "text": {"content": message}},
        timeout=10,
    )
    response.raise_for_status()


def build_live_signal(signal_row, last_closed_row, args):
    if signal_row is None:
        return None
    signal_time = signal_row["time"]
    close_time = pd.to_datetime(int(last_closed_row["close_time"]), unit="ms", utc=True)
    signal_key = (
        f"{args.symbol}:{args.interval}:{signal_time.strftime('%Y-%m-%d %H:%M:%S')}:"
        f"{signal_row['side']}:{signal_row['score']}"
    )
    return {
        "signal_key": signal_key,
        "signal_time": signal_time,
        "close_time": close_time,
        "symbol": args.symbol,
        "interval": args.interval,
        "side": signal_row["side"],
        "score": int(signal_row["score"]),
        "price": float(signal_row["price"]),
        "rsi14": float(signal_row["rsi14"]),
        "reasons": signal_row["reasons"],
    }


def latest_closed_bar_signal(df, args, horizons):
    df = filter_closed_klines(df)
    if len(df) < 30:
        return None, None

    df = prepare_indicators(df)
    signals_df = build_signal_records(df, args.min_score, args.cooldown, horizons)
    last_closed_row = df.iloc[-1]
    if signals_df.empty:
        return None, last_closed_row

    latest_signal = signals_df.iloc[-1]
    latest_signal_time = int(latest_signal["time"].timestamp() * 1000)
    latest_bar_time = int(last_closed_row["time"])

    if latest_signal_time != latest_bar_time:
        return None, last_closed_row

    return build_live_signal(latest_signal, last_closed_row, args), last_closed_row


def format_monitor_message(signal):
    side_text = "做多拐点" if signal["side"] == "long" else "做空拐点"
    icon = "📈" if signal["side"] == "long" else "📉"
    return (
        f"{icon} 拐点信号\n"
        f"标的: {signal['symbol']}\n"
        f"周期: {signal['interval']}\n"
        f"方向: {side_text}\n"
        f"信号时间(UTC): {signal['signal_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"价格: {signal['price']:.2f}\n"
        f"评分: {signal['score']}\n"
        f"RSI14: {signal['rsi14']:.2f}\n"
        f"触发条件: {signal['reasons']}"
    )


def run_monitor_once(args, horizons):
    df = fetch_recent_klines(args.symbol, args.interval, args.limit)
    signal, last_closed_row = latest_closed_bar_signal(df, args, horizons)
    closed_dt = pd.to_datetime(int(last_closed_row["time"]), unit="ms", utc=True) if last_closed_row is not None else None

    if last_closed_row is None:
        print("可用已收盘K线不足，跳过。")
        return

    state = load_state(args.state_file)
    state["last_checked_bar_time"] = closed_dt.strftime("%Y-%m-%d %H:%M:%S")

    if signal is None:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] 最近已收盘K线无新拐点信号")
        save_state(args.state_file, state)
        return

    last_sent_key = state.get("last_sent_signal_key")
    if signal["signal_key"] == last_sent_key:
        print(
            f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
            f"信号已发送，跳过: {signal['signal_key']}"
        )
        save_state(args.state_file, state)
        return

    message = format_monitor_message(signal)
    send_dingtalk_message(args.webhook, args.secret, message)
    state["last_sent_signal_key"] = signal["signal_key"]
    state["last_sent_signal_time"] = signal["signal_time"].strftime("%Y-%m-%d %H:%M:%S")
    state["last_sent_side"] = signal["side"]
    state["last_sent_score"] = signal["score"]
    save_state(args.state_file, state)
    print(
        f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
        f"已发送信号: {signal['side']} score={signal['score']} {signal['reasons']}"
    )


def run_monitor(args, horizons):
    print(
        f"开始监控 {args.symbol} {args.interval}，轮询间隔 {args.poll_seconds} 秒，"
        f"最低分 {args.min_score}。"
    )
    while True:
        try:
            run_monitor_once(args, horizons)
        except Exception as exc:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] 监控异常: {exc}")

        if args.once:
            return
        time.sleep(args.poll_seconds)


def run_scan(args, horizons):
    df = fetch_recent_klines(args.symbol, args.interval, args.limit)
    df = filter_closed_klines(df)
    df = prepare_indicators(df)
    signals_df = build_signal_records(df, args.min_score, args.cooldown, horizons)

    print(f"扫描标的: {args.symbol}")
    print(f"周期: {args.interval}")
    print(f"最近已识别拐点信号数: {len(signals_df)}")
    if args.show_all:
        print_signal_table(signals_df, horizons)
    else:
        print_signal_table(signals_df, horizons, limit=args.recent)

    if not signals_df.empty:
        latest = signals_df.iloc[-1]
        print("\n最近一个信号:")
        print(
            f"{latest['time'].strftime('%Y-%m-%d %H:%M:%S')} UTC | "
            f"{latest['side']} | score={latest['score']} | {latest['reasons']}"
        )


def run_backtest(args, horizons):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    df = fetch_historical_klines(args.symbol, args.interval, start_ms, end_ms)
    df = prepare_indicators(df)
    signals_df = build_signal_records(df, args.min_score, args.cooldown, horizons)
    summary = summarize_backtest(signals_df, horizons)

    print(f"回测标的: {args.symbol}")
    print(f"周期: {args.interval}")
    print(
        f"样本区间(UTC): {start_dt.strftime('%Y-%m-%d %H:%M:%S')} -> "
        f"{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(
        f"总信号数: {summary['total']} | 多头: {summary['long_count']} | "
        f"空头: {summary['short_count']} | min_score: {args.min_score}"
    )

    if signals_df.empty:
        print("近三个月没有满足条件的拐点信号。")
        return

    print("\n总体分数分布:")
    for score, count in summary["score_mix"].items():
        print(f"  score {score}: {count}")

    print("\n整体理由组合分布:")
    for reason, count in summary["reason_mix"].head(10).items():
        print(f"  {reason}: {count}")

    for side in ["long", "short"]:
        side_summary = summary["per_side"].get(side, {})
        print(f"\n{side.upper()} 信号: {side_summary.get('count', 0)}")
        for score, count in side_summary.get("score_mix", pd.Series(dtype="int64")).items():
            print(f"  score {score}: {count}")
        for horizon in horizons:
            stats = side_summary.get("horizon_stats", {}).get(horizon)
            if not stats or stats["count"] == 0:
                print(f"  {horizon:>3} 根后: 无足够样本")
                continue
            print(
                f"  {horizon:>3} 根后: 样本 {stats['count']}, "
                f"均值 {stats['avg_return'] * 100:.2f}%, "
                f"中位数 {stats['median_return'] * 100:.2f}%, "
                f"胜率 {stats['win_rate'] * 100:.2f}%"
            )

    if args.show_all:
        print("\n全部拐点信号:")
        print_signal_table(signals_df, horizons)
    else:
        print("\n最近 15 个拐点信号:")
        print_signal_table(signals_df, horizons, limit=15)


def main():
    args = parse_args()
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    if not horizons:
        horizons = DEFAULT_HORIZONS

    if args.mode == "scan":
        run_scan(args, horizons)
    elif args.mode == "monitor":
        run_monitor(args, horizons)
    else:
        run_backtest(args, horizons)


if __name__ == "__main__":
    main()


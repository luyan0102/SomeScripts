import base64
import hashlib
import hmac
import os
import time
import urllib.parse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# 配置
# =========================
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")
COINGLASS_BASE_URL = "https://open-api-v4.coinglass.com"
COINGLASS_COIN_SYMBOL = os.getenv("COINGLASS_COIN_SYMBOL", "BTC")
COINGLASS_PAIR_SYMBOL = os.getenv("COINGLASS_PAIR_SYMBOL", "BTCUSDT")
COINGLASS_EXCHANGE = os.getenv("COINGLASS_EXCHANGE", "Binance")

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK")
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET")
ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "1") != "0"

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "30"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "300"))
MONITOR_COOLDOWN_SECONDS = int(os.getenv("MONITOR_COOLDOWN_SECONDS", "900"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))

MIN_LIQUIDATION_USD_1H = float(os.getenv("MIN_LIQUIDATION_USD_1H", "500000"))
MIN_IMBALANCE_RATIO = float(os.getenv("MIN_IMBALANCE_RATIO", "0.25"))
MIN_OI_CHANGE_PCT_15M = float(os.getenv("MIN_OI_CHANGE_PCT_15M", "0.05"))
MIN_LIQUIDATION_ACCEL = float(os.getenv("MIN_LIQUIDATION_ACCEL", "1.2"))

TRADE_SL_PCT = float(os.getenv("TRADE_SL_PCT", "0.008"))
TRADE_TP1_PCT = float(os.getenv("TRADE_TP1_PCT", "0.010"))
TRADE_TP2_PCT = float(os.getenv("TRADE_TP2_PCT", "0.018"))

MARKET_BASE_URL = "https://fapi.binance.com"

last_notifications = {
    "monitor": {"ts": 0.0},
    "signal": {"key": None, "ts": 0.0},
}
funding_cache = {"value": 0.0, "ts": 0.0}
oi_cache = {"value": None, "ts": 0.0}
liq_cache = {"value": None, "ts": 0.0}

http_session = requests.Session()
http_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(408, 429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
    ),
)


def validate_config():
    if not COINGLASS_API_KEY:
        raise RuntimeError("未设置 COINGLASS_API_KEY 环境变量")
    if ENABLE_ALERTS and (not DINGTALK_WEBHOOK or not DINGTALK_SECRET):
        raise RuntimeError(
            "ENABLE_ALERTS=1 但未设置 DINGTALK_WEBHOOK / DINGTALK_SECRET 环境变量"
        )


# =========================
# 钉钉
# =========================
def send(msg):
    if not ENABLE_ALERTS:
        return False

    timestamp = str(round(time.time() * 1000))
    secret = DINGTALK_SECRET.encode()
    string = f"{timestamp}\n{DINGTALK_SECRET}"
    sign = urllib.parse.quote_plus(
        base64.b64encode(
            hmac.new(secret, string.encode(), hashlib.sha256).digest()
        )
    )
    url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"

    resp = http_session.post(
        url,
        json={"msgtype": "text", "text": {"content": msg}},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return True


# =========================
# 数据源
# =========================
def fetch_json(base_url, path, params=None, headers=None):
    resp = http_session.get(
        f"{base_url}{path}",
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_coinglass_json(path, params=None):
    payload = fetch_json(
        COINGLASS_BASE_URL,
        path,
        params=params,
        headers={"CG-API-KEY": COINGLASS_API_KEY},
    )

    if isinstance(payload, dict):
        code = payload.get("code")
        if code not in (None, 0, "0"):
            raise RuntimeError(
                f"CoinGlass API error {code}: {payload.get('msg', 'unknown error')}"
            )
        if "data" in payload:
            return payload["data"]

    raise RuntimeError(f"CoinGlass API 返回异常: {payload!r}")


def price():
    data = fetch_json(
        MARKET_BASE_URL,
        "/fapi/v1/ticker/price",
        params={"symbol": COINGLASS_PAIR_SYMBOL},
    )
    return float(data["price"])


def funding():
    now = time.time()
    if now - funding_cache["ts"] < 300:
        return funding_cache["value"]

    rows = fetch_coinglass_json("/api/futures/funding-rate/exchange-list")
    target = next(
        (row for row in rows if row.get("symbol") == COINGLASS_COIN_SYMBOL),
        None,
    )
    if not target:
        raise RuntimeError(f"未找到 {COINGLASS_COIN_SYMBOL} 的 funding 数据")

    candidates = []
    for key in ("stablecoin_margin_list", "token_margin_list"):
        for item in target.get(key, []):
            if "funding_rate" in item:
                candidates.append(item)

    exchange_match = next(
        (item for item in candidates if item.get("exchange") == COINGLASS_EXCHANGE),
        None,
    )
    selected = exchange_match or (candidates[0] if candidates else None)
    if not selected:
        raise RuntimeError(f"未找到 {COINGLASS_EXCHANGE} 的 funding 数据")

    value = float(selected["funding_rate"])
    funding_cache["value"] = value
    funding_cache["ts"] = now
    return value


def oi_snapshot():
    now = time.time()
    if now - oi_cache["ts"] < 60 and oi_cache["value"] is not None:
        return oi_cache["value"]

    rows = fetch_coinglass_json(
        "/api/futures/open-interest/exchange-list",
        {"symbol": COINGLASS_COIN_SYMBOL},
    )
    target = next(
        (row for row in rows if row.get("exchange") == COINGLASS_EXCHANGE),
        None,
    )
    if not target:
        target = next((row for row in rows if row.get("exchange") == "All"), None)
    if not target:
        raise RuntimeError(f"未找到 {COINGLASS_COIN_SYMBOL} 的 OI 数据")

    value = {
        "exchange": target.get("exchange", COINGLASS_EXCHANGE),
        "symbol": target.get("symbol", COINGLASS_COIN_SYMBOL),
        "open_interest_usd": float(target.get("open_interest_usd", 0.0)),
        "open_interest_quantity": float(target.get("open_interest_quantity", 0.0)),
        "oi_change_5m": float(target.get("open_interest_change_percent_5m", 0.0)),
        "oi_change_15m": float(target.get("open_interest_change_percent_15m", 0.0)),
        "oi_change_30m": float(target.get("open_interest_change_percent_30m", 0.0)),
        "oi_change_1h": float(target.get("open_interest_change_percent_1h", 0.0)),
        "oi_change_4h": float(target.get("open_interest_change_percent_4h", 0.0)),
        "oi_change_24h": float(target.get("open_interest_change_percent_24h", 0.0)),
    }
    oi_cache["value"] = value
    oi_cache["ts"] = now
    return value


def liquidation_snapshot():
    now = time.time()
    if now - liq_cache["ts"] < 60 and liq_cache["value"] is not None:
        return liq_cache["value"]

    rows = fetch_coinglass_json(
        "/api/futures/liquidation/coin-list",
        {"exchange": COINGLASS_EXCHANGE},
    )
    target = next(
        (row for row in rows if row.get("symbol") == COINGLASS_COIN_SYMBOL),
        None,
    )
    if not target:
        raise RuntimeError(f"未找到 {COINGLASS_COIN_SYMBOL} 的爆仓数据")

    value = {
        "total_24h": float(target.get("liquidation_usd_24h", 0.0)),
        "long_24h": float(target.get("long_liquidation_usd_24h", 0.0)),
        "short_24h": float(target.get("short_liquidation_usd_24h", 0.0)),
        "total_12h": float(target.get("liquidation_usd_12h", 0.0)),
        "long_12h": float(target.get("long_liquidation_usd_12h", 0.0)),
        "short_12h": float(target.get("short_liquidation_usd_12h", 0.0)),
        "total_4h": float(target.get("liquidation_usd_4h", 0.0)),
        "long_4h": float(target.get("long_liquidation_usd_4h", 0.0)),
        "short_4h": float(target.get("short_liquidation_usd_4h", 0.0)),
        "total_1h": float(target.get("liquidation_usd_1h", 0.0)),
        "long_1h": float(target.get("long_liquidation_usd_1h", 0.0)),
        "short_1h": float(target.get("short_liquidation_usd_1h", 0.0)),
    }
    liq_cache["value"] = value
    liq_cache["ts"] = now
    return value


# =========================
# 信号分析
# =========================
def clamp(value, low, high):
    return max(low, min(value, high))


def analyze(fund, oi_data, liq_data):
    total_1h = liq_data["total_1h"]
    total_4h = liq_data["total_4h"]
    total_24h = liq_data["total_24h"]

    bias_1h = (liq_data["short_1h"] - liq_data["long_1h"]) / (total_1h + 1e-6)
    bias_4h = (liq_data["short_4h"] - liq_data["long_4h"]) / (total_4h + 1e-6)
    combined_bias = 0.7 * bias_1h + 0.3 * bias_4h

    hourly_avg_24h = total_24h / 24 if total_24h > 0 else 0.0
    liq_accel = total_1h / hourly_avg_24h if hourly_avg_24h > 0 else 0.0
    oi_change_15m = oi_data["oi_change_15m"]
    oi_change_1h = oi_data["oi_change_1h"]

    has_event = (
        total_1h >= MIN_LIQUIDATION_USD_1H
        and abs(combined_bias) >= MIN_IMBALANCE_RATIO
        and liq_accel >= MIN_LIQUIDATION_ACCEL
    )

    if has_event and combined_bias > 0 and oi_change_15m >= MIN_OI_CHANGE_PCT_15M:
        signal = "LONG"
    elif has_event and combined_bias < 0 and oi_change_15m >= MIN_OI_CHANGE_PCT_15M:
        signal = "SHORT"
    else:
        signal = "WAIT"

    confidence = (
        0.55 * clamp(abs(combined_bias), 0.0, 1.0)
        + 0.25 * clamp(liq_accel / max(MIN_LIQUIDATION_ACCEL, 1e-6) - 1, 0.0, 1.0)
        + 0.20 * clamp(oi_change_15m / max(MIN_OI_CHANGE_PCT_15M, 1e-6) - 1, 0.0, 1.0)
    )

    if signal == "LONG" and fund < 0:
        confidence += 0.05
    if signal == "SHORT" and fund > 0:
        confidence += 0.05
    confidence = clamp(confidence, 0.0, 0.99)

    metrics = {
        "bias_1h": bias_1h,
        "bias_4h": bias_4h,
        "combined_bias": combined_bias,
        "liq_accel": liq_accel,
        "hourly_avg_24h": hourly_avg_24h,
        "oi_change_15m": oi_change_15m,
        "oi_change_1h": oi_change_1h,
        "event_flag": has_event,
    }
    return signal, confidence, metrics


# =========================
# 交易计划
# =========================
def build_trade_plan(px, signal, confidence):
    sl_pct = TRADE_SL_PCT
    tp1_pct = TRADE_TP1_PCT
    tp2_pct = TRADE_TP2_PCT

    if signal == "LONG":
        sl = px * (1 - sl_pct)
        tp1 = px * (1 + tp1_pct)
        tp2 = px * (1 + tp2_pct)
    elif signal == "SHORT":
        sl = px * (1 + sl_pct)
        tp1 = px * (1 - tp1_pct)
        tp2 = px * (1 - tp2_pct)
    else:
        return None

    risk = abs(px - sl)
    if risk < 1e-6:
        return None

    reward = 0.5 * abs(tp1 - px) + 0.5 * abs(tp2 - px)
    rr = reward / risk
    risk_amt = ACCOUNT_BALANCE * RISK_PER_TRADE
    size = (risk_amt / risk) * (0.5 + confidence)

    return {
        "entry": px,
        "tp1": tp1,
        "tp2": tp2,
        "sl": sl,
        "rr": rr,
        "size": size,
    }


def format_monitor_message(px, fund, oi_data, liq_data, sig, conf, metrics):
    status = "事件不足" if not metrics["event_flag"] else "可触发监控"

    return f"""
📡 BTC 日常监控

数据源: CoinGlass Non-Heatmap
交易所: {COINGLASS_EXCHANGE}
交易对: {COINGLASS_PAIR_SYMBOL}

价格: {px:.2f}
Funding: {fund:.6f}
OI: {oi_data['open_interest_quantity']:.2f}
OI(USD): {oi_data['open_interest_usd']:,.2f}
OI变化: 15m {oi_data['oi_change_15m']:.2f}% / 1h {oi_data['oi_change_1h']:.2f}%

1h爆仓: {liq_data['total_1h']:,.2f} USD
1h多头爆仓: {liq_data['long_1h']:,.2f}
1h空头爆仓: {liq_data['short_1h']:,.2f}
4h爆仓: {liq_data['total_4h']:,.2f} USD
24h小时均值: {metrics['hourly_avg_24h']:,.2f} USD

监控状态: {status}
方向判断: {sig}
置信度: {conf:.3f}
失衡度(1h/4h): {metrics['bias_1h']:.3f} / {metrics['bias_4h']:.3f}
综合偏向: {metrics['combined_bias']:.3f}
爆仓加速度: {metrics['liq_accel']:.2f}
""".strip()


def format_signal_message(px, fund, oi_data, liq_data, sig, conf, metrics, plan):
    return f"""
🚨 BTC 交易信号触发

数据源: CoinGlass Non-Heatmap
交易所: {COINGLASS_EXCHANGE}
交易对: {COINGLASS_PAIR_SYMBOL}

价格: {px:.2f}
Funding: {fund:.6f}
OI: {oi_data['open_interest_quantity']:.2f}
OI变化: 15m {oi_data['oi_change_15m']:.2f}% / 1h {oi_data['oi_change_1h']:.2f}%

1h爆仓: {liq_data['total_1h']:,.2f} USD
1h多头爆仓: {liq_data['long_1h']:,.2f}
1h空头爆仓: {liq_data['short_1h']:,.2f}
综合偏向: {metrics['combined_bias']:.3f}
爆仓加速度: {metrics['liq_accel']:.2f}

信号: {sig}
置信度: {conf:.3f}

——————————
🎯 固定百分比交易计划

入场: {plan['entry']:.2f}
止损: {plan['sl']:.2f}

止盈:
TP1: {plan['tp1']:.2f} (减仓50%)
TP2: {plan['tp2']:.2f} (全平)

风险收益比: {plan['rr']:.2f}
建议仓位: {plan['size']:.4f} BTC
风险资金: {ACCOUNT_BALANCE * RISK_PER_TRADE:.2f}
""".strip()


def signal_key(sig, plan, metrics):
    return (
        f"{sig}:{plan['entry']:.2f}:{plan['sl']:.2f}:"
        f"{plan['tp1']:.2f}:{plan['tp2']:.2f}:{metrics['combined_bias']:.3f}"
    )


# =========================
# 主循环
# =========================
def run():
    print("🚀 CoinGlass non-heatmap 监控启动")

    while True:
        try:
            px = price()
            fund = funding()
            oi_data = oi_snapshot()
            liq_data = liquidation_snapshot()

            sig, conf, metrics = analyze(fund, oi_data, liq_data)
            plan = build_trade_plan(px, sig, conf) if sig in {"LONG", "SHORT"} else None

            monitor_msg = format_monitor_message(
                px=px,
                fund=fund,
                oi_data=oi_data,
                liq_data=liq_data,
                sig=sig,
                conf=conf,
                metrics=metrics,
            )
            print(monitor_msg)

            now = time.time()
            if now - last_notifications["monitor"]["ts"] >= MONITOR_COOLDOWN_SECONDS:
                try:
                    send(monitor_msg)
                    last_notifications["monitor"]["ts"] = now
                except Exception as e:
                    print("monitor send error:", e)

            if not plan or sig not in {"LONG", "SHORT"}:
                time.sleep(LOOP_INTERVAL)
                continue

            trade_msg = format_signal_message(
                px=px,
                fund=fund,
                oi_data=oi_data,
                liq_data=liq_data,
                sig=sig,
                conf=conf,
                metrics=metrics,
                plan=plan,
            )
            trade_signal_key = signal_key(sig, plan, metrics)
            should_alert = (
                metrics["event_flag"]
                and (
                    trade_signal_key != last_notifications["signal"]["key"]
                    or now - last_notifications["signal"]["ts"] >= ALERT_COOLDOWN_SECONDS
                )
            )
            if should_alert:
                print(trade_msg)
                try:
                    send(trade_msg)
                    last_notifications["signal"]["key"] = trade_signal_key
                    last_notifications["signal"]["ts"] = now
                except Exception as e:
                    print("signal send error:", e)

        except Exception as e:
            print("run error:", e)

        time.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    validate_config()
    run()

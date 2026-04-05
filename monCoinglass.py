import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
from collections import deque

import requests
import websocket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# 配置
# =========================
SYMBOL = "BTCUSDT"

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK")
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET")
ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "1") != "0"

ACCOUNT_BALANCE = 10000
RISK_PER_TRADE = 0.01
LOOP_INTERVAL = 3
MIN_LEVELS = 10
ALERT_COOLDOWN_SECONDS = 300
MONITOR_COOLDOWN_SECONDS = int(os.getenv("MONITOR_COOLDOWN_SECONDS", "900"))
REQUEST_TIMEOUT = 3
MARKET_BASE_URL = "https://fapi.binance.com"
PRICE_BUCKET_SIZE = 100
PRECHECK_MIN_QTY = 0.5
PRECHECK_MIN_EVENTS = 2

heatmap = {}
liq_history = deque()
oi_history = deque()
last_notifications = {
    "monitor": {"ts": 0.0},
    "signal": {"key": None, "ts": 0.0},
}
funding_cache = {"value": 0.0, "ts": 0.0}
state_lock = threading.Lock()

market_session = requests.Session()
market_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
    ),
)


def validate_config():
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

    timestamp = str(round(time.time()*1000))
    secret = DINGTALK_SECRET.encode()

    string = f"{timestamp}\n{DINGTALK_SECRET}"
    sign = urllib.parse.quote_plus(
        base64.b64encode(
            hmac.new(secret, string.encode(), hashlib.sha256).digest()
        )
    )

    url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"

    resp = requests.post(
        url,
        json={"msgtype":"text","text":{"content":msg}},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return True

# =========================
# 数据
# =========================
def fetch_json(path, params):
    resp = market_session.get(
        f"{MARKET_BASE_URL}{path}",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def price():
    return float(fetch_json(
        "/fapi/v1/ticker/price",
        {"symbol":SYMBOL},
    )["price"])

def funding():
    now = time.time()
    if now - funding_cache["ts"] < 60:
        return funding_cache["value"]

    value = float(fetch_json(
        "/fapi/v1/premiumIndex",
        {"symbol":SYMBOL},
    )["lastFundingRate"])
    funding_cache["value"] = value
    funding_cache["ts"] = now
    return value

def oi():
    return float(fetch_json(
        "/fapi/v1/openInterest",
        {"symbol":SYMBOL},
    )["openInterest"])
# =========================
# WS
# =========================
def on_msg(ws, msg):
    data = json.loads(msg)
    now = time.time()

    if isinstance(data, list):
        events = data
    elif isinstance(data, dict):
        events = [data]
    else:
        return

    with state_lock:
        for event in events:
            payload = event.get("o", event)

            if not {"p", "S", "s"} <= payload.keys():
                continue

            if payload["s"] != SYMBOL:
                continue

            p = float(payload["p"])
            q = float(payload.get("z") or payload.get("q") or 0)
            s = payload["S"]

            lvl = int(p // PRICE_BUCKET_SIZE) * PRICE_BUCKET_SIZE

            if lvl not in heatmap:
                heatmap[lvl] = {
                    "long": 0,
                    "short": 0,
                    "ts": now,
                    "decay_ts": now,
                }

            if s == "SELL":
                heatmap[lvl]["long"] += q
            else:
                heatmap[lvl]["short"] += q

            heatmap[lvl]["ts"] = now
            liq_history.append({"t": now, "q": q})

        while liq_history and now-liq_history[0]["t"]>30:
            liq_history.popleft()

def ws():
    while True:
        try:
            websocket.WebSocketApp(
                "wss://fstream.binance.com/ws/!forceOrder@arr",
                on_message=on_msg
            ).run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("WS error:", e)
            time.sleep(5)

# =========================
# 工具
# =========================
def decay():
    now = time.time()
    with state_lock:
        for k in list(heatmap.keys()):
            age = now - heatmap[k].get("decay_ts", heatmap[k]["ts"])
            if age <= 0:
                continue

            f = 0.97 ** (age / 5)

            heatmap[k]["long"] *= f
            heatmap[k]["short"] *= f
            heatmap[k]["decay_ts"] = now

            if heatmap[k]["long"] + heatmap[k]["short"] < 0.1:
                del heatmap[k]

def levels():
    with state_lock:
        return [{"price":p,"size":d["long"]+d["short"]} for p,d in heatmap.items()]

def update_oi(v):
    now=time.time()
    with state_lock:
        oi_history.append({"t":now,"oi":v})

        while oi_history and now-oi_history[0]["t"]>60:
            oi_history.popleft()

# =========================
# 信号分析
# =========================
def analyze(px, lv, fund):
    up, down = 0,0

    for l in lv:
        w = l["size"]/(abs(l["price"]-px)+50)

        if l["price"]>px:
            up+=w
        else:
            down+=w

    if fund>0: down*=1.2
    else: up*=1.2

    if down>up*1.2:
        sig="SHORT"
    elif up>down*1.2:
        sig="LONG"
    else:
        sig="WAIT"

    conf = abs(up-down)/(up+down+1e-6)

    return sig,conf,up,down

# =========================
# 抢跑
# =========================
def pre():
    now = time.time()
    with state_lock:
        recent_liq = list(liq_history)
        oi_start = oi_history[0]["oi"] if oi_history else None
        oi_end = oi_history[-1]["oi"] if oi_history else None

    recent_window = [x for x in recent_liq if now - x["t"] < 5]
    previous_window = [x for x in recent_liq if 5 < now - x["t"] < 10]
    r = sum(x["q"] for x in recent_window)
    p = sum(x["q"] for x in previous_window)

    accel = r / p if p > 0 else 0.0

    if oi_start is None or oi_end is None or oi_start == oi_end:
        oi_change = 0
    else:
        oi_change = oi_end - oi_start

    enough_liq = (
        len(recent_window) >= PRECHECK_MIN_EVENTS
        and len(previous_window) >= PRECHECK_MIN_EVENTS
        and r >= PRECHECK_MIN_QTY
        and p >= PRECHECK_MIN_QTY
    )

    return enough_liq and accel > 2 and oi_change < 0, accel

# =========================
# 🎯 核心：交易计划生成
# =========================
def build_trade_plan(px, sig, conf, lv):

    # 找最近流动性
    above = sorted([l for l in lv if l["price"]>px], key=lambda x:x["price"])
    below = sorted([l for l in lv if l["price"]<px], key=lambda x:x["price"], reverse=True)

    if not above or not below:
        return None

    if sig=="LONG":
        tp1 = above[0]["price"]
        tp2 = above[min(1,len(above)-1)]["price"]
        sl = below[0]["price"]

    elif sig=="SHORT":
        tp1 = below[0]["price"]
        tp2 = below[min(1,len(below)-1)]["price"]
        sl = above[0]["price"]

    else:
        return None

    # 风险收益
    risk = abs(px-sl)
    reward = 0.5 * abs(tp1 - px) + 0.5 * abs(tp2 - px)
    rr = reward/(risk+1e-6)

    # 仓位
    risk_amt = ACCOUNT_BALANCE*RISK_PER_TRADE
    size = (risk_amt/risk)*(0.5+conf)

    return {
        "entry":px,
        "tp1":tp1,
        "tp2":tp2,
        "sl":sl,
        "rr":rr,
        "size":size
    }


def format_monitor_message(px, fund, o, lv, sig=None, conf=None, up=None, down=None, accel=None, pflag=None):
    status = "数据积累中" if len(lv) < MIN_LEVELS else "正常监控"
    signal_text = sig if sig else "WAIT"
    conf_text = f"{conf:.3f}" if conf is not None else "-"
    accel_text = f"{accel:.2f}" if accel is not None else "-"
    up_text = f"{up:.2f}" if up is not None else "-"
    down_text = f"{down:.2f}" if down is not None else "-"
    pflag_text = str(pflag) if pflag is not None else "-"

    return f"""
📡 BTC 日常监控

价格: {px:.2f}
Funding: {fund:.6f}
OI: {o:.2f}
流动性层数: {len(lv)}
监控状态: {status}

方向判断: {signal_text}
置信度: {conf_text}
加速度: {accel_text}
抢跑: {pflag_text}

流动性:
↑ {up_text}
↓ {down_text}
""".strip()


def format_signal_message(px, fund, o, sig, conf, up, down, accel, pflag, plan):
    return f"""
🚨 BTC 交易信号触发

价格: {px:.2f}
Funding: {fund:.6f}
OI: {o:.2f}

信号: {sig}
置信度: {conf:.3f}
加速度: {accel:.2f}
抢跑: {pflag}

流动性:
↑ {up:.2f}
↓ {down:.2f}

——————————
🎯 交易计划

入场: {plan['entry']:.2f}
止损: {plan['sl']:.2f}

止盈:
TP1: {plan['tp1']:.2f} (减仓50%)
TP2: {plan['tp2']:.2f} (全平)

风险收益比: {plan['rr']:.2f}
建议仓位: {plan['size']:.4f} BTC
风险资金: {ACCOUNT_BALANCE * RISK_PER_TRADE:.2f}
""".strip()


def signal_key(sig, plan):
    return (
        f"{sig}:{plan['entry']:.2f}:{plan['sl']:.2f}:"
        f"{plan['tp1']:.2f}:{plan['tp2']:.2f}"
    )

# =========================
# 主循环
# =========================
def run():
    threading.Thread(target=ws, daemon=True).start()

    print("🚀 决策系统启动")

    while True:
        try:
            px = price()
            fund = funding()
            o = oi()

            update_oi(o)
            decay()

            lv = levels()
            sig = conf = up = down = None
            plan = None
            pflag, accel = pre()

            if len(lv) >= MIN_LEVELS:
                sig, conf, up, down = analyze(px, lv, fund)
                plan = build_trade_plan(px, sig, conf, lv)

            monitor_msg = format_monitor_message(
                px=px,
                fund=fund,
                o=o,
                lv=lv,
                sig=sig,
                conf=conf,
                up=up,
                down=down,
                accel=accel,
                pflag=pflag,
            )
            print(monitor_msg)

            now = time.time()
            should_send_monitor = (
                now - last_notifications["monitor"]["ts"] >= MONITOR_COOLDOWN_SECONDS
            )
            if should_send_monitor:
                try:
                    send(monitor_msg)
                    last_notifications["monitor"]["ts"] = now
                except Exception as e:
                    print("monitor send error:", e)

            if not plan or sig not in {"LONG", "SHORT"}:
                continue

            trade_msg = format_signal_message(px, fund, o, sig, conf, up, down, accel, pflag, plan)
            trade_signal_key = signal_key(sig, plan)
            should_alert = (
                pflag and (
                    trade_signal_key != last_notifications["signal"]["key"] or
                    now - last_notifications["signal"]["ts"] >= ALERT_COOLDOWN_SECONDS
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

        finally:
            time.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    validate_config()
    run()

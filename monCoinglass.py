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
REQUEST_TIMEOUT = 3
MARKET_BASE_URL = "https://fapi.binance.com"

heatmap = {}
liq_history = deque()
oi_history = deque()
last_alert = {"signal": None, "ts": 0.0}
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

            if not {"p", "S"} <= payload.keys():
                continue

            p = float(payload["p"])
            q = float(payload.get("z") or payload.get("q") or 0)
            s = payload["S"]

            lvl = int(p//100)*100

            if lvl not in heatmap:
                heatmap[lvl]={"long":0,"short":0,"ts":now}

            if s=="SELL":
                heatmap[lvl]["long"]+=q
            else:
                heatmap[lvl]["short"]+=q

            heatmap[lvl]["ts"]=now
            liq_history.append({"t":now,"q":q})

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
    now=time.time()
    with state_lock:
        for k in list(heatmap.keys()):
            age=now-heatmap[k]["ts"]
            f=0.97**(age/5)

            heatmap[k]["long"]*=f
            heatmap[k]["short"]*=f

            if heatmap[k]["long"]+heatmap[k]["short"]<0.1:
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
    now=time.time()
    with state_lock:
        recent_liq = list(liq_history)
        oi_start = oi_history[0]["oi"] if oi_history else None
        oi_end = oi_history[-1]["oi"] if oi_history else None

    r=sum(x["q"] for x in recent_liq if now-x["t"]<5)
    p=sum(x["q"] for x in recent_liq if 5<now-x["t"]<10)+1e-6

    accel=r/p

    if oi_start is None or oi_end is None or oi_start == oi_end:
        oi_change = 0
    else:
        oi_change = oi_end - oi_start

    return accel>2 and oi_change<0, accel

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
            if len(lv)<MIN_LEVELS:
                continue

            sig, conf, up, down = analyze(px, lv, fund)
            pflag, accel = pre()

            plan = build_trade_plan(px, sig, conf, lv)

            if not plan:
                continue

            msg = f"""
📊 BTC 交易决策

价格: {px}
Funding: {fund:.6f}
OI: {o}

信号: {sig}
置信度: {round(conf,3)}
加速度: {round(accel,2)}

流动性:
↑ {round(up,2)}
↓ {round(down,2)}

——————————
🎯 交易计划

入场: {round(plan['entry'],2)}

止损: {round(plan['sl'],2)}

止盈:
TP1: {round(plan['tp1'],2)} (减仓50%)
TP2: {round(plan['tp2'],2)} (全平)

风险收益比: {round(plan['rr'],2)}

建议仓位:
{round(plan['size'],4)} BTC
风险资金: {ACCOUNT_BALANCE*RISK_PER_TRADE}

——————————
状态:
抢跑: {pflag}
"""

            print(msg)

            now = time.time()
            should_alert = (
                pflag and (
                    sig != last_alert["signal"] or
                    now - last_alert["ts"] >= ALERT_COOLDOWN_SECONDS
                )
            )
            if should_alert:
                send("🚨 信号触发\n"+msg)
                last_alert["signal"] = sig
                last_alert["ts"] = now

        except Exception as e:
            print("run error:", e)

        finally:
            time.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    validate_config()
    run()

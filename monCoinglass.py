import requests
import websocket
import json
import threading
import time
import hmac
import hashlib
import base64
import urllib.parse

# =========================
# 配置
# =========================
SYMBOL = "BTCUSDT"

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=d4764056baa8a34085ed2d6ee5c5583fc0b269aad7afaccc01829c293f7a22d1"
DINGTALK_SECRET = "SEC5f82cb8692508d03d40004ef4fec7d25b059177e0fa7c938971d93d2c073897d"

ACCOUNT_BALANCE = 10000
RISK_PER_TRADE = 0.01

heatmap = {}
liq_history = []
oi_history = []
last_signal = None

# =========================
# 钉钉
# =========================
def send(msg):
    timestamp = str(round(time.time()*1000))
    secret = DINGTALK_SECRET.encode()

    string = f"{timestamp}\n{DINGTALK_SECRET}"
    sign = urllib.parse.quote_plus(
        base64.b64encode(
            hmac.new(secret, string.encode(), hashlib.sha256).digest()
        )
    )

    url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"

    requests.post(url, json={"msgtype":"text","text":{"content":msg}})

# =========================
# 数据
# =========================
def price():
    return float(requests.get(
        "https://fapi.binance.com/fapi/v1/ticker/price",
        params={"symbol":SYMBOL},
        timeout=3
    ).json()["price"])

def funding():
    return float(requests.get(
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        params={"symbol":SYMBOL},
        timeout=3
    ).json()["lastFundingRate"])

def oi():
    return float(requests.get(
        "https://fapi.binance.com/fapi/v1/openInterest",
                params={"symbol":SYMBOL},
        timeout=3
    ).json()["openInterest"])
# =========================
# WS
# =========================
def on_msg(ws, msg):
    data = json.loads(msg)
    now = time.time()

    for o in data:
        p = float(o["p"])
        q = float(o["q"])
        s = o["S"]

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
        liq_history.pop(0)

def ws():
    while True:
        try:
            websocket.WebSocketApp(
                "wss://fstream.binance.com/ws/!forceOrder@arr",
                on_message=on_msg
            ).run_forever()
        except Exception as e:
            print("WS error:", e)
            time.sleep(5)

# =========================
# 工具
# =========================
def decay():
    now=time.time()
    for k in list(heatmap.keys()):
        age=now-heatmap[k]["ts"]
        f=0.97**(age/5)

        heatmap[k]["long"]*=f
        heatmap[k]["short"]*=f

        if heatmap[k]["long"]+heatmap[k]["short"]<0.1:
            del heatmap[k]

def levels():
    return [{"price":p,"size":d["long"]+d["short"]} for p,d in heatmap.items()]

def update_oi(v):
    now=time.time()
    oi_history.append({"t":now,"oi":v})

    while oi_history and now-oi_history[0]["t"]>60:
        oi_history.pop(0)

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

    r=sum(x["q"] for x in liq_history if now-x["t"]<5)
    p=sum(x["q"] for x in liq_history if 5<now-x["t"]<10)+1e-6

    accel=r/p

    oi_change = oi_history[-1]["oi"]-oi_history[0]["oi"] if len(oi_history)>1 else 0

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
    global last_signal

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
            if len(lv)<10:
                time.sleep(2)
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

            if pflag and sig != last_signal:
                send("🚨 信号触发\n"+msg)
                last_signal = sig

        except Exception as e:
            print(e)

        time.sleep(3)


if __name__ == "__main__":
    run()
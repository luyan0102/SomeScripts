import base64
import hashlib
import hmac
import os
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from env_loader import load_local_env


load_local_env()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def env_float(name: str) -> Optional[float]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return float(value)


COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")
COINGLASS_BASE_URL = os.getenv("COINGLASS_BASE_URL", "https://open-api-v4.coinglass.com")
COINGLASS_COIN_SYMBOL = os.getenv("COINGLASS_COIN_SYMBOL", "BTC")
COINGLASS_EXCHANGE = os.getenv("COINGLASS_EXCHANGE", "Binance")

DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK3") or os.getenv("DINGTALK_WEBHOOK")
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET3") or os.getenv("DINGTALK_SECRET")
ENABLE_ALERTS = env_bool("ENABLE_ALERTS", True)
SEND_OBSERVE_ALERTS = env_bool("SEND_OBSERVE_ALERTS", False)

LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "300"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MARKET_CACHE_SECONDS = int(os.getenv("MARKET_CACHE_SECONDS", "60"))
TWITTER_CACHE_SECONDS = int(os.getenv("TWITTER_CACHE_SECONDS", "300"))
ETF_CACHE_SECONDS = int(os.getenv("ETF_CACHE_SECONDS", "3600"))
MACRO_CACHE_SECONDS = int(os.getenv("MACRO_CACHE_SECONDS", "21600"))

ETF_FLOW_MILLION = env_float("ETF_FLOW_MILLION")
ETF_STRONG_INFLOW_MILLION = float(os.getenv("ETF_STRONG_INFLOW_MILLION", "100"))
ETF_STRONG_OUTFLOW_MILLION = float(os.getenv("ETF_STRONG_OUTFLOW_MILLION", "-100"))
ETF_FLOW_PATH = os.getenv("ETF_FLOW_PATH", "/api/etf/bitcoin/flow-history")

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = os.getenv("FRED_BASE_URL", "https://api.stlouisfed.org")
FRED_CPI_SERIES_ID = os.getenv("FRED_CPI_SERIES_ID", "CPIAUCSL")
FRED_CORE_CPI_SERIES_ID = os.getenv("FRED_CORE_CPI_SERIES_ID", "CPILFESL")
MACRO_ACTUAL = env_float("MACRO_ACTUAL")
MACRO_FORECAST = env_float("MACRO_FORECAST")
MACRO_SURPRISE_THRESHOLD = float(os.getenv("MACRO_SURPRISE_THRESHOLD", "0.2"))
MACRO_COOLING_THRESHOLD = float(os.getenv("MACRO_COOLING_THRESHOLD", "0.1"))
MACRO_HEATING_THRESHOLD = float(os.getenv("MACRO_HEATING_THRESHOLD", "0.1"))

ENABLE_TWITTER_SENTIMENT = env_bool("ENABLE_TWITTER_SENTIMENT", False)
TWITTER_QUERY = os.getenv("TWITTER_QUERY", "Bitcoin OR BTC Fed CPI lang:en")
TWITTER_SAMPLE_SIZE = int(os.getenv("TWITTER_SAMPLE_SIZE", "20"))

FUNDING_BEARISH_THRESHOLD = float(os.getenv("FUNDING_BEARISH_THRESHOLD", "0.0005"))
FUNDING_BULLISH_THRESHOLD = float(os.getenv("FUNDING_BULLISH_THRESHOLD", "0.0005"))
MIN_LIQUIDATION_USD_1H = float(os.getenv("MIN_LIQUIDATION_USD_1H", "500000"))
LIQUIDATION_IMBALANCE_RATIO = float(os.getenv("LIQUIDATION_IMBALANCE_RATIO", "2.0"))

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

market_cache = {"value": None, "ts": 0.0}
twitter_cache = {"value": None, "ts": 0.0}
etf_cache = {"value": None, "ts": 0.0}
macro_cache = {"value": None, "ts": 0.0}
last_alert = {"key": None, "ts": 0.0}
last_error_alert = {"key": None, "ts": 0.0}


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def format_money(value: float) -> str:
    return f"{value:,.0f}"


def validate_config() -> None:
    if not COINGLASS_API_KEY:
        raise RuntimeError("未设置 COINGLASS_API_KEY 环境变量")
    if ENABLE_ALERTS and not DINGTALK_WEBHOOK:
        raise RuntimeError("ENABLE_ALERTS=1 但未设置 DINGTALK_WEBHOOK3 环境变量")


def signed_dingtalk_url() -> str:
    if not DINGTALK_SECRET:
        return DINGTALK_WEBHOOK or ""

    timestamp = str(round(time.time() * 1000))
    secret = DINGTALK_SECRET.encode()
    string_to_sign = f"{timestamp}\n{DINGTALK_SECRET}"
    sign = urllib.parse.quote_plus(
        base64.b64encode(
            hmac.new(secret, string_to_sign.encode(), hashlib.sha256).digest()
        )
    )
    separator = "&" if "?" in (DINGTALK_WEBHOOK or "") else "?"
    return f"{DINGTALK_WEBHOOK}{separator}timestamp={timestamp}&sign={sign}"


def send(msg: str) -> bool:
    if not ENABLE_ALERTS:
        return False

    response = http_session.post(
        signed_dingtalk_url(),
        json={"msgtype": "text", "text": {"content": msg}},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    if payload.get("errcode") not in (0, "0", None):
        raise RuntimeError(f"钉钉返回异常: {payload}")
    return True


def fetch_json(base_url: str, path: str, params=None, headers=None):
    response = http_session.get(
        f"{base_url}{path}",
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_coinglass_json(path: str, params=None):
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


def fetch_fred_json(path: str, params=None):
    if not FRED_API_KEY:
        raise RuntimeError("未设置 FRED_API_KEY 环境变量")

    payload = fetch_json(
        FRED_BASE_URL,
        path,
        params={
            **(params or {}),
            "api_key": FRED_API_KEY,
            "file_type": "json",
        },
    )
    if "error_code" in payload:
        raise RuntimeError(
            f"FRED API error {payload.get('error_code')}: {payload.get('error_message')}"
        )
    return payload


def funding() -> float:
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

    return float(selected["funding_rate"])


def liquidation_snapshot() -> Dict[str, float]:
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

    return {
        "total_1h": float(target.get("liquidation_usd_1h", 0.0)),
        "long_1h": float(target.get("long_liquidation_usd_1h", 0.0)),
        "short_1h": float(target.get("short_liquidation_usd_1h", 0.0)),
        "total_4h": float(target.get("liquidation_usd_4h", 0.0)),
        "long_4h": float(target.get("long_liquidation_usd_4h", 0.0)),
        "short_4h": float(target.get("short_liquidation_usd_4h", 0.0)),
        "total_24h": float(target.get("liquidation_usd_24h", 0.0)),
        "long_24h": float(target.get("long_liquidation_usd_24h", 0.0)),
        "short_24h": float(target.get("short_liquidation_usd_24h", 0.0)),
    }


def get_market_data() -> Dict[str, float]:
    now = time.time()
    cached = market_cache["value"]
    if cached and now - market_cache["ts"] < MARKET_CACHE_SECONDS:
        return cached

    liq_data = liquidation_snapshot()
    value = {
        "funding": funding(),
        "long_liq": liq_data["long_1h"],
        "short_liq": liq_data["short_1h"],
        "total_liq": liq_data["total_1h"],
        "long_liq_4h": liq_data["long_4h"],
        "short_liq_4h": liq_data["short_4h"],
    }
    market_cache["value"] = value
    market_cache["ts"] = now
    return value


def latest_numeric_observations(observations: List[Dict[str, str]], limit: int) -> List[Dict[str, str]]:
    items = []
    for item in observations:
        value = item.get("value")
        if value in (None, ".", ""):
            continue
        try:
            float(value)
        except (TypeError, ValueError):
            continue
        items.append(item)
        if len(items) >= limit:
            break
    return items


def extract_flow_value(entry: Dict[str, object]) -> Optional[float]:
    for key in ("flow_usd", "changeUsd", "net_flow_usd"):
        value = entry.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def extract_flow_timestamp(entry: Dict[str, object]) -> Optional[int]:
    for key in ("timestamp", "date", "time"):
        value = entry.get(key)
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def extract_flow_breakdown(entry: Dict[str, object]) -> List[Dict[str, float]]:
    breakdown = []
    raw_items = entry.get("etf_flows") or entry.get("list") or []
    if not isinstance(raw_items, list):
        return breakdown

    for item in raw_items:
        if not isinstance(item, dict):
            continue

        ticker = item.get("etf_ticker") or item.get("ticker")
        value = extract_flow_value(item)
        if not ticker or value is None:
            continue
        breakdown.append({"ticker": str(ticker), "flow_usd": value})
    return breakdown


def get_etf_flow() -> Dict[str, object]:
    now = time.time()
    cached = etf_cache["value"]
    if cached and now - etf_cache["ts"] < ETF_CACHE_SECONDS:
        return cached

    try:
        rows = fetch_coinglass_json(ETF_FLOW_PATH)
        if not isinstance(rows, list) or not rows:
            raise RuntimeError(f"ETF Flow 数据为空: {rows!r}")

        latest_entry = next(
            (
                row
                for row in sorted(
                    (item for item in rows if isinstance(item, dict)),
                    key=lambda item: extract_flow_timestamp(item) or 0,
                    reverse=True,
                )
                if extract_flow_value(row) is not None
            ),
            None,
        )
        if not latest_entry:
            raise RuntimeError("ETF Flow 缺少可用的 flow 数值")

        timestamp = extract_flow_timestamp(latest_entry)
        flow_usd = extract_flow_value(latest_entry)
        price_usd = latest_entry.get("price_usd") or latest_entry.get("price")
        value = {
            "flow": (flow_usd or 0.0) / 1_000_000,
            "configured": True,
            "source": "coinglass",
            "as_of": (
                datetime.fromtimestamp(timestamp / 1000).date().isoformat()
                if timestamp
                else None
            ),
            "price_usd": float(price_usd) if price_usd not in (None, "") else None,
            "breakdown": extract_flow_breakdown(latest_entry),
        }
        etf_cache["value"] = value
        etf_cache["ts"] = now
        return value
    except Exception:
        if ETF_FLOW_MILLION is None:
            raise

        value = {
            "flow": ETF_FLOW_MILLION,
            "configured": True,
            "source": "manual",
            "as_of": None,
            "price_usd": None,
            "breakdown": [],
        }
        etf_cache["value"] = value
        etf_cache["ts"] = now
        return value


def get_macro_data() -> Dict[str, object]:
    now = time.time()
    cached = macro_cache["value"]
    if cached and now - macro_cache["ts"] < MACRO_CACHE_SECONDS:
        return cached

    try:
        headline_payload = fetch_fred_json(
            "/fred/series/observations",
            {
                "series_id": FRED_CPI_SERIES_ID,
                "units": "pc1",
                "sort_order": "desc",
                "limit": 3,
            },
        )
        core_payload = fetch_fred_json(
            "/fred/series/observations",
            {
                "series_id": FRED_CORE_CPI_SERIES_ID,
                "units": "pc1",
                "sort_order": "desc",
                "limit": 3,
            },
        )

        headline_observations = latest_numeric_observations(
            headline_payload.get("observations", []), 2
        )
        core_observations = latest_numeric_observations(
            core_payload.get("observations", []), 2
        )

        if len(headline_observations) < 2 or len(core_observations) < 2:
            raise RuntimeError("FRED CPI 数据不足，无法计算趋势")

        value = {
            "source": "fred",
            "headline_yoy": float(headline_observations[0]["value"]),
            "headline_prev_yoy": float(headline_observations[1]["value"]),
            "headline_date": headline_observations[0]["date"],
            "core_yoy": float(core_observations[0]["value"]),
            "core_prev_yoy": float(core_observations[1]["value"]),
            "core_date": core_observations[0]["date"],
        }
        macro_cache["value"] = value
        macro_cache["ts"] = now
        return value
    except Exception:
        if MACRO_ACTUAL is None or MACRO_FORECAST is None:
            raise

        value = {
            "source": "manual",
            "headline_yoy": MACRO_ACTUAL,
            "headline_prev_yoy": MACRO_FORECAST,
            "headline_date": None,
            "core_yoy": None,
            "core_prev_yoy": None,
            "core_date": None,
        }
        macro_cache["value"] = value
        macro_cache["ts"] = now
        return value


def macro_signal() -> Tuple[int, List[str], Optional[str], Dict[str, object]]:
    try:
        macro = get_macro_data()
    except Exception as exc:
        return 0, [], f"宏观数据不可用: {exc}", {}

    if macro.get("source") == "manual":
        actual = float(macro["headline_yoy"])
        forecast = float(macro["headline_prev_yoy"])
        surprise = actual - forecast
        if surprise >= MACRO_SURPRISE_THRESHOLD:
            return (
                -20,
                [f"CPI 高于预期 {surprise:.2f}"],
                f"宏观源: manual; actual {actual:.2f}; forecast {forecast:.2f}",
                macro,
            )
        if surprise <= -MACRO_SURPRISE_THRESHOLD:
            return (
                10,
                [f"CPI 低于预期 {abs(surprise):.2f}"],
                f"宏观源: manual; actual {actual:.2f}; forecast {forecast:.2f}",
                macro,
            )
        return (
            0,
            [],
            f"宏观源: manual; actual {actual:.2f}; forecast {forecast:.2f}",
            macro,
        )

    headline_yoy = float(macro["headline_yoy"])
    headline_prev_yoy = float(macro["headline_prev_yoy"])
    headline_delta = headline_yoy - headline_prev_yoy

    core_yoy = macro.get("core_yoy")
    core_prev_yoy = macro.get("core_prev_yoy")
    core_delta = None
    if core_yoy is not None and core_prev_yoy is not None:
        core_delta = float(core_yoy) - float(core_prev_yoy)

    reasons: List[str] = []
    score = 0
    if headline_delta <= -MACRO_COOLING_THRESHOLD:
        score += 10
        reasons.append(f"CPI 同比回落至 {headline_yoy:.2f}%")
    elif headline_delta >= MACRO_HEATING_THRESHOLD:
        score -= 15
        reasons.append(f"CPI 同比升至 {headline_yoy:.2f}%")

    if core_delta is not None:
        if core_delta <= -MACRO_COOLING_THRESHOLD:
            score += 8
            reasons.append(f"核心 CPI 回落至 {float(core_yoy):.2f}%")
        elif core_delta >= MACRO_HEATING_THRESHOLD:
            score -= 10
            reasons.append(f"核心 CPI 升至 {float(core_yoy):.2f}%")

    note_parts = [
        f"宏观源: {macro.get('source', 'unknown')}",
        (
            f"headline {headline_yoy:.2f}% ({headline_prev_yoy:.2f}% -> {headline_yoy:.2f}%)"
        ),
    ]
    if macro.get("headline_date"):
        note_parts.append(f"日期 {macro['headline_date']}")
    if core_delta is not None:
        note_parts.append(
            f"core {float(core_prev_yoy):.2f}% -> {float(core_yoy):.2f}%"
        )

    return score, reasons, "; ".join(note_parts), macro


def twitter_sentiment() -> Tuple[int, List[str], Optional[str]]:
    if not ENABLE_TWITTER_SENTIMENT:
        return 0, [], "Twitter 情绪未启用"

    now = time.time()
    cached = twitter_cache["value"]
    if cached and now - twitter_cache["ts"] < TWITTER_CACHE_SECONDS:
        return cached

    try:
        import snscrape.modules.twitter as sntwitter
    except ModuleNotFoundError:
        result = (0, [], "缺少 snscrape，已跳过 Twitter 情绪")
        twitter_cache["value"] = result
        twitter_cache["ts"] = now
        return result

    positive_hits = 0
    negative_hits = 0
    sample_count = 0
    positive_keywords = ("bullish", "buy", "long", "breakout")
    negative_keywords = ("bearish", "sell", "short", "dump")

    try:
        for index, tweet in enumerate(sntwitter.TwitterSearchScraper(TWITTER_QUERY).get_items()):
            if index >= TWITTER_SAMPLE_SIZE:
                break

            sample_count += 1
            text = getattr(tweet, "content", "") or getattr(tweet, "rawContent", "")
            content = text.lower()

            if any(keyword in content for keyword in positive_keywords):
                positive_hits += 1
            if any(keyword in content for keyword in negative_keywords):
                negative_hits += 1
    except Exception as exc:
        result = (0, [], f"Twitter 抓取失败: {exc}")
        twitter_cache["value"] = result
        twitter_cache["ts"] = now
        return result

    raw_score = positive_hits - negative_hits
    score = int(clamp(raw_score * 2, -12, 12))
    if score > 0:
        reasons = [f"Twitter 情绪偏多 ({positive_hits}/{negative_hits})"]
    elif score < 0:
        reasons = [f"Twitter 情绪偏空 ({positive_hits}/{negative_hits})"]
    else:
        reasons = []

    note = f"Twitter 样本 {sample_count} 条"
    result = (score, reasons, note)
    twitter_cache["value"] = result
    twitter_cache["ts"] = now
    return result


def funding_signal(rate: float) -> Tuple[int, List[str]]:
    if rate >= FUNDING_BEARISH_THRESHOLD:
        return -8, [f"Funding 偏热 ({rate:.5f})"]
    if rate <= -FUNDING_BULLISH_THRESHOLD:
        return 8, [f"Funding 偏冷 ({rate:.5f})"]
    return 0, []


def liquidation_signal(data: Dict[str, float]) -> Tuple[int, List[str], Optional[str]]:
    total_liq = data["total_liq"]
    long_liq = data["long_liq"]
    short_liq = data["short_liq"]

    if total_liq < MIN_LIQUIDATION_USD_1H:
        return 0, [], f"1h 爆仓规模不足 ({format_money(total_liq)} USD)"

    if long_liq >= short_liq * LIQUIDATION_IMBALANCE_RATIO:
        return 25, [f"多头踩踏主导 ({format_money(long_liq)} vs {format_money(short_liq)})"], None
    if short_liq >= long_liq * LIQUIDATION_IMBALANCE_RATIO:
        return -25, [f"空头挤压主导 ({format_money(short_liq)} vs {format_money(long_liq)})"], None
    return 0, [], "爆仓方向未形成明显失衡"


def etf_signal(etf: Dict[str, object]) -> Tuple[int, List[str], Optional[str]]:
    flow = etf.get("flow")
    if flow is None:
        return 0, [], "ETF Flow 未配置"
    if flow >= ETF_STRONG_INFLOW_MILLION:
        note = f"ETF 源: {etf.get('source', 'unknown')}"
        if etf.get("as_of"):
            note = f"{note}; 日期 {etf['as_of']}"
        return 20, [f"ETF 大幅流入 ({flow:.1f}M)"], note
    if flow <= ETF_STRONG_OUTFLOW_MILLION:
        note = f"ETF 源: {etf.get('source', 'unknown')}"
        if etf.get("as_of"):
            note = f"{note}; 日期 {etf['as_of']}"
        return -20, [f"ETF 明显流出 ({flow:.1f}M)"], note

    note = f"ETF 资金中性 ({flow:.1f}M); 源: {etf.get('source', 'unknown')}"
    if etf.get("as_of"):
        note = f"{note}; 日期 {etf['as_of']}"
    return 0, [], note


def total_signal():
    market = get_market_data()
    etf = get_etf_flow()
    macro = {}

    reasons: List[str] = []
    notes: List[str] = []
    score = 0

    macro_score, macro_reasons, macro_note, macro = macro_signal()
    twitter_score, twitter_reasons, twitter_note = twitter_sentiment()
    funding_score, funding_reasons = funding_signal(market["funding"])
    liq_score, liq_reasons, liq_note = liquidation_signal(market)
    etf_score, etf_reasons, etf_note = etf_signal(etf)

    score += macro_score + twitter_score + funding_score + liq_score + etf_score
    reasons.extend(macro_reasons + twitter_reasons + funding_reasons + liq_reasons + etf_reasons)

    for note in (macro_note, twitter_note, liq_note, etf_note):
        if note:
            notes.append(note)

    return score, reasons, notes, market, etf, macro


def decision(score: int) -> Tuple[str, float]:
    if score >= 35:
        return "强多", 0.8
    if score >= 15:
        return "偏多", 0.6
    if score <= -35:
        return "强空", 0.8
    if score <= -15:
        return "偏空", 0.6
    return "观望", 0.3


def risk(conf: float) -> Tuple[str, str, str]:
    if conf > 0.7:
        return "仓位80%", "止损3%", "止盈10%"
    if conf > 0.5:
        return "仓位50%", "止损2%", "止盈6%"
    return "仓位20%", "止损1%", "止盈3%"


def get_alert_key(decision_name: str, score: int) -> Optional[str]:
    if decision_name == "观望" and not SEND_OBSERVE_ALERTS:
        return None

    bucket = int(score / 10)
    return f"{decision_name}:{bucket}"


def should_send_alert(alert_key: Optional[str]) -> bool:
    if not alert_key:
        return False

    now = time.time()
    if last_alert["key"] == alert_key and now - last_alert["ts"] < ALERT_COOLDOWN_SECONDS:
        return False
    return True


def mark_alert_sent(alert_key: Optional[str]) -> None:
    if not alert_key:
        return
    last_alert["key"] = alert_key
    last_alert["ts"] = time.time()


def maybe_send_error_alert(exc: Exception) -> None:
    if not ENABLE_ALERTS:
        return

    key = f"{type(exc).__name__}:{exc}"
    now = time.time()
    if last_error_alert["key"] == key and now - last_error_alert["ts"] < ALERT_COOLDOWN_SECONDS:
        return

    last_error_alert["key"] = key
    last_error_alert["ts"] = now

    msg = (
        "⚠️ monnews 运行异常\n\n"
        f"🕒 {datetime.now().isoformat(sep=' ', timespec='seconds')}\n"
        f"错误: {type(exc).__name__}: {exc}"
    )
    try:
        send(msg)
    except Exception:
        pass


def build_message(
    score: int,
    reasons: List[str],
    notes: List[str],
    market: Dict[str, float],
    etf: Dict[str, object],
    macro: Dict[str, object],
    decision_name: str,
    conf: float,
    pos: str,
    sl: str,
    tp: str,
) -> str:
    reason_text = "; ".join(reasons) if reasons else "无明显共振信号"
    note_text = "; ".join(notes) if notes else "无"
    etf_value = "N/A" if etf["flow"] is None else f"{etf['flow']:.1f}M"
    etf_source = etf.get("source", "unknown")
    etf_date = etf.get("as_of") or "N/A"
    macro_headline = macro.get("headline_yoy")
    macro_source = macro.get("source", "unknown") if macro else "unknown"
    macro_date = macro.get("headline_date") if macro else None
    macro_text = "N/A"
    if macro_headline is not None:
        macro_text = f"{float(macro_headline):.2f}%"

    return f"""
📊 BTC 新闻+衍生品监控

🕒 {datetime.now().isoformat(sep=' ', timespec='seconds')}

📈 Score: {score}
🎯 决策: {decision_name}
🔥 置信度: {conf:.2f}

💰 ETF Flow: {etf_value} ({etf_source}, {etf_date})
🏛️ CPI YoY: {macro_text} ({macro_source}, {macro_date or 'N/A'})
💸 Funding: {market['funding']:.5f}

⚡ 1h 爆仓:
Long: {format_money(market['long_liq'])}
Short: {format_money(market['short_liq'])}
Total: {format_money(market['total_liq'])}

📍 理由:
- {reason_text}

📝 补充:
- {note_text}

📌 交易建议:
{pos}
{sl}
{tp}
""".strip()


def run() -> Dict[str, object]:
    print("monnews 运行中")

    score, reasons, notes, market, etf, macro = total_signal()
    decision_name, conf = decision(score)
    pos, sl, tp = risk(conf)
    msg = build_message(
        score, reasons, notes, market, etf, macro, decision_name, conf, pos, sl, tp
    )

    print(msg)

    sent = False
    alert_key = get_alert_key(decision_name, score)
    if should_send_alert(alert_key):
        sent = send(msg)
        if sent:
            mark_alert_sent(alert_key)

    return {
        "score": score,
        "decision": decision_name,
        "confidence": conf,
        "sent": sent,
        "market": market,
        "etf": etf,
        "macro": macro,
        "reasons": reasons,
        "notes": notes,
    }


def main() -> None:
    validate_config()
    run_once = env_bool("RUN_ONCE", False)

    if run_once:
        run()
        return

    while True:
        try:
            run()
        except Exception as exc:
            print(f"运行失败: {type(exc).__name__}: {exc}")
            maybe_send_error_alert(exc)
        time.sleep(max(LOOP_INTERVAL, 10))


if __name__ == "__main__":
    main()

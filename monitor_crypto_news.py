#!/usr/bin/env python3
"""
加密货币新闻监控脚本
使用 RSS 源，无需 API key
"""

import argparse
import hashlib
import hmac
import json
import os
import time
import base64
import urllib.parse
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser

# =========================
# RSS 新闻源配置
# =========================
RSS_FEEDS = {
    "decrypt": {
        "url": "https://decrypt.co/feed",
        "name": "Decrypt",
        "language": "en",
    },
    "bitcoinmagazine": {
        "url": "https://bitcoinmagazine.com/.rss",
        "name": "Bitcoin Magazine",
        "language": "en",
    },
    "coindesk": {
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "name": "CoinDesk",
        "language": "en",
    },
}

def _get_env():
    """延迟加载环境变量"""
    global DINGTALK_WEBHOOK, DINGTALK_SECRET, ENABLE_ALERTS, POLL_INTERVAL, ALERT_COOLDOWN_SECONDS, MAX_NEWS_PER_POLL
    DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK3")
    DINGTALK_SECRET = os.getenv("DINGTALK_SECRET3")
    ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "1") != "0"
    POLL_INTERVAL = int(os.getenv("NEWS_POLL_INTERVAL", "60"))
    ALERT_COOLDOWN_SECONDS = int(os.getenv("NEWS_ALERT_COOLDOWN", "60"))
    MAX_NEWS_PER_POLL = int(os.getenv("MAX_NEWS_PER_POLL", "10"))

_get_env()
ALERT_COOLDOWN_SECONDS = int(os.getenv("NEWS_ALERT_COOLDOWN", "60"))
MAX_NEWS_PER_POLL = int(os.getenv("MAX_NEWS_PER_POLL", "10"))

DEFAULT_KEYWORDS = ["BTC", "Bitcoin", "ETH", "Ethereum", "Fed", "SEC", "利率"]
KEYWORDS_ENV = os.getenv("NEWS_KEYWORDS", "")
MONITOR_KEYWORDS = [k.strip() for k in KEYWORDS_ENV.split(",") if k.strip()] or DEFAULT_KEYWORDS

EXCLUDE_KEYWORDS_ENV = os.getenv("NEWS_EXCLUDE_KEYWORDS", "")
EXCLUDE_KEYWORDS = [k.strip() for k in EXCLUDE_KEYWORDS_ENV.split(",") if k.strip()]

STATE_FILE = os.getenv("NEWS_STATE_FILE", "crypto_news_state.json")
PREFERRED_SOURCE = os.getenv("NEWS_PREFERRED_SOURCE", "")

http_session = requests.Session()
http_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(408, 429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
    ),
)

last_notifications = {
    "news_id": None,
    "ts": 0.0,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/rss+xml, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def fetch_rss_feed(feed_key: str, limit: int = 20) -> Optional[List[Dict]]:
    """
    获取 RSS 新闻源
    """
    try:
        feed_info = RSS_FEEDS.get(feed_key)
        if not feed_info:
            return None
        
        response = http_session.get(
            feed_info["url"],
            headers=HEADERS,
            timeout=15,
        )
        response.raise_for_status()
        
        feed = feedparser.parse(response.content)
        
        news_list = []
        for entry in feed.entries[:limit]:
            pub_time = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_time = time.mktime(entry.published_parsed)
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_time = time.mktime(entry.updated_parsed)
            
            if not pub_time:
                pub_time = time.time()
            
            content = ""
            if entry.get('summary'):
                content = entry['summary']
            elif entry.get('content') and len(entry['content']) > 0:
                content = entry['content'][0].get('value', '')
            elif entry.get('description'):
                content = entry['description']
            
            news_list.append({
                "id": hashlib.md5(entry.link.encode()).hexdigest()[:16],
                "title": entry.title,
                "content": content,
                "pubtime": int(pub_time),
                "source": feed_info["name"],
                "url": entry.link,
                "language": feed_info.get("language", "en"),
            })
        
        return news_list
    except Exception as e:
        print(f"{feed_key} 获取失败：{e}")
        return None


def fetch_news() -> Optional[List[Dict]]:
    """
    获取新闻（尝试多个源）
    """
    sources = list(RSS_FEEDS.keys())
    
    if PREFERRED_SOURCE and PREFERRED_SOURCE in sources:
        sources = [PREFERRED_SOURCE] + [s for s in sources if s != PREFERRED_SOURCE]
    
    for source_key in sources:
        print(f"尝试从 {RSS_FEEDS[source_key]['name']} 获取...")
        try:
            news = fetch_rss_feed(source_key, limit=20)
            if news:
                print(f"{RSS_FEEDS[source_key]['name']} 获取成功，共 {len(news)} 条")
                return news
        except Exception as e:
            print(f"{RSS_FEEDS[source_key]['name']} 失败：{e}")
    
    return None


def filter_news(news_list: List[Dict]) -> List[Dict]:
    """
    根据关键词过滤新闻
    """
    filtered = []
    
    for news in news_list:
        title = news.get("title", "") or ""
        content = news.get("content", "") or ""
        text = f"{title} {content}".lower()
        
        news_id = news.get("id")
        if not news_id:
            continue
        
        pubtime = news.get("pubtime", 0)
        if not pubtime:
            continue
        
        try:
            pubtime = int(pubtime)
        except:
            continue
        
        if pubtime < time.time() - 86400:
            continue
        
        should_exclude = False
        for exclude in EXCLUDE_KEYWORDS:
            if exclude.lower() in text:
                should_exclude = True
                break
        
        if should_exclude:
            continue
        
        matched_keywords = []
        for keyword in MONITOR_KEYWORDS:
            if keyword.lower() in text:
                matched_keywords.append(keyword)
        
        if matched_keywords:
            news["_matched_keywords"] = matched_keywords
            filtered.append(news)
    
    return filtered


def format_news_message(news: Dict) -> str:
    """
    格式化新闻消息
    """
    title = news.get("title", "") or ""
    content = news.get("content", "") or ""
    pubtime = news.get("pubtime", 0)
    source = news.get("source", "未知")
    matched = news.get("_matched_keywords", [])
    url = news.get("url", "")
    
    try:
        pubtime = int(pubtime)
        pub_datetime = datetime.fromtimestamp(pubtime, tz=timezone.utc)
        time_str = pub_datetime.strftime("%Y-%m-%d %H:%M:%S")
    except:
        time_str = str(pubtime)
    
    content_short = content[:200] + "..." if len(content) > 200 else content
    
    lines = [
        f"📰 加密货币新闻 ({source})",
        "",
        f"时间：{time_str} UTC",
        f"标题：{title}",
        f"摘要：{content_short}",
    ]
    
    if matched:
        lines.append(f"🏷️ 关键词：{', '.join(matched)}")
    
    if url:
        lines.append(f"🔗 {url}")
    
    return "\n".join(lines)


def send_dingtalk(msg: str) -> bool:
    """
    发送钉钉消息
    """
    if not ENABLE_ALERTS or not DINGTALK_WEBHOOK or not DINGTALK_SECRET:
        print(f"[通知] {msg}")
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
    
    try:
        response = http_session.post(
            url,
            json={"msgtype": "text", "text": {"content": msg}},
            timeout=10,
        )
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"钉钉发送失败：{e}")
        return False


def load_state() -> Dict:
    """加载状态文件"""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_state(state: Dict):
    """保存状态文件"""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def run_once(args) -> int:
    """
    执行一次监控
    """
    state = load_state()
    last_news_id = state.get("last_news_id")
    last_timestamp = state.get("last_timestamp", 0)
    
    news_list = fetch_news()
    
    if not news_list:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] 获取新闻失败")
        return 0
    
    filtered = filter_news(news_list)
    new_count = 0
    
    for news in reversed(filtered):
        news_id = news.get("id")
        pubtime = news.get("pubtime", 0)
        
        try:
            pubtime = int(pubtime)
        except:
            continue
        
        if last_news_id and news_id == last_news_id:
            continue
        
        if pubtime < last_timestamp:
            continue
        
        new_count += 1
        if new_count > args.limit:
            break
        
        msg = format_news_message(news)
        print(f"\n{'='*50}")
        print(msg)
        
        if ENABLE_ALERTS:
            send_dingtalk(msg)
    
    if filtered:
        latest = filtered[-1]
        state["last_timestamp"] = int(latest.get("pubtime", 0))
        state["last_news_id"] = latest.get("id")
        state["last_check"] = int(time.time())
        state["last_source"] = latest.get("source", "")
        save_state(state)
    
    print(f"\n[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
          f"检查完成，新消息：{new_count} 条")
    
    return new_count


def run(args):
    """主循环"""
    print(f"🚀 加密货币新闻监控启动")
    print(f"监控关键词：{', '.join(MONITOR_KEYWORDS)}")
    if EXCLUDE_KEYWORDS:
        print(f"排除关键词：{', '.join(EXCLUDE_KEYWORDS)}")
    print(f"轮询间隔：{POLL_INTERVAL} 秒")
    print(f"单次最多通知：{MAX_NEWS_PER_POLL} 条\n")
    
    while True:
        try:
            run_once(args)
        except Exception as e:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] 监控异常：{e}")
        
        if args.once:
            break
        
        time.sleep(POLL_INTERVAL)


def parse_args():
    parser = argparse.ArgumentParser(description="加密货币新闻监控（RSS 源）")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="每次获取新闻数量"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只执行一次"
    )
    parser.add_argument(
        "--keywords",
        type=str,
        default="",
        help="监控关键词，逗号分隔"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="",
        choices=["decrypt", "bitcoinmagazine", "coindesk"],
        help="优先使用某个新闻源"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    if args.keywords:
        global MONITOR_KEYWORDS
        MONITOR_KEYWORDS = [k.strip() for k in args.keywords.split(",") if k.strip()]
    
    if args.source:
        global PREFERRED_SOURCE
        PREFERRED_SOURCE = args.source
    
    run(args)


if __name__ == "__main__":
    main()

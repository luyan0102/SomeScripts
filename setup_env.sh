#!/bin/bash
# 环境变量配置脚本
# 运行方式：bash setup_env.sh

# =========================
# 钉钉通知配置
# =========================
echo 'export ENABLE_ALERTS=1' >> ~/.bashrc

# 钉钉机器人 1 - BTC 拐点信号 (4h/1h)
echo 'export DINGTALK_WEBHOOK1="https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN_1"' >> ~/.bashrc
echo 'export DINGTALK_SECRET1="SEC_YOUR_SECRET_1"' >> ~/.bashrc

# 钉钉机器人 2 - CoinGlass 监控
echo 'export DINGTALK_WEBHOOK2="https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN_2"' >> ~/.bashrc
echo 'export DINGTALK_SECRET2="SEC_YOUR_SECRET_2"' >> ~/.bashrc

# 钉钉机器人 3 - 加密货币新闻
echo 'export DINGTALK_WEBHOOK3="https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN_3"' >> ~/.bashrc
echo 'export DINGTALK_SECRET3="SEC_YOUR_SECRET_3"' >> ~/.bashrc


# =========================
# CoinGlass 监控配置 (monCoinglass.py)
# =========================
echo 'export COINGLASS_API_KEY="your_coinglass_api_key"' >> ~/.bashrc
echo 'export COINGLASS_BASE_URL="https://open-api-v4.coinglass.com"' >> ~/.bashrc
echo 'export COINGLASS_COIN_SYMBOL="BTC"' >> ~/.bashrc
echo 'export COINGLASS_EXCHANGE="Binance"' >> ~/.bashrc
echo 'export MIN_LIQUIDATION_USD_1H="500000"' >> ~/.bashrc
echo 'export LIQUIDATION_IMBALANCE_RATIO="2.0"' >> ~/.bashrc
echo 'export FUNDING_BEARISH_THRESHOLD="0.0005"' >> ~/.bashrc
echo 'export FUNDING_BULLISH_THRESHOLD="0.0005"' >> ~/.bashrc


# =========================
# 新闻监控配置 (monitor_crypto_news.py)
# =========================
echo 'export NEWS_POLL_INTERVAL="60"' >> ~/.bashrc
echo 'export NEWS_ALERT_COOLDOWN="60"' >> ~/.bashrc
echo 'export MAX_NEWS_PER_POLL="10"' >> ~/.bashrc
echo 'export NEWS_KEYWORDS="BTC,Bitcoin,ETH,Ethereum,Fed,SEC，利率"' >> ~/.bashrc
echo 'export NEWS_EXCLUDE_KEYWORDS=""' >> ~/.bashrc
echo 'export NEWS_STATE_FILE="crypto_news_state.json"' >> ~/.bashrc
echo 'export NEWS_PREFERRED_SOURCE=""' >> ~/.bashrc


# =========================
# 循环与超时配置
# =========================
echo 'export LOOP_INTERVAL="30"' >> ~/.bashrc
echo 'export ALERT_COOLDOWN_SECONDS="300"' >> ~/.bashrc
echo 'export REQUEST_TIMEOUT="10"' >> ~/.bashrc
echo 'export RUN_ONCE="0"' >> ~/.bashrc


# =========================
# 缓存配置
# =========================
echo 'export MARKET_CACHE_SECONDS="60"' >> ~/.bashrc
echo 'export ETF_CACHE_SECONDS="3600"' >> ~/.bashrc
echo 'export MACRO_CACHE_SECONDS="21600"' >> ~/.bashrc


# =========================
# ETF 流动配置
# =========================
echo 'export ETF_FLOW_MILLION=""' >> ~/.bashrc
echo 'export ETF_STRONG_INFLOW_MILLION="100"' >> ~/.bashrc
echo 'export ETF_STRONG_OUTFLOW_MILLION="-100"' >> ~/.bashrc
echo 'export ETF_FLOW_PATH="/api/etf/bitcoin/flow-history"' >> ~/.bashrc


# =========================
# 宏观经济数据配置 (FRED API)
# =========================
echo 'export FRED_API_KEY="your_fred_api_key"' >> ~/.bashrc
echo 'export FRED_BASE_URL="https://api.stlouisfed.org"' >> ~/.bashrc
echo 'export FRED_CPI_SERIES_ID="CPIAUCSL"' >> ~/.bashrc
echo 'export FRED_CORE_CPI_SERIES_ID="CPILFESL"' >> ~/.bashrc
echo 'export MACRO_ACTUAL=""' >> ~/.bashrc
echo 'export MACRO_FORECAST=""' >> ~/.bashrc
echo 'export MACRO_SURPRISE_THRESHOLD="0.2"' >> ~/.bashrc
echo 'export MACRO_COOLING_THRESHOLD="0.1"' >> ~/.bashrc
echo 'export MACRO_HEATING_THRESHOLD="0.1"' >> ~/.bashrc


# =========================
# Twitter 情绪监控 (可选)
# =========================
echo 'export ENABLE_TWITTER_SENTIMENT="0"' >> ~/.bashrc
echo "export TWITTER_QUERY='Bitcoin OR BTC Fed CPI lang:en'" >> ~/.bashrc
echo 'export TWITTER_SAMPLE_SIZE="20"' >> ~/.bashrc


echo ""
echo "✅ 环境变量已写入 ~/.bashrc"
echo "👉 请运行以下命令使配置生效：source ~/.bashrc"

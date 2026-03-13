
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt

# ======================================================================================
# AI Market Database Agent
# ======================================================================================
# This Streamlit application demonstrates:
# - Yahoo Finance market data ingestion
# - MySQL database integration
# - Quantitative analytics and portfolio optimization
# - AI-style SQL querying
# - Advanced SQL analytics using UNION, IN, ANY, CTE, Window Functions, and ROLLUP
# ======================================================================================

st.set_page_config(page_title="AI Market Data Agent", page_icon="📈", layout="wide")

st.title("AI Market Database Agent")
st.write("Financial Market Analytics Platform")
st.caption("Built by Zakariya Boutayeb — Data Science & Quantitative Finance")

# ======================================================================================
# TICKER LABELS
# Friendly labels for third-party users
# ======================================================================================
TICKER_LABELS = {
    # Stocks
    "AAPL": "AAPL — Apple",
    "MSFT": "MSFT — Microsoft",
    "AMD": "AMD — Advanced Micro Devices",
    "NVDA": "NVDA — NVIDIA",
    "GOOGL": "GOOGL — Alphabet",
    "AMZN": "AMZN — Amazon",
    "META": "META — Meta Platforms",
    "TSLA": "TSLA — Tesla",
    "ORCL": "ORCL — Oracle",
    "JPM": "JPM — JPMorgan Chase",

    # Index ETFs
    "SPY": "SPY — S&P 500 ETF",
    "QQQ": "QQQ — Nasdaq 100 ETF",
    "DIA": "DIA — Dow Jones ETF",
    "IWM": "IWM — Russell 2000 ETF",
    "VTI": "VTI — Total Stock Market ETF",
    "VOO": "VOO — Vanguard S&P 500 ETF",

    # Sector ETFs
    "XLF": "XLF — Financial Sector ETF",
    "XLK": "XLK — Technology Sector ETF",
    "XLE": "XLE — Energy Sector ETF",
    "XLV": "XLV — Healthcare Sector ETF",
    "XLI": "XLI — Industrials Sector ETF",
    "XLP": "XLP — Consumer Staples ETF",

    # Bond ETFs
    "TLT": "TLT — 20+ Year Treasury Bond ETF",
    "IEF": "IEF — 7-10 Year Treasury Bond ETF",
    "SHY": "SHY — 1-3 Year Treasury Bond ETF",
    "LQD": "LQD — Investment Grade Corporate Bond ETF",
    "HYG": "HYG — High Yield Bond ETF",
    "BND": "BND — Total Bond Market ETF",

    # Commodity ETFs
    "GLD": "GLD — Gold ETF",
    "SLV": "SLV — Silver ETF",
    "USO": "USO — Oil ETF",
    "UNG": "UNG — Natural Gas ETF",
    "DBC": "DBC — Broad Commodities ETF",

    # REITs
    "VNQ": "VNQ — Vanguard Real Estate ETF",
    "O": "O — Realty Income",
    "PLD": "PLD — Prologis",
    "SPG": "SPG — Simon Property Group",
    "EQIX": "EQIX — Equinix",

    # Crypto / Digital Asset Proxies
    "BTC-USD": "BTC-USD — Bitcoin Spot",
    "ETH-USD": "ETH-USD — Ethereum Spot",
    "IBIT": "IBIT — iShares Bitcoin Trust",
    "GBTC": "GBTC — Grayscale Bitcoin Trust",
}

# ======================================================================================
# TICKER UNIVERSE BY CATEGORY
# ======================================================================================
TICKER_OPTIONS = {
    "Stocks": [
        "AAPL", "MSFT", "AMD", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "ORCL", "JPM"
    ],
    "Index ETFs": [
        "SPY", "QQQ", "DIA", "IWM", "VTI", "VOO"
    ],
    "Sector ETFs": [
        "XLF", "XLK", "XLE", "XLV", "XLI", "XLP"
    ],
    "Bond ETFs": [
        "TLT", "IEF", "SHY", "LQD", "HYG", "BND"
    ],
    "Commodity ETFs": [
        "GLD", "SLV", "USO", "UNG", "DBC"
    ],
    "REITs": [
        "VNQ", "O", "PLD", "SPG", "EQIX"
    ],
    "Crypto / Digital Asset Proxies": [
        "BTC-USD", "ETH-USD", "IBIT", "GBTC"
    ]
}

ALL_TICKERS = [ticker for tickers in TICKER_OPTIONS.values() for ticker in tickers]

# ======================================================================================
# HELPER FUNCTIONS FOR LABELS
# ======================================================================================
def ticker_to_label(ticker: str) -> str:
    return TICKER_LABELS.get(ticker, ticker)

def label_to_ticker(label: str) -> str:
    for ticker, display_label in TICKER_LABELS.items():
        if display_label == label:
            return ticker
    return label

def tickers_to_labels(ticker_list):
    return [ticker_to_label(t) for t in ticker_list]

def labels_to_tickers(label_list):
    return [label_to_ticker(label) for label in label_list]

# ======================================================================================
# SIDEBAR NAVIGATION
# ======================================================================================
st.sidebar.header("Navigation")

menu = st.sidebar.selectbox(
    "Menu",
    [
        "Quick Market Chart",
        "Download Market Data",
        "Latest Price",
        "Returns Analysis",
        "Compare Returns",
        "Portfolio Analytics",
        "AI SQL Agent",
        "Advanced SQL Analytics"
    ]
)

# ======================================================================================
# DATABASE CONNECTION
# Reads hosted/local credentials from Streamlit secrets
# ======================================================================================
def get_connection():
    try:
        conn = mysql.connector.connect(
            host=st.secrets["DB_HOST"],
            port=int(st.secrets["DB_PORT"]),
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASS"],
            database=st.secrets["DB_NAME"],
        )
        return conn
    except Error as e:
        st.error(f"Database connection error: {e}")
        return None

# ======================================================================================
# TEST CONNECTION
# ======================================================================================
if st.sidebar.button("Test DB Connection"):
    conn = get_connection()
    if conn and conn.is_connected():
        st.sidebar.success("MySQL connection successful")
        conn.close()
    else:
        st.sidebar.error("MySQL connection failed")

# ======================================================================================
# ENSURE TICKER EXISTS
# Inserts ticker if not already in database, then returns ticker_id
# ======================================================================================
def ensure_ticker(symbol):
    conn = get_connection()
    if conn is None:
        return None

    cursor = conn.cursor()

    try:
        clean_symbol = symbol.upper().strip()

        cursor.execute(
            """
            INSERT INTO tickers(symbol)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE symbol = VALUES(symbol)
            """,
            (clean_symbol,)
        )

        cursor.execute(
            "SELECT id FROM tickers WHERE symbol = %s",
            (clean_symbol,)
        )
        row = cursor.fetchone()
        conn.commit()

        return row[0] if row else None

    finally:
        cursor.close()
        conn.close()

# ======================================================================================
# DOWNLOAD PRICES FROM YAHOO FINANCE
# ======================================================================================
def download_prices(symbol, startdate, enddate):
    df = yf.download(
        symbol,
        start=startdate,
        end=enddate,
        auto_adjust=False,
        progress=False
    )

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index()
    return df

# ======================================================================================
# INSERT PRICE DATA INTO MYSQL
# ======================================================================================
def insert_prices(symbol, startdate, enddate):
    ticker_id = ensure_ticker(symbol)
    if ticker_id is None:
        return 0

    df = download_prices(symbol, startdate, enddate)
    if df.empty:
        return 0

    conn = get_connection()
    if conn is None:
        return 0

    cursor = conn.cursor()

    try:
        for _, row in df.iterrows():
            adj_close_value = row["Adj Close"] if "Adj Close" in df.columns else row["Close"]

            cursor.execute(
                """
                INSERT INTO price_daily
                (ticker_id, dt, open, high, low, close, adj_close, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    adj_close = VALUES(adj_close),
                    volume = VALUES(volume)
                """,
                (
                    ticker_id,
                    row["Date"],
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    adj_close_value,
                    row["Volume"]
                )
            )

        conn.commit()
        return len(df)

    finally:
        cursor.close()
        conn.close()

# ======================================================================================
# QUERY: GET LATEST PRICE
# ======================================================================================
def get_latest_price(symbol):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    SELECT p.dt, p.close, p.adj_close
    FROM price_daily p
    JOIN tickers t ON p.ticker_id = t.id
    WHERE t.symbol = %s
    ORDER BY p.dt DESC
    LIMIT 1
    """

    try:
        return pd.read_sql(query, conn, params=[symbol.upper().strip()])
    finally:
        conn.close()

# ======================================================================================
# QUERY: GET RETURNS
# ======================================================================================
def get_returns(symbol):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    SELECT p.dt, p.adj_close
    FROM price_daily p
    JOIN tickers t ON p.ticker_id = t.id
    WHERE t.symbol = %s
    ORDER BY p.dt
    """

    try:
        df = pd.read_sql(query, conn, params=[symbol.upper().strip()])
        if df.empty:
            return df

        df["returns"] = df["adj_close"].pct_change(fill_method=None)
        return df

    finally:
        conn.close()

# ======================================================================================
# QUERY: PRICE MATRIX FOR MULTIPLE TICKERS
# ======================================================================================
def get_price_matrix(stock_list, startdate, enddate):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    clean_list = [s.upper().strip() for s in stock_list if s.strip()]
    if not clean_list:
        return pd.DataFrame()

    placeholders = ",".join(["%s"] * len(clean_list))
    query = f"""
        SELECT t.symbol, p.dt, p.adj_close
        FROM price_daily p
        JOIN tickers t ON p.ticker_id = t.id
        WHERE t.symbol IN ({placeholders})
          AND p.dt BETWEEN %s AND %s
        ORDER BY p.dt
    """
    params = clean_list + [startdate, enddate]

    try:
        df = pd.read_sql(query, conn, params=params)
        if df.empty:
            return pd.DataFrame()

        return df.pivot(index="dt", columns="symbol", values="adj_close")

    finally:
        conn.close()

# ======================================================================================
# HELPER: EXECUTE ANY SELECT QUERY AND RETURN DATAFRAME
# ======================================================================================
def run_sql_query(query, params=None):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()

# ======================================================================================
# PAGE 1 - QUICK MARKET CHART
# Quick external chart from Yahoo Finance without DB dependency
# ======================================================================================
if menu == "Quick Market Chart":
    st.subheader("Quick Market Chart")

    ticker_label = st.selectbox(
        "Select Ticker",
        options=tickers_to_labels(ALL_TICKERS),
        index=0
    )
    ticker = label_to_ticker(ticker_label)

    chart_start = st.date_input("Chart Start Date", value=pd.to_datetime("2020-01-01"))

    data = yf.download(ticker, start=chart_start, auto_adjust=False, progress=False)

    if not data.empty:
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [c[0] for c in data.columns]
        st.line_chart(data["Close"])
        st.dataframe(data.tail(10), use_container_width=True)
    else:
        st.warning(f"No Yahoo data found for {ticker}.")

# ======================================================================================
# PAGE 2 - DOWNLOAD MARKET DATA
# Category selection + ticker dropdowns + DB ingestion
# ======================================================================================
elif menu == "Download Market Data":
    st.subheader("Download Market Data")

    selected_categories = st.multiselect(
        "Select asset categories",
        options=list(TICKER_OPTIONS.keys()),
        default=["Stocks", "Index ETFs"]
    )

    filtered_tickers = []
    for category in selected_categories:
        filtered_tickers.extend(TICKER_OPTIONS[category])

    default_download = [ticker for ticker in ["AMD", "AAPL", "MSFT", "ORCL"] if ticker in filtered_tickers]

    selected_ticker_labels = st.multiselect(
        "Select tickers to download",
        options=tickers_to_labels(filtered_tickers),
        default=tickers_to_labels(default_download)
    )
    selected_tickers = labels_to_tickers(selected_ticker_labels)

    startdate = st.date_input("Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("End Date", value=pd.to_datetime("2024-11-18"))

    if st.button("Download"):
        if not selected_categories:
            st.warning("Please select at least one asset category.")
        elif not selected_tickers:
            st.warning("Please select at least one ticker.")
        elif startdate >= enddate:
            st.warning("Start date must be earlier than end date.")
        else:
            total_rows = 0
            results = {}

            for symbol in selected_tickers:
                rows = insert_prices(symbol, startdate, enddate)
                total_rows += rows
                results[symbol] = rows

            st.success(f"Inserted/updated {total_rows} rows for {len(selected_tickers)} tickers.")
            st.dataframe(
                pd.DataFrame(
                    [{"Ticker": k, "Rows Inserted/Updated": v} for k, v in results.items()]
                ),
                use_container_width=True
            )

# ======================================================================================
# PAGE 3 - LATEST PRICE
# Single-ticker latest stored price from DB
# ======================================================================================
elif menu == "Latest Price":
    st.subheader("Latest Price")

    symbol_label = st.selectbox(
        "Select Ticker",
        options=tickers_to_labels(ALL_TICKERS),
        index=0
    )
    symbol = label_to_ticker(symbol_label)

    df = get_latest_price(symbol)
    if df.empty:
        st.warning(f"No price data found for {symbol}. Download it first.")
    else:
        st.dataframe(df, use_container_width=True)

# ======================================================================================
# PAGE 4 - RETURNS ANALYSIS
# Single-ticker return series from DB
# ======================================================================================
elif menu == "Returns Analysis":
    st.subheader("Returns Analysis")

    symbol_label = st.selectbox(
        "Select Ticker",
        options=tickers_to_labels(ALL_TICKERS),
        index=0
    )
    symbol = label_to_ticker(symbol_label)

    df = get_returns(symbol)
    if not df.empty:
        st.line_chart(df["returns"].dropna())
        st.dataframe(df.tail(10), use_container_width=True)
    else:
        st.warning(f"No return data found for {symbol}.")

# ======================================================================================
# PAGE 5 - COMPARE RETURNS
# Multi-asset normalized comparison
# ======================================================================================
elif menu == "Compare Returns":
    st.subheader("Compare Returns")

    selected_categories = st.multiselect(
        "Select asset categories",
        options=list(TICKER_OPTIONS.keys()),
        default=["Stocks", "Index ETFs", "Commodity ETFs", "REITs"]
    )

    filtered_tickers = []
    for category in selected_categories:
        filtered_tickers.extend(TICKER_OPTIONS[category])

    default_compare = [ticker for ticker in ["SPY", "QQQ", "GLD", "VNQ"] if ticker in filtered_tickers]

    selected_ticker_labels = st.multiselect(
        "Select tickers to compare",
        options=tickers_to_labels(filtered_tickers),
        default=tickers_to_labels(default_compare)
    )
    selected_tickers = labels_to_tickers(selected_ticker_labels)

    startdate = st.date_input("Compare Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("Compare End Date", value=pd.to_datetime("2024-11-18"))

    if not selected_categories:
        st.info("Select one or more asset categories.")
    elif not selected_tickers:
        st.info("Select at least one ticker to compare.")
    elif startdate >= enddate:
        st.warning("Start date must be earlier than end date.")
    else:
        prices = get_price_matrix(selected_tickers, startdate, enddate)

        if not prices.empty:
            normalized = prices / prices.iloc[0]
            st.write("### Normalized Price Comparison")
            st.line_chart(normalized)

            st.write("### Latest Normalized Values")
            st.dataframe(normalized.tail(10), use_container_width=True)
        else:
            st.warning("No data found for the selected tickers and date range.")

# ======================================================================================
# PAGE 6 - PORTFOLIO ANALYTICS
# Equal-weight metrics + Monte Carlo simulation
# ======================================================================================
elif menu == "Portfolio Analytics":
    st.subheader("Portfolio Analytics")

    selected_categories = st.multiselect(
        "Select asset categories",
        options=list(TICKER_OPTIONS.keys()),
        default=["Stocks", "Index ETFs"]
    )

    filtered_tickers = []
    for category in selected_categories:
        filtered_tickers.extend(TICKER_OPTIONS[category])

    default_portfolio = [ticker for ticker in ["AMD", "AAPL", "MSFT", "ORCL"] if ticker in filtered_tickers]

    selected_ticker_labels = st.multiselect(
        "Select portfolio assets",
        options=tickers_to_labels(filtered_tickers),
        default=tickers_to_labels(default_portfolio)
    )
    selected_tickers = labels_to_tickers(selected_ticker_labels)

    startdate = st.date_input("Portfolio Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("Portfolio End Date", value=pd.to_datetime("2024-11-18"))
    num_simulations = st.number_input("Monte Carlo Simulations", value=10000, step=1000, min_value=1000)

    if not selected_categories:
        st.info("Select one or more asset categories.")
    elif len(selected_tickers) < 2:
        st.info("Select at least two assets to build a portfolio.")
    elif startdate >= enddate:
        st.warning("Start date must be earlier than end date.")
    else:
        prices = get_price_matrix(selected_tickers, startdate, enddate)

        if not prices.empty:
            returns = prices.pct_change(fill_method=None).dropna()

            if returns.empty:
                st.warning("Not enough return data to run portfolio analytics.")
            else:
                n = len(selected_tickers)
                equal_weights = np.array([1 / n] * n)
                portfolio_returns = returns.dot(equal_weights)

                cumulative_return = (1 + portfolio_returns).prod() - 1
                mean_daily_return = portfolio_returns.mean()
                std_daily_return = portfolio_returns.std()
                sharpe_ratio = mean_daily_return / std_daily_return if std_daily_return != 0 else np.nan
                annualized_sharpe = sharpe_ratio * np.sqrt(252) if pd.notna(sharpe_ratio) else np.nan

                st.write("### Equal-Weighted Portfolio Metrics")
                metrics_df = pd.DataFrame({
                    "Metric": [
                        "Cumulative Return",
                        "Mean Daily Return",
                        "Standard Deviation Daily Return",
                        "Sharpe Ratio",
                        "Annualized Sharpe Ratio"
                    ],
                    "Value": [
                        cumulative_return,
                        mean_daily_return,
                        std_daily_return,
                        sharpe_ratio,
                        annualized_sharpe
                    ]
                })
                st.dataframe(metrics_df, use_container_width=True)

                mean_returns = returns.mean()
                cov_matrix = returns.cov()

                results = []
                weights_record = []

                for _ in range(int(num_simulations)):
                    weights = np.random.random(len(selected_tickers))
                    weights /= np.sum(weights)

                    port_return = np.sum(mean_returns * weights)
                    port_std = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
                    sharpe = port_return / port_std if port_std != 0 else 0
                    annual_sharpe = sharpe * np.sqrt(252)

                    results.append([port_return, port_std, sharpe, annual_sharpe])
                    weights_record.append(weights)

                results_df = pd.DataFrame(
                    results,
                    columns=["mean_return", "std_dev", "sharpe", "annualized_sharpe"]
                )
                weights_df = pd.DataFrame(weights_record, columns=selected_tickers)
                sim_df = pd.concat([results_df, weights_df], axis=1)

                optimal_idx = sim_df["annualized_sharpe"].idxmax()
                optimal = sim_df.loc[optimal_idx]

                st.write("### Optimal Portfolio Weights")
                optimal_weights_df = pd.DataFrame({
                    "Ticker": selected_tickers,
                    "Weight": [optimal[t] for t in selected_tickers]
                })
                st.dataframe(optimal_weights_df, use_container_width=True)

                st.write("### Optimal Portfolio Metrics")
                optimal_metrics_df = pd.DataFrame({
                    "Metric": ["Mean Return", "Risk (Std Dev)", "Sharpe Ratio", "Annualized Sharpe Ratio"],
                    "Value": [
                        optimal["mean_return"],
                        optimal["std_dev"],
                        optimal["sharpe"],
                        optimal["annualized_sharpe"]
                    ]
                })
                st.dataframe(optimal_metrics_df, use_container_width=True)

                fig, ax = plt.subplots(figsize=(10, 6))
                scatter = ax.scatter(
                    sim_df["std_dev"],
                    sim_df["mean_return"],
                    c=sim_df["annualized_sharpe"],
                    cmap="viridis",
                    alpha=0.5
                )
                ax.scatter(
                    optimal["std_dev"],
                    optimal["mean_return"],
                    marker="*",
                    s=300,
                    label="Optimal Portfolio"
                )
                ax.set_xlabel("Portfolio Risk (Std Dev)")
                ax.set_ylabel("Portfolio Mean Return")
                ax.set_title("Monte Carlo Portfolio Simulation")
                ax.legend()
                plt.colorbar(scatter, ax=ax, label="Annualized Sharpe Ratio")
                st.pyplot(fig)

        else:
            st.warning("No price data found for selected assets and dates.")

# ======================================================================================
# PAGE 7 - AI SQL AGENT
# Rule-based natural-language to SQL interface
# ======================================================================================
elif menu == "AI SQL Agent":
    st.subheader("AI SQL Agent")

    user_prompt = st.text_area(
        "Ask a question about your market database",
        value="Show me the latest prices for all tickers"
    )

    show_sql = st.checkbox("Show generated SQL", value=False)

    if st.button("Run AI Query"):
        prompt_lower = user_prompt.lower()
        query = None

        if "latest prices" in prompt_lower or "latest price" in prompt_lower:
            query = """
            SELECT t.symbol, p.dt AS latest_date, p.close AS latest_close
            FROM price_daily p
            JOIN tickers t ON p.ticker_id = t.id
            JOIN (
                SELECT ticker_id, MAX(dt) AS max_dt
                FROM price_daily
                GROUP BY ticker_id
            ) latest
            ON p.ticker_id = latest.ticker_id
            AND p.dt = latest.max_dt
            ORDER BY t.symbol
            """
        elif "most volatile" in prompt_lower:
            query = """
            SELECT t.symbol, STDDEV(p.adj_close) AS volatility
            FROM price_daily p
            JOIN tickers t ON p.ticker_id = t.id
            GROUP BY t.symbol
            ORDER BY volatility DESC
            LIMIT 10
            """
        elif "count tickers" in prompt_lower:
            query = "SELECT COUNT(*) AS ticker_count FROM tickers"

        if query is None:
            st.warning("No SQL rule matched your request yet. Add more prompt-to-SQL rules.")
        else:
            df = run_sql_query(query)

            if show_sql:
                st.code(query, language="sql")

            st.dataframe(df, use_container_width=True)

# ======================================================================================
# PAGE 8 - ADVANCED SQL ANALYTICS
# Demonstrates advanced SQL concepts on market database
# ======================================================================================
elif menu == "Advanced SQL Analytics":
    st.subheader("Advanced SQL Analytics")

    sql_topic = st.selectbox(
        "Select SQL topic",
        [
            "Set Operations (UNION)",
            "Set Membership (IN)",
            "Set Comparison (ANY)",
            "CTE Query (WITH)",
            "Window Functions (DENSE_RANK)",
            "OLAP Query (ROLLUP)"
        ]
    )

    show_sql = st.checkbox("Show SQL code", value=True)

    query = None
    explanation = ""

    if sql_topic == "Set Operations (UNION)":
        explanation = "Show tickers that are either technology-related benchmark assets or defensive assets."
        query = """
        SELECT symbol
        FROM tickers
        WHERE symbol IN ('AAPL', 'MSFT', 'NVDA', 'QQQ')
        UNION
        SELECT symbol
        FROM tickers
        WHERE symbol IN ('GLD', 'TLT', 'VNQ', 'XLP')
        ORDER BY symbol
        """

    elif sql_topic == "Set Membership (IN)":
        explanation = "Show recent stored prices for a selected portfolio universe using IN."
        query = """
        SELECT t.symbol, p.dt, p.close
        FROM price_daily p
        JOIN tickers t ON p.ticker_id = t.id
        WHERE t.symbol IN ('AAPL', 'MSFT', 'AMD', 'ORCL')
        ORDER BY p.dt DESC
        LIMIT 20
        """

    elif sql_topic == "Set Comparison (ANY)":
        explanation = "Show assets whose latest adjusted close is greater than at least one benchmark asset."
        query = """
        SELECT t.symbol, p.adj_close
        FROM price_daily p
        JOIN tickers t ON p.ticker_id = t.id
        WHERE (t.symbol, p.dt) IN (
            SELECT t2.symbol, MAX(p2.dt)
            FROM price_daily p2
            JOIN tickers t2 ON p2.ticker_id = t2.id
            GROUP BY t2.symbol
        )
        AND p.adj_close > ANY (
            SELECT p3.adj_close
            FROM price_daily p3
            JOIN tickers t3 ON p3.ticker_id = t3.id
            WHERE t3.symbol IN ('GLD', 'TLT', 'VNQ')
              AND (t3.symbol, p3.dt) IN (
                  SELECT t4.symbol, MAX(p4.dt)
                  FROM price_daily p4
                  JOIN tickers t4 ON p4.ticker_id = t4.id
                  GROUP BY t4.symbol
              )
        )
        ORDER BY p.adj_close DESC
        """

    elif sql_topic == "CTE Query (WITH)":
        explanation = "Use a CTE to calculate average adjusted close by ticker."
        query = """
        WITH avg_prices AS (
            SELECT t.symbol, AVG(p.adj_close) AS avg_adj_close
            FROM price_daily p
            JOIN tickers t ON p.ticker_id = t.id
            GROUP BY t.symbol
        )
        SELECT *
        FROM avg_prices
        ORDER BY avg_adj_close DESC
        LIMIT 15
        """

    elif sql_topic == "Window Functions (DENSE_RANK)":
        explanation = "Rank assets by average adjusted close using DENSE_RANK."
        query = """
        SELECT
            symbol,
            avg_adj_close,
            DENSE_RANK() OVER (ORDER BY avg_adj_close DESC) AS price_rank
        FROM (
            SELECT t.symbol, AVG(p.adj_close) AS avg_adj_close
            FROM price_daily p
            JOIN tickers t ON p.ticker_id = t.id
            GROUP BY t.symbol
        ) ranked_assets
        ORDER BY price_rank
        LIMIT 15
        """

    elif sql_topic == "OLAP Query (ROLLUP)":
        explanation = "Aggregate the number of stored price rows by symbol, plus a grand total using ROLLUP."
        query = """
        SELECT
            COALESCE(t.symbol, 'ALL TICKERS') AS symbol,
            COUNT(*) AS row_count
        FROM price_daily p
        JOIN tickers t ON p.ticker_id = t.id
        GROUP BY t.symbol WITH ROLLUP
        """

    st.write(explanation)

    if query is not None:
        df = run_sql_query(query)

        if show_sql:
            st.code(query, language="sql")

        st.dataframe(df, use_container_width=True)

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(page_title="AI Market Data Agent", page_icon="📈", layout="wide")

st.title("AI Market Database Agent")
st.write("Financial Market Analytics Platform")
st.caption("Built by Zakariya Boutayeb — Data Science & Quantitative Finance")

# =========================================================
# SIDEBAR
# =========================================================
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
        "AI SQL Agent"
    ]
)

# =========================================================
# DATABASE CONNECTION
# =========================================================
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

# =========================================================
# TEST CONNECTION
# =========================================================
if st.sidebar.button("Test DB Connection"):
    conn = get_connection()
    if conn and conn.is_connected():
        st.sidebar.success("MySQL connection successful")
        conn.close()
    else:
        st.sidebar.error("MySQL connection failed")

# =========================================================
# ENSURE TICKER EXISTS
# =========================================================
def ensure_ticker(symbol):
    conn = get_connection()
    if conn is None:
        return None

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO tickers(symbol)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE symbol = VALUES(symbol)
            """,
            (symbol.upper().strip(),)
        )

        cursor.execute(
            "SELECT id FROM tickers WHERE symbol = %s",
            (symbol.upper().strip(),)
        )
        row = cursor.fetchone()
        conn.commit()

        return row[0] if row else None

    finally:
        cursor.close()
        conn.close()

# =========================================================
# DOWNLOAD PRICES FROM YAHOO
# =========================================================
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

# =========================================================
# INSERT PRICE DATA
# =========================================================
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

# =========================================================
# QUERY: LATEST PRICE
# =========================================================
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
        df = pd.read_sql(query, conn, params=[symbol.upper().strip()])
        return df
    finally:
        conn.close()

# =========================================================
# QUERY: RETURNS
# =========================================================
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

# =========================================================
# QUERY: PRICE MATRIX
# =========================================================
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

# =========================================================
# PAGE 1 - QUICK MARKET CHART
# =========================================================
if menu == "Quick Market Chart":
    st.subheader("Quick Market Chart")

    ticker = st.text_input("Enter Ticker", "AAPL").upper().strip()
    chart_start = st.date_input("Chart Start Date", value=pd.to_datetime("2020-01-01"))

    data = yf.download(ticker, start=chart_start, auto_adjust=False, progress=False)

    if not data.empty:
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [c[0] for c in data.columns]
        st.line_chart(data["Close"])
        st.dataframe(data.tail(10), use_container_width=True)
    else:
        st.warning(f"No Yahoo data found for {ticker}.")

# =========================================================
# PAGE 2 - DOWNLOAD MARKET DATA
# =========================================================
elif menu == "Download Market Data":
    st.subheader("Download Market Data")

    symbols_text = st.text_area("Tickers (comma separated)", value="AMD,AAPL,MSFT,ORCL")
    startdate = st.date_input("Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("End Date", value=pd.to_datetime("2024-11-18"))

    if st.button("Download"):
        symbols = [s.strip().upper() for s in symbols_text.split(",") if s.strip()]
        total_rows = 0

        for symbol in symbols:
            rows = insert_prices(symbol, startdate, enddate)
            total_rows += rows

        st.success(f"Inserted/updated {total_rows} rows for {len(symbols)} tickers.")

# =========================================================
# PAGE 3 - LATEST PRICE
# =========================================================
elif menu == "Latest Price":
    st.subheader("Latest Price")

    symbol = st.text_input("Ticker", value="AAPL").upper().strip()

    if symbol:
        df = get_latest_price(symbol)
        if df.empty:
            st.warning(f"No price data found for {symbol}. Download it first.")
        else:
            st.dataframe(df, use_container_width=True)

# =========================================================
# PAGE 4 - RETURNS ANALYSIS
# =========================================================
elif menu == "Returns Analysis":
    st.subheader("Returns Analysis")
    symbol = st.text_input("Ticker", value="AAPL").upper().strip()

    if symbol:
        df = get_returns(symbol)
        if not df.empty:
            st.line_chart(df["returns"].dropna())
            st.dataframe(df.tail(10), use_container_width=True)
        else:
            st.warning(f"No return data found for {symbol}.")

# =========================================================
# PAGE 5 - RETURNS COMPARISON
# =========================================================
elif menu == "Compare Returns":
    st.subheader("Compare Returns")

    symbols_text = st.text_input("Tickers", value="AMD,AAPL,MSFT,ORCL")
    startdate = st.date_input("Compare Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("Compare End Date", value=pd.to_datetime("2024-11-18"))

    symbols = [s.strip().upper() for s in symbols_text.split(",") if s.strip()]
    prices = get_price_matrix(symbols, startdate, enddate)

    if not prices.empty:
        normalized = prices / prices.iloc[0]
        st.line_chart(normalized)
        st.dataframe(normalized.tail(10), use_container_width=True)
    else:
        st.warning("No data found for comparison.")

# =========================================================
# PAGE 6 - PORTFOLIO ANALYTICS
# =========================================================
elif menu == "Portfolio Analytics":
    st.subheader("Portfolio Analytics")

    symbols_text = st.text_input("Portfolio Tickers", value="AMD,AAPL,MSFT,ORCL")
    startdate = st.date_input("Portfolio Start Date", value=pd.to_datetime("2019-01-01"))
    enddate = st.date_input("Portfolio End Date", value=pd.to_datetime("2024-11-18"))
    num_simulations = st.number_input("Monte Carlo Simulations", value=10000, step=1000)

    symbols = [s.strip().upper() for s in symbols_text.split(",") if s.strip()]
    prices = get_price_matrix(symbols, startdate, enddate)

    if not prices.empty:
        returns = prices.pct_change(fill_method=None).dropna()

        if returns.empty:
            st.warning("Not enough return data to run portfolio analytics.")
        else:
            n = len(symbols)
            equal_weights = np.array([1 / n] * n)
            portfolio_returns = returns.dot(equal_weights)

            cumulative_return = (1 + portfolio_returns).prod() - 1
            mean_daily_return = portfolio_returns.mean()
            std_daily_return = portfolio_returns.std()
            sharpe_ratio = mean_daily_return / std_daily_return if std_daily_return != 0 else np.nan
            annualized_sharpe = sharpe_ratio * np.sqrt(252) if pd.notna(sharpe_ratio) else np.nan

            st.write("### Equal-Weighted Portfolio Metrics")
            st.write(f"Cumulative Return: {cumulative_return:.4f}")
            st.write(f"Mean Daily Return: {mean_daily_return:.6f}")
            st.write(f"Standard Deviation Daily Return: {std_daily_return:.6f}")
            st.write(f"Sharpe Ratio: {sharpe_ratio:.6f}")
            st.write(f"Annualized Sharpe Ratio: {annualized_sharpe:.6f}")

            mean_returns = returns.mean()
            cov_matrix = returns.cov()

            results = []
            weights_record = []

            for _ in range(int(num_simulations)):
                weights = np.random.random(len(symbols))
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
            weights_df = pd.DataFrame(weights_record, columns=symbols)
            sim_df = pd.concat([results_df, weights_df], axis=1)

            optimal_idx = sim_df["annualized_sharpe"].idxmax()
            optimal = sim_df.loc[optimal_idx]

            st.write("### Optimal Portfolio")
            st.dataframe(optimal.to_frame(name="value"))

            fig, ax = plt.subplots(figsize=(10, 6))
            scatter = ax.scatter(
                sim_df["std_dev"],
                sim_df["mean_return"],
                c=sim_df["annualized_sharpe"],
                cmap="viridis",
                alpha=0.5
            )
            ax.scatter(optimal["std_dev"], optimal["mean_return"], marker="*", s=300)
            ax.set_xlabel("Risk (Std Dev)")
            ax.set_ylabel("Mean Return")
            ax.set_title("Monte Carlo Portfolio Simulation")
            plt.colorbar(scatter, ax=ax, label="Annualized Sharpe Ratio")
            st.pyplot(fig)

    else:
        st.warning("No price data found for selected tickers and dates.")

# =========================================================
# PAGE 7 - AI SQL DATABASE AGENT
# =========================================================
elif menu == "AI SQL Agent":
    st.subheader("AI SQL Agent")

    user_prompt = st.text_area(
        "Ask a question about your market database",
        value="Show me the latest prices for all tickers"
    )

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
            conn = get_connection()
            if conn is not None:
                df = pd.read_sql(query, conn)
                conn.close()

                st.code(query, language="sql")
                st.dataframe(df, use_container_width=True)
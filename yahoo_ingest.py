#!/usr/bin/env python
# coding: utf-8

# ## Stock Market Data Ingestion (ingest_yahoo.py)
# 
# The application retrieves daily stock market data from Yahoo Finance using the yfinance API.
# 
# For each ticker, the system collects:
# - Open price
# - High price
# - Low price
# - Close price
# - Adjusted close price
# - Trading volume
# 
# The data is automatically stored in a MySQL database.

# In[ ]:


from db import get_connection

def ensure_ticker(symbol: str) -> int | None:
    """
    Insert ticker if it does not exist, then return ticker_id.
    """
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
            (symbol.upper().strip(),),
        )

        cursor.execute(
            "SELECT id FROM tickers WHERE symbol = %s",
            (symbol.upper().strip(),),
        )

        row = cursor.fetchone()
        conn.commit()

        return row[0] if row else None

    finally:
        cursor.close()
        conn.close()


# In[ ]:


def download_prices(symbol: str, startdate: str, enddate: str) -> pd.DataFrame:
    """
    Download Yahoo Finance daily prices for a symbol.
    """
    df = yf.download(
        symbol,
        start=startdate,
        end=enddate,
        auto_adjust=False,
        progress=False,
    )

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index()
    return df


# In[ ]:


def insert_prices(symbol: str, startdate: str, enddate: str) -> int:
    """
    Download and insert/update price data into price_daily.
    Returns number of rows processed.
    """
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                    row["Volume"],
                ),
            )

        conn.commit()
        return len(df)

    finally:
        cursor.close()
        conn.close()


# In[ ]:


def insert_multiple_tickers(stock_list: list[str], startdate: str, enddate: str) -> dict[str, int]:
    """
    Insert/update multiple tickers and return row counts by symbol.
    """
    results = {}
    for symbol in stock_list:
        clean_symbol = symbol.upper().strip()
        results[clean_symbol] = insert_prices(clean_symbol, startdate, enddate)
    return results


# In[ ]:





# In[ ]:





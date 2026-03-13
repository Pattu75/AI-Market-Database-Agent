#!/usr/bin/env python
# coding: utf-8

# ## Query Functions
# 
# 

# In[ ]:


from db import get_connection


def get_latest_price(symbol: str) -> pd.DataFrame:
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


# In[ ]:


def get_returns(symbol: str) -> pd.DataFrame:
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


# In[ ]:


def get_price_matrix(stock_list: list[str], startdate: str, enddate: str) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    placeholders = ",".join(["%s"] * len(stock_list))
    query = f"""
        SELECT t.symbol, p.dt, p.adj_close
        FROM price_daily p
        JOIN tickers t ON p.ticker_id = t.id
        WHERE t.symbol IN ({placeholders})
          AND p.dt BETWEEN %s AND %s
        ORDER BY p.dt
    """

    params = [s.upper().strip() for s in stock_list] + [startdate, enddate]

    try:
        df = pd.read_sql(query, conn, params=params)
        if df.empty:
            return pd.DataFrame()

        return df.pivot(index="dt", columns="symbol", values="adj_close")
    finally:
        conn.close()


# In[ ]:


def get_all_tickers() -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    SELECT id, symbol, created_at
    FROM tickers
    ORDER BY symbol
    """

    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





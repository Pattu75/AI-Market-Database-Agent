#!/usr/bin/env python
# coding: utf-8

# ## Feature Engineering
# 
# **compute_features.py**

# In[ ]:


from db import get_connection


def compute_features(symbol: str) -> pd.DataFrame:
    """
    Compute core quant indicators for one ticker.
    """
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
    SELECT p.dt, p.adj_close
    FROM price_daily p
    JOIN tickers t ON t.id = p.ticker_id
    WHERE t.symbol = %s
    ORDER BY dt
    """

    try:
        df = pd.read_sql(query, conn, params=[symbol.upper().strip()])
        if df.empty:
            return df

        df["return_1d"] = df["adj_close"].pct_change(fill_method=None)
        df["ma20"] = df["adj_close"].rolling(20).mean()
        df["ma50"] = df["adj_close"].rolling(50).mean()
        df["ma200"] = df["adj_close"].rolling(200).mean()
        df["vol_20"] = df["return_1d"].rolling(20).std()
        df["cum_return"] = (1 + df["return_1d"]).cumprod() - 1

        return df.dropna()

    finally:
        conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





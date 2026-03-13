#!/usr/bin/env python
# coding: utf-8

# ## Database Connection (db.py)

# In[ ]:


# Import libraries
import yfinance as yf
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt


# In[ ]:


import mysql.connector
from mysql.connector import Error


def get_connection(
    host: str = "localhost",
    port: int = 3306,
    user: str = "sqluser",
    password: str = "Temara1975@",
    database: str = "marketdata",
):
    """
    Create and return a MySQL connection.
    Returns None if the connection fails.
    """
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        return conn
    except Error as e:
        print(f"Database connection error: {e}")
        return None


# In[ ]:





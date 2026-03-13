# AI Market Database Agent Application

# Overview

The AI Market Database Agent is an end-to-end financial data platform designed to automatically retrieve, store, analyze, and visualize market data.

It demonstrates a full data engineering and quantitative analytics pipeline, integrating market data ingestion, database storage, financial analytics, and portfolio optimization.

The system combines Python, MySQL, and Streamlit to build a scalable financial data infrastructure capable of supporting investment research and algorithmic trading strategies.

This project showcases practical skills in:
- Financial data engineering
- Quantitative finance
- Database systems
- Data analytics
- Interactive dashboards

# System Architecture

The platform follows a modular architecture:
Yahoo Finance API
        │
        ▼
Data Ingestion (Python / yfinance)
        │
        ▼
Data Processing (Pandas / NumPy)
        │
        ▼
MySQL Database Storage
        │
        ▼
Financial Analytics Engine
        │
        ▼
Portfolio Optimization
        │
        ▼
Streamlit Interactive Dashboard

# Market Data Ingestion

The system retrieves financial market data directly from Yahoo Finance using the yfinance API.

Data retrieved includes:
- Historical prices
- Trading volume
- Adjusted close prices
- Daily returns

# Database Storage

All financial data is stored in a MySQL relational database, enabling structured querying and long-term storage.

Benefits of database storage:
- persistent data storage
- structured financial datasets
- efficient SQL queries
- scalable architecture

# Quantitative Financial Analytics

The analytics engine computes key financial indicators commonly used in quantitative finance.
- Moving Averages
- MA50
- 50-day moving average used for trend detection.
- MA200
- 200-day moving average used for long-term trend analysis.
- Annualized volatility
- Sharpe Ratio

# Portfolio Optimization

The system includes a Monte Carlo simulation to identify optimal portfolio allocations.

Steps:
- Generate 10,000 of random portfolios
- Compute expected return and volatility
- Calculate Sharpe ratio
- Identify the portfolio with the maximum Sharpe ratio

Example simulation:
- 10,000 portfolios
- random weight allocations
- risk-return optimization

Outputs include:
- Efficient frontier
- Optimal weights
- Maximum Sharpe portfolio

# Interactive Dashboard

The Streamlit dashboard allows users to interactively explore financial data.

Features include:
- Market data visualization
- Portfolio performance charts
- Sharpe ratio analysis
- Efficient frontier visualization
- SQL query results

Example dashboard outputs:
- price charts
- moving average crossover signals
- risk-return scatter plots
- optimized portfolio weights

# Author

**Zakariya Boutayeb**

Fields:
- Quantitative Finance
- Data Science
- Financial Analytics

# Links
- GitHub: https://github.com/Pattu75
- Tableau: https://public.tableau.com/app/profile/zakariya.boutayeb
- 

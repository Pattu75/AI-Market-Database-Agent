# AI Market Database Agent
### Financial Data Engineering & Quantitative Analytics Platform

## Live Application

Try the deployed application:

🔗 (https://ai-market-database-agent-2muca7kdggfkcm7ehgaenx.streamlit.app/)

## Overview

The **AI Market Database Agent** is an end-to-end financial data platform designed to retrieve, store, analyze, and visualize market data.

The project demonstrates a full financial data workflow that integrates:

- **market data ingestion**
- **relational database storage**
- **quantitative analytics**
- **portfolio optimization**
- **interactive dashboarding**
- **AI-style SQL querying**

It combines **Python**, **MySQL**, and **Streamlit** to build a practical financial analytics system that can support investment research, portfolio analysis, and data-driven decision-making.

## Key Features

- Download historical market data from **Yahoo Finance**
- Store market data in a **MySQL relational database**
- Analyze returns using **Pandas** and **NumPy**
- Compare multiple assets across categories
- Build equal-weight portfolios
- Run **Monte Carlo portfolio simulations**
- Compute **Sharpe ratio** and risk-return metrics
- Explore the market database through an **AI SQL Agent**
- Demonstrate advanced SQL analytics:
  - `UNION`
  - `IN`
  - `ANY`
  - `WITH`
  - `DENSE_RANK`
  - `ROLLUP`

## Technology Stack

### Programming
- Python
- SQL

### Data & Analytics Libraries
- Pandas
- NumPy
- Matplotlib
- yfinance

### Database
- MySQL

### Dashboard & Deployment
- Streamlit
- Streamlit Community Cloud

## System Architecture

The platform follows this workflow:

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

# Portfolio Optimization

The platform performs Monte Carlo portfolio simulations to identify optimal asset allocations.

Simulation Process:

1. Generate 10,000 random portfolios
2. Assign random weight allocations
3. Compute expected portfolio return
4. Compute portfolio volatility
5. Calculate the Sharpe ratio
6. Identify the portfolio with the maximum Sharpe ratio

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

# Technology Stack

Programming:
- Python

Data Science Libraries:
- Pandas
- NumPy
- Matplotlib
- yfinance

Database:
- MySQL

Data Visualization:
- Streamlit

Financial Analytics:
- Monte Carlo Simulation
- Sharpe Ratio Optimization
- Portfolio Risk-Return Analysis
  
# Author

Zakariya Boutayeb  
Data Science & Quantitative Finance

Links:
- GitHub: https://github.com/Pattu75
- Tableau: https://public.tableau.com/app/profile/zakariya.boutayeb


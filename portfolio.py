#!/usr/bin/env python
# coding: utf-8

# ## Portfolio Analytics
# 
# **portfolio.py**

# In[ ]:


from db import get_connection
from queries import get_price_matrix


def equal_weight_portfolio(stock_list: list[str], startdate: str, enddate: str):
    """
    Build equal-weight portfolio and calculate metrics.
    """
    prices = get_price_matrix(stock_list, startdate, enddate)
    if prices.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.Series(dtype=float), np.array([]), {}

    returns = prices.pct_change(fill_method=None).dropna()
    if returns.empty:
        return prices, returns, pd.Series(dtype=float), np.array([]), {}

    n = len(stock_list)
    weights = np.array([1 / n] * n)

    portfolio_returns = returns.dot(weights)

    cumulative_return = (1 + portfolio_returns).prod() - 1
    mean_daily_return = portfolio_returns.mean()
    std_daily_return = portfolio_returns.std()
    sharpe_ratio = mean_daily_return / std_daily_return if std_daily_return != 0 else np.nan
    annualized_sharpe = sharpe_ratio * np.sqrt(252) if pd.notna(sharpe_ratio) else np.nan

    metrics = {
        "Cumulative Return": cumulative_return,
        "Mean Daily Return": mean_daily_return,
        "Standard Deviation Daily Return": std_daily_return,
        "Sharpe Ratio": sharpe_ratio,
        "Annualized Sharpe Ratio": annualized_sharpe,
    }

    return prices, returns, portfolio_returns, weights, metrics


# In[ ]:


def monte_carlo_portfolios(
    stock_list: list[str],
    startdate: str,
    enddate: str,
    num_simulations: int = 10000,
):
    """
    Generate Monte Carlo random portfolios.
    """
    prices = get_price_matrix(stock_list, startdate, enddate)
    if prices.empty:
        return pd.DataFrame(), pd.DataFrame()

    returns = prices.pct_change(fill_method=None).dropna()
    if returns.empty:
        return returns, pd.DataFrame()

    mean_returns = returns.mean()
    cov_matrix = returns.cov()

    results = []
    weights_record = []

    for _ in range(num_simulations):
        weights = np.random.random(len(stock_list))
        weights = weights / np.sum(weights)

        portfolio_return = np.sum(mean_returns * weights)
        portfolio_std = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))

        sharpe = portfolio_return / portfolio_std if portfolio_std != 0 else 0
        annualized_sharpe = sharpe * np.sqrt(252)

        results.append([portfolio_return, portfolio_std, sharpe, annualized_sharpe])
        weights_record.append(weights)

    results_df = pd.DataFrame(
        results,
        columns=["mean_return", "std_dev", "sharpe", "annualized_sharpe"],
    )

    weights_df = pd.DataFrame(weights_record, columns=stock_list)
    simulation_df = pd.concat([results_df, weights_df], axis=1)

    return returns, simulation_df


# In[ ]:


def get_optimal_portfolio(simulation_df: pd.DataFrame) -> pd.Series:
    """
    Return the portfolio with the highest annualized Sharpe ratio.
    """
    if simulation_df.empty:
        return pd.Series(dtype=float)

    optimal_idx = simulation_df["annualized_sharpe"].idxmax()
    return simulation_df.loc[optimal_idx]


# In[ ]:


def plot_portfolio_scenarios(simulation_df: pd.DataFrame, optimal_portfolio: pd.Series):
    """
    Plot all portfolio scenarios and highlight the optimal portfolio.
    """
    if simulation_df.empty or optimal_portfolio.empty:
        print("No simulation data available to plot.")
        return

    plt.figure(figsize=(10, 6))
    plt.scatter(
        simulation_df["std_dev"],
        simulation_df["mean_return"],
        c=simulation_df["annualized_sharpe"],
        cmap="viridis",
        alpha=0.5,
    )
    plt.colorbar(label="Annualized Sharpe Ratio")

    plt.scatter(
        optimal_portfolio["std_dev"],
        optimal_portfolio["mean_return"],
        marker="*",
        s=300,
    )

    plt.xlabel("Portfolio Risk (Std Dev)")
    plt.ylabel("Portfolio Mean Return")
    plt.title("Monte Carlo Portfolio Simulation")
    plt.show()


# In[ ]:


def save_optimal_portfolio(
    run_name: str,
    startdate: str,
    enddate: str,
    stock_list: list[str],
    num_simulations: int,
    optimal_portfolio: pd.Series,
) -> int | None:
    """
    Save optimal portfolio summary and weights to MySQL.
    """
    if optimal_portfolio.empty:
        return None

    conn = get_connection()
    if conn is None:
        return None

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO portfolio_runs
            (run_name, start_date, end_date, num_assets, num_simulations,
             cumulative_return, mean_daily_return, std_daily_return,
             sharpe_ratio, annualized_sharpe)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_name,
                startdate,
                enddate,
                len(stock_list),
                num_simulations,
                None,
                optimal_portfolio["mean_return"],
                optimal_portfolio["std_dev"],
                optimal_portfolio["sharpe"],
                optimal_portfolio["annualized_sharpe"],
            ),
        )

        run_id = cursor.lastrowid

        for symbol in stock_list:
            cursor.execute(
                """
                INSERT INTO portfolio_weights (run_id, symbol, weight_value)
                VALUES (%s, %s, %s)
                """,
                (run_id, symbol, float(optimal_portfolio[symbol])),
            )

        conn.commit()
        return run_id

    finally:
        cursor.close()
        conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





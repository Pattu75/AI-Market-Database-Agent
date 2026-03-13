USE marketdata;

CREATE TABLE IF NOT EXISTS tickers (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(16) NOT NULL UNIQUE,
  name VARCHAR(255),
  asset_type VARCHAR(64),      -- equity, etf, crypto, fx, etc.
  currency VARCHAR(16),
  exchange VARCHAR(32),
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_daily (
  ticker_id BIGINT NOT NULL,
  dt DATE NOT NULL,
  open DECIMAL(18,6),
  high DECIMAL(18,6),
  low  DECIMAL(18,6),
  close DECIMAL(18,6),
  adj_close DECIMAL(18,6),
  volume BIGINT,
  source VARCHAR(32) DEFAULT 'yahoo',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (ticker_id, dt),
  CONSTRAINT fk_price_ticker FOREIGN KEY (ticker_id) REFERENCES tickers(id)
);

CREATE TABLE job_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(100),
    ticker_symbol VARCHAR(16),
    rows_inserted INT,
    status VARCHAR(20),
    error_message TEXT,
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE INDEX idx_price_dt ON price_daily(dt);
CREATE INDEX idx_price_ticker ON price_daily(ticker_id);
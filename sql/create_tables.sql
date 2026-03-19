
-- create_tables.sql
-- Purpose: Define the structure of our raw
-- data table where ingested stock data lands



IF NOT EXISTS (
    SELECT * FROM sys.table_types 
    WHERE name='stock_metadata'
)
CREATE TABLE stock_metadata (
     id              INT IDENTITY(1,1) PRIMARY KEY,  -- auto incrementing unique ID
    ticker          VARCHAR(10)    NOT NULL UNIQUE,  -- e.g. AAPL, MSFT (must be unique)
    company_name    VARCHAR(100)   NOT NULL,         -- e.g. Apple Inc.
    exchange        VARCHAR(20),                     -- e.g. NASDAQ, NYSE
    currency        VARCHAR(10),                     -- e.g. USD
    country         VARCHAR(50),                     -- e.g. USA
    sector          VARCHAR(50),                     -- e.g. Technology
    industry        VARCHAR(100),                    -- e.g. Electronic Computers
    added_at        DATETIME       DEFAULT GETDATE() -- when we added this ticker
);

if not exists(
	select * from sys.tables 
	where name='raw_stocks' 
)

create table raw_stocks(
	id		int identity(1,1) primary key,
	ticker	varchar(10) not null,
	trade_date	date not null,
	open_price	decimal(10,4) not null,  --stock APIs return like 4decimal numbers
	high_price decimal(10,4) not null,
	low_price decimal(10,4) not null,
	close_price decimal(10,4) not null,
	volume	bigint not null,				--- bigint for the millions of share traded per day
	ingested_at	datetime default getdate() 
	constraint uq_ticker_date unique (ticker,trade_date) --prevent duplicate during our pipline run
	CONSTRAINT fk_raw_ticker
        FOREIGN KEY (ticker)
        REFERENCES stock_metadata(ticker)
);

IF NOT EXISTS (
    SELECT * FROM sysobjects
    WHERE name='technical_indicators' AND xtype='U'
)
CREATE TABLE technical_indicators (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,          -- stock symbol
    trade_date      DATE           NOT NULL,          -- trading day

    -- FROM ALPHA VANTAGE API
    sma_20          DECIMAL(10,4),                    -- Simple Moving Average 20 day
    ema_20          DECIMAL(10,4),                    -- Exponential Moving Average 20 day
    rsi_14          DECIMAL(10,4),                    -- Relative Strength Index 14 day

    -- CALCULATED IN DATABRICKS (Week 2)
    -- NULL for now — Databricks will fill these in
    macd            DECIMAL(10,4),                    -- MACD line
    macd_signal     DECIMAL(10,4),                    -- MACD signal line
    macd_hist       DECIMAL(10,4),                    -- MACD histogram

    -- BBANDS — confirm tomorrow if free or calculated
    bb_upper        DECIMAL(10,4),                    -- Bollinger Band upper
    bb_middle       DECIMAL(10,4),                    -- Bollinger Band middle (SMA)
    bb_lower        DECIMAL(10,4),                    -- Bollinger Band lower

    ingested_at     DATETIME       DEFAULT GETDATE(), -- when pipeline loaded this row

    -- Same pattern as raw_stocks —
    -- one row per ticker per day maximum
    CONSTRAINT uq_ind_ticker_date
        UNIQUE (ticker, trade_date),

    -- Must be a ticker we're tracking
    CONSTRAINT fk_ind_ticker
        FOREIGN KEY (ticker)
        REFERENCES stock_metadata(ticker)
);



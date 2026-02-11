-- Initial Schema for Crypto-Warehouse

-- Raw Data Layer (Staging)
DROP TABLE IF EXISTS Staging_API_Response CASCADE;
CREATE TABLE Staging_API_Response (
    ResponseID SERIAL PRIMARY KEY,
    IngestedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    RawJSON JSONB NOT NULL
);

-- Quality Assurance Layer
DROP TABLE IF EXISTS Data_Quality_Logs CASCADE;
CREATE TABLE Data_Quality_Logs (
    LogID SERIAL PRIMARY KEY,
    OccurredAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ErrorLevel VARCHAR(20) CHECK (ErrorLevel IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    Message TEXT,
    RawRecordID INT -- References Staging_API_Response if applicable
);

-- Dimension Tables
DROP TABLE IF EXISTS Dim_Currency CASCADE;
CREATE TABLE Dim_Currency (
    CurrencyID SERIAL PRIMARY KEY,
    CoinGeckoID VARCHAR(100) UNIQUE NOT NULL, -- e.g. 'bitcoin'
    Symbol VARCHAR(20),
    Name VARCHAR(100),
    MaxSupply NUMERIC,
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Dim_Date CASCADE;
CREATE TABLE Dim_Date (
    DateID INT PRIMARY KEY, -- Format YYYYMMDD
    FullDate DATE UNIQUE NOT NULL,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    DayOfWeek INT,
    DayName VARCHAR(10),
    MonthName VARCHAR(10)
);

-- Fact Table
DROP TABLE IF EXISTS Fact_Market_Metrics CASCADE;
CREATE TABLE Fact_Market_Metrics (
    FactID SERIAL PRIMARY KEY,
    CurrencyID INT REFERENCES Dim_Currency(CurrencyID),
    DateID INT REFERENCES Dim_Date(DateID),
    Timestamp TIMESTAMP NOT NULL,
    PriceUSD NUMERIC(20, 8),
    MarketCapUSD NUMERIC(20, 2),
    Volume24hUSD NUMERIC(20, 2),
    VolatilityHourly NUMERIC(10, 4), -- Calculated later
    CONSTRAINT uq_fact_entry UNIQUE (CurrencyID, Timestamp)
);

-- Indexes for performance
CREATE INDEX idx_fact_currency ON Fact_Market_Metrics(CurrencyID);
CREATE INDEX idx_fact_date ON Fact_Market_Metrics(DateID);
CREATE INDEX idx_staging_ingest ON Staging_API_Response(IngestedAt);
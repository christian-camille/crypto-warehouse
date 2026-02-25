-- Analytical Views for Crypto-Warehouse

-- Moving Averages (7-day MA for Price)
CREATE OR REPLACE VIEW vw_MovingAverages AS
WITH daily_prices AS (
    SELECT
        d.FullDate,
        c.CurrencyID,
        c.Name AS Currency,
        AVG(f.PriceUSD)::NUMERIC(20, 8) AS PriceUSD
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    JOIN Dim_Date d ON f.DateID = d.DateID
    WHERE f.PriceUSD IS NOT NULL
    GROUP BY d.FullDate, c.CurrencyID, c.Name
)
SELECT
    dp.FullDate,
    dp.Currency,
    dp.PriceUSD,
    AVG(dp.PriceUSD) OVER (
        PARTITION BY dp.CurrencyID
        ORDER BY dp.FullDate::TIMESTAMP
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
    ) AS MovingAvg7Day
FROM daily_prices dp;

-- Volatility Analysis (Hourly percentage change)
CREATE OR REPLACE VIEW vw_Volatility AS
WITH hourly_prices AS (
    SELECT
        DATE_TRUNC('hour', f.Timestamp) AS Timestamp,
        f.CurrencyID,
        c.Name AS Currency,
        AVG(f.PriceUSD)::NUMERIC(20, 8) AS PriceUSD
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    WHERE f.PriceUSD IS NOT NULL
    GROUP BY DATE_TRUNC('hour', f.Timestamp), f.CurrencyID, c.Name
)
SELECT
    current_hour.Timestamp,
    current_hour.Currency,
    current_hour.PriceUSD,
    previous_hour.PriceUSD AS PrevHourPrice,
    ROUND(
        (current_hour.PriceUSD - previous_hour.PriceUSD)
        / NULLIF(previous_hour.PriceUSD, 0) * 100,
    2) AS PctChangeHourly
FROM hourly_prices current_hour
LEFT JOIN (
    SELECT
        Timestamp,
        CurrencyID,
        PriceUSD::NUMERIC AS PriceUSD
    FROM hourly_prices
) previous_hour
    ON previous_hour.CurrencyID = current_hour.CurrencyID
    AND previous_hour.Timestamp = current_hour.Timestamp - INTERVAL '1 hour';

-- Currency Ranking (Rank by Daily Volume)
CREATE OR REPLACE VIEW vw_DailyVolumeRank AS
WITH daily_last_snapshot AS (
    SELECT DISTINCT ON (d.FullDate, f.CurrencyID)
        d.FullDate,
        f.CurrencyID,
        c.Name AS Currency,
        f.Volume24hUSD::NUMERIC AS TotalDailyVolume
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    JOIN Dim_Date d ON f.DateID = d.DateID
    WHERE f.Volume24hUSD IS NOT NULL
    ORDER BY d.FullDate, f.CurrencyID, f.Timestamp DESC
)
SELECT
    dls.FullDate,
    dls.Currency,
    dls.TotalDailyVolume,
    DENSE_RANK() OVER (
        PARTITION BY dls.FullDate
        ORDER BY dls.TotalDailyVolume DESC
    ) AS VolumeRank
FROM daily_last_snapshot dls;
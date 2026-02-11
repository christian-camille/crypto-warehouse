-- Analytical Views for Crypto-Warehouse

-- Moving Averages (7-day MA for Price)
CREATE OR REPLACE VIEW vw_MovingAverages AS
SELECT 
    d.FullDate,
    c.Name AS Currency,
    f.PriceUSD,
    AVG(f.PriceUSD) OVER (
        PARTITION BY c.CurrencyID 
        ORDER BY f.Timestamp 
        ROWS BETWEEN 168 PRECEDING AND CURRENT ROW -- Approx 7 days * 24h = 168 hours
    ) AS MovingAvg7Day
FROM Fact_Market_Metrics f
JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
JOIN Dim_Date d ON f.DateID = d.DateID;

-- Volatility Analysis (Hourly percentage change)
CREATE OR REPLACE VIEW vw_Volatility AS
SELECT 
    f.Timestamp,
    c.Name AS Currency,
    f.PriceUSD,
    LAG(f.PriceUSD, 1) OVER (PARTITION BY c.CurrencyID ORDER BY f.Timestamp) AS PrevHourPrice,
    ROUND(
        (f.PriceUSD - LAG(f.PriceUSD, 1) OVER (PARTITION BY c.CurrencyID ORDER BY f.Timestamp)) 
        / NULLIF(LAG(f.PriceUSD, 1) OVER (PARTITION BY c.CurrencyID ORDER BY f.Timestamp), 0) * 100, 
    2) AS PctChangeHourly
FROM Fact_Market_Metrics f
JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID;

-- Currency Ranking (Rank by Daily Volume)
CREATE OR REPLACE VIEW vw_DailyVolumeRank AS
SELECT 
    d.FullDate,
    c.Name AS Currency,
    SUM(f.Volume24hUSD) AS TotalDailyVolume, -- This is an approximation if using 'total_volume' snapshot, better to take max or avg
    DENSE_RANK() OVER (PARTITION BY d.FullDate ORDER BY SUM(f.Volume24hUSD) DESC) AS VolumeRank
FROM Fact_Market_Metrics f
JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
JOIN Dim_Date d ON f.DateID = d.DateID
GROUP BY d.FullDate, c.Name;

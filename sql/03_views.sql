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

-- Market Cap Trends (MoM/YoY changes + rankings)
CREATE OR REPLACE VIEW vw_MarketCapTrends AS
WITH monthly_market_cap AS (
    SELECT
        DATE_TRUNC('month', f.Timestamp)::DATE AS MonthStart,
        c.CurrencyID,
        c.Name AS Currency,
        AVG(f.MarketCapUSD) AS AvgMarketCapUSD,
        MAX(f.MarketCapUSD) AS PeakMarketCapUSD
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    WHERE f.MarketCapUSD IS NOT NULL
    GROUP BY DATE_TRUNC('month', f.Timestamp)::DATE, c.CurrencyID, c.Name
), trend_deltas AS (
    SELECT
        current_month.MonthStart,
        current_month.CurrencyID,
        current_month.Currency,
        current_month.AvgMarketCapUSD,
        current_month.PeakMarketCapUSD,
        previous_month.AvgMarketCapUSD AS PrevMonthMarketCapUSD,
        previous_year.AvgMarketCapUSD AS PrevYearMarketCapUSD
    FROM monthly_market_cap current_month
    LEFT JOIN monthly_market_cap previous_month
        ON previous_month.CurrencyID = current_month.CurrencyID
        AND previous_month.MonthStart = (current_month.MonthStart - INTERVAL '1 month')::DATE
    LEFT JOIN monthly_market_cap previous_year
        ON previous_year.CurrencyID = current_month.CurrencyID
        AND previous_year.MonthStart = (current_month.MonthStart - INTERVAL '1 year')::DATE
), trend_changes AS (
    SELECT
        MonthStart,
        CurrencyID,
        Currency,
        AvgMarketCapUSD,
        PeakMarketCapUSD,
        ((AvgMarketCapUSD - PrevMonthMarketCapUSD) / NULLIF(PrevMonthMarketCapUSD, 0)) AS MoMMarketCapChangeRatio,
        ((AvgMarketCapUSD - PrevYearMarketCapUSD) / NULLIF(PrevYearMarketCapUSD, 0)) AS YoYMarketCapChangeRatio
    FROM trend_deltas
)
SELECT
    MonthStart,
    CurrencyID,
    Currency,
    AvgMarketCapUSD,
    PeakMarketCapUSD,
    ROUND(MoMMarketCapChangeRatio * 100, 2) AS MoMMarketCapChangePct,
    ROUND(YoYMarketCapChangeRatio * 100, 2) AS YoYMarketCapChangePct,
    DENSE_RANK() OVER (PARTITION BY MonthStart ORDER BY AvgMarketCapUSD DESC) AS MarketCapRank,
    DENSE_RANK() OVER (
        PARTITION BY MonthStart
        ORDER BY MoMMarketCapChangeRatio DESC NULLS LAST
    ) AS MoMChangeRank,
    DENSE_RANK() OVER (
        PARTITION BY MonthStart
        ORDER BY YoYMarketCapChangeRatio DESC NULLS LAST
    ) AS YoYChangeRank
FROM trend_changes;

-- Correlation Matrix (Top 20 currencies by latest market cap)
CREATE OR REPLACE VIEW vw_PriceCorrelation AS
WITH latest_snapshot AS (
    SELECT MAX(Timestamp) AS LatestTimestamp
    FROM Fact_Market_Metrics
), top_20_ranked AS (
    SELECT
        f.CurrencyID,
        c.Name AS Currency,
        f.MarketCapUSD,
        ROW_NUMBER() OVER (ORDER BY f.MarketCapUSD DESC NULLS LAST) AS MarketCapRank
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    JOIN latest_snapshot ls ON f.Timestamp = ls.LatestTimestamp
    WHERE f.MarketCapUSD IS NOT NULL
), top_20 AS (
    SELECT
        CurrencyID,
        Currency,
        MarketCapUSD,
        MarketCapRank
    FROM top_20_ranked
    WHERE MarketCapRank <= 20
), hourly_prices AS (
    SELECT
        DATE_TRUNC('hour', f.Timestamp) AS Timestamp,
        f.CurrencyID,
        t.Currency,
        AVG(f.PriceUSD) AS PriceUSD
    FROM Fact_Market_Metrics f
    JOIN top_20 t ON f.CurrencyID = t.CurrencyID
    JOIN latest_snapshot ls ON TRUE
    WHERE f.PriceUSD IS NOT NULL
      AND f.Timestamp >= ls.LatestTimestamp - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('hour', f.Timestamp), f.CurrencyID, t.Currency
), hourly_returns AS (
    SELECT
        current_hour.Timestamp,
        current_hour.CurrencyID,
        current_hour.Currency,
        ((current_hour.PriceUSD - previous_hour.PriceUSD)
            / NULLIF(previous_hour.PriceUSD, 0))::DOUBLE PRECISION AS HourlyReturn
    FROM hourly_prices current_hour
    LEFT JOIN hourly_prices previous_hour
        ON previous_hour.CurrencyID = current_hour.CurrencyID
        AND previous_hour.Timestamp = current_hour.Timestamp - INTERVAL '1 hour'
), valid_returns AS (
    SELECT *
    FROM hourly_returns
    WHERE HourlyReturn IS NOT NULL
), pairwise_correlations AS (
    SELECT
        r1.CurrencyID AS CurrencyID1,
        r2.CurrencyID AS CurrencyID2,
        CORR(r1.HourlyReturn, r2.HourlyReturn) AS CorrelationValue,
        COUNT(*) AS OverlappingObservations
    FROM valid_returns r1
    JOIN valid_returns r2
        ON r1.Timestamp = r2.Timestamp
        AND r1.CurrencyID < r2.CurrencyID
    GROUP BY r1.CurrencyID, r2.CurrencyID
)
SELECT
    b.CurrencyID AS BaseCurrencyID,
    b.Currency AS BaseCurrency,
    c.CurrencyID AS ComparedCurrencyID,
    c.Currency AS ComparedCurrency,
    CASE
        WHEN b.CurrencyID = c.CurrencyID THEN 1.0
        ELSE pc.CorrelationValue
    END AS CorrelationValue,
    CASE
        WHEN b.CurrencyID = c.CurrencyID THEN NULL
        ELSE pc.OverlappingObservations
    END AS OverlappingObservations,
    b.MarketCapRank AS BaseMarketCapRank,
    c.MarketCapRank AS ComparedMarketCapRank
FROM top_20 b
CROSS JOIN top_20 c
LEFT JOIN pairwise_correlations pc
    ON pc.CurrencyID1 = LEAST(b.CurrencyID, c.CurrencyID)
    AND pc.CurrencyID2 = GREATEST(b.CurrencyID, c.CurrencyID);

-- Statistical Anomaly Detection (z-score + percentile thresholds)
CREATE OR REPLACE VIEW vw_AnomalyDetection AS
WITH hourly_metrics AS (
    SELECT
        DATE_TRUNC('hour', f.Timestamp) AS Timestamp,
        f.CurrencyID,
        c.Name AS Currency,
        AVG(f.PriceUSD)::NUMERIC(20, 8) AS PriceUSD,
        AVG(f.Volume24hUSD)::NUMERIC(20, 2) AS Volume24hUSD
    FROM Fact_Market_Metrics f
    JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
    WHERE f.PriceUSD IS NOT NULL
      AND f.Volume24hUSD IS NOT NULL
    GROUP BY DATE_TRUNC('hour', f.Timestamp), f.CurrencyID, c.Name
), ordered_metrics AS (
    SELECT
        current_hour.Timestamp,
        current_hour.CurrencyID,
        current_hour.Currency,
        current_hour.PriceUSD,
        current_hour.Volume24hUSD,
        ((current_hour.PriceUSD - previous_hour.PriceUSD)
            / NULLIF(previous_hour.PriceUSD, 0)) * 100 AS HourlyReturnPct
    FROM hourly_metrics current_hour
    LEFT JOIN hourly_metrics previous_hour
        ON previous_hour.CurrencyID = current_hour.CurrencyID
        AND previous_hour.Timestamp = current_hour.Timestamp - INTERVAL '1 hour'
), rolling_stats AS (
    SELECT
        om.*,
        AVG(PriceUSD) OVER (
            PARTITION BY CurrencyID
            ORDER BY Timestamp
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 hour' PRECEDING
        ) AS PriceMean7D,
        STDDEV_SAMP(PriceUSD) OVER (
            PARTITION BY CurrencyID
            ORDER BY Timestamp
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 hour' PRECEDING
        ) AS PriceStdDev7D,
        AVG(Volume24hUSD) OVER (
            PARTITION BY CurrencyID
            ORDER BY Timestamp
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 hour' PRECEDING
        ) AS VolumeMean7D,
        STDDEV_SAMP(Volume24hUSD) OVER (
            PARTITION BY CurrencyID
            ORDER BY Timestamp
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 hour' PRECEDING
        ) AS VolumeStdDev7D
    FROM ordered_metrics om
), percentile_thresholds AS (
    SELECT
        om.Timestamp,
        om.CurrencyID,
        pct.P99AbsReturnPct,
        pct.P99VolumeUSD
    FROM ordered_metrics om
    JOIN LATERAL (
        SELECT
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ABS(windowed.HourlyReturnPct)) AS P99AbsReturnPct,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY windowed.Volume24hUSD) AS P99VolumeUSD
        FROM ordered_metrics windowed
        WHERE windowed.CurrencyID = om.CurrencyID
          AND windowed.Timestamp BETWEEN om.Timestamp - INTERVAL '30 days' AND om.Timestamp - INTERVAL '1 hour'
          AND windowed.HourlyReturnPct IS NOT NULL
    ) pct ON TRUE
    WHERE om.HourlyReturnPct IS NOT NULL
), scored_metrics AS (
    SELECT
        rs.Timestamp,
        rs.CurrencyID,
        rs.Currency,
        rs.PriceUSD,
        rs.Volume24hUSD,
        rs.HourlyReturnPct,
        ((rs.PriceUSD - rs.PriceMean7D) / NULLIF(rs.PriceStdDev7D, 0))::NUMERIC AS PriceZScoreRaw,
        ((rs.Volume24hUSD - rs.VolumeMean7D) / NULLIF(rs.VolumeStdDev7D, 0))::NUMERIC AS VolumeZScoreRaw,
        pt.P99AbsReturnPct,
        pt.P99VolumeUSD
    FROM rolling_stats rs
    JOIN percentile_thresholds pt
        ON rs.CurrencyID = pt.CurrencyID
        AND rs.Timestamp = pt.Timestamp
    WHERE rs.HourlyReturnPct IS NOT NULL
), anomaly_flags AS (
    SELECT
        sm.Timestamp,
        sm.CurrencyID,
        sm.Currency,
        sm.PriceUSD,
        sm.Volume24hUSD,
        sm.HourlyReturnPct,
        sm.PriceZScoreRaw,
        sm.VolumeZScoreRaw,
        sm.P99AbsReturnPct,
        sm.P99VolumeUSD,
        ABS(sm.PriceZScoreRaw) AS AbsPriceZScore,
        ABS(sm.VolumeZScoreRaw) AS AbsVolumeZScore,
        ABS(sm.HourlyReturnPct) AS AbsHourlyReturnPct,
        (
            ABS(sm.PriceZScoreRaw) >= 3
            OR ABS(sm.VolumeZScoreRaw) >= 3
            OR ABS(sm.HourlyReturnPct) >= sm.P99AbsReturnPct
            OR sm.Volume24hUSD >= sm.P99VolumeUSD
        ) AS IsAnomalyRaw,
        (
            ABS(sm.PriceZScoreRaw) >= 4
            OR ABS(sm.VolumeZScoreRaw) >= 4
            OR ABS(sm.HourlyReturnPct) >= sm.P99AbsReturnPct * 1.5
        ) AS IsCriticalRaw
    FROM scored_metrics sm
)
SELECT
    af.Timestamp,
    af.CurrencyID,
    af.Currency,
    af.PriceUSD,
    af.Volume24hUSD,
    af.HourlyReturnPct,
    ROUND(af.PriceZScoreRaw, 3) AS PriceZScore,
    ROUND(af.VolumeZScoreRaw, 3) AS VolumeZScore,
    af.P99AbsReturnPct,
    af.P99VolumeUSD,
    af.IsAnomalyRaw AS IsAnomaly,
    CASE
        WHEN af.IsCriticalRaw THEN 'CRITICAL'
        WHEN af.IsAnomalyRaw THEN 'WARNING'
        ELSE 'NORMAL'
    END AS AnomalySeverity
FROM anomaly_flags af;

-- Market Health (Composite score: volatility + correlation + volume)
CREATE OR REPLACE VIEW vw_MarketHealth AS
WITH latest_snapshot AS (
    SELECT MAX(Timestamp) AS LatestTimestamp
    FROM Fact_Market_Metrics
), daily_market_cap AS (
    SELECT
        DATE_TRUNC('day', f.Timestamp)::DATE AS FullDate,
        f.CurrencyID,
        AVG(f.MarketCapUSD) AS AvgMarketCapUSD
    FROM Fact_Market_Metrics f
    JOIN latest_snapshot ls ON TRUE
    WHERE f.MarketCapUSD IS NOT NULL
      AND f.Timestamp >= ls.LatestTimestamp - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', f.Timestamp)::DATE, f.CurrencyID
), top_20_daily AS (
    SELECT
        FullDate,
        CurrencyID
    FROM (
        SELECT
            FullDate,
            CurrencyID,
            ROW_NUMBER() OVER (
                PARTITION BY FullDate
                ORDER BY AvgMarketCapUSD DESC NULLS LAST
            ) AS MarketCapRank
        FROM daily_market_cap
    ) ranked
    WHERE MarketCapRank <= 20
), hourly_metrics AS (
    SELECT
        DATE_TRUNC('hour', f.Timestamp) AS Timestamp,
        DATE_TRUNC('day', f.Timestamp)::DATE AS FullDate,
        f.CurrencyID,
        AVG(f.PriceUSD) AS PriceUSD,
        AVG(f.Volume24hUSD) AS Volume24hUSD
    FROM Fact_Market_Metrics f
    JOIN top_20_daily t
        ON t.CurrencyID = f.CurrencyID
        AND t.FullDate = DATE_TRUNC('day', f.Timestamp)::DATE
    JOIN latest_snapshot ls ON TRUE
    WHERE f.PriceUSD IS NOT NULL
      AND f.Volume24hUSD IS NOT NULL
      AND f.Timestamp >= ls.LatestTimestamp - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('hour', f.Timestamp), DATE_TRUNC('day', f.Timestamp)::DATE, f.CurrencyID
), base_returns AS (
    SELECT
        current_hour.Timestamp,
        current_hour.FullDate,
        current_hour.CurrencyID,
        current_hour.Volume24hUSD,
        ((current_hour.PriceUSD - previous_hour.PriceUSD)
            / NULLIF(previous_hour.PriceUSD, 0))::DOUBLE PRECISION AS HourlyReturn
    FROM hourly_metrics current_hour
    LEFT JOIN hourly_metrics previous_hour
        ON previous_hour.CurrencyID = current_hour.CurrencyID
        AND previous_hour.Timestamp = current_hour.Timestamp - INTERVAL '1 hour'
), daily_market_stats AS (
    SELECT
        FullDate,
        STDDEV_SAMP(HourlyReturn) AS MarketVolatility,
        AVG(ABS(HourlyReturn)) AS AvgAbsReturn,
        AVG(Volume24hUSD) AS AvgVolume24hUSD
    FROM base_returns
    WHERE HourlyReturn IS NOT NULL
    GROUP BY FullDate
), daily_pairwise_corr AS (
    SELECT
        r1.FullDate,
        r1.CurrencyID AS CurrencyID1,
        r2.CurrencyID AS CurrencyID2,
        CORR(r1.HourlyReturn, r2.HourlyReturn) AS PairCorrelation
    FROM base_returns r1
    JOIN base_returns r2
        ON r1.FullDate = r2.FullDate
        AND r1.Timestamp = r2.Timestamp
        AND r1.CurrencyID < r2.CurrencyID
    WHERE r1.HourlyReturn IS NOT NULL
      AND r2.HourlyReturn IS NOT NULL
    GROUP BY r1.FullDate, r1.CurrencyID, r2.CurrencyID
), daily_corr_stats AS (
    SELECT
        FullDate,
        AVG(PairCorrelation) AS AvgPairwiseCorrelation
    FROM daily_pairwise_corr
    GROUP BY FullDate
), health_inputs AS (
    SELECT
        dms.FullDate,
        dms.MarketVolatility,
        dms.AvgAbsReturn,
        dms.AvgVolume24hUSD,
        dcs.AvgPairwiseCorrelation
    FROM daily_market_stats dms
    LEFT JOIN daily_corr_stats dcs ON dms.FullDate = dcs.FullDate
), historical_bounds AS (
    SELECT
        FullDate,
        MarketVolatility,
        AvgAbsReturn,
        AvgVolume24hUSD,
        AvgPairwiseCorrelation,
        MIN(MarketVolatility) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MinVolatilityPrior,
        MAX(MarketVolatility) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MaxVolatilityPrior,
        MIN(AvgPairwiseCorrelation) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MinCorrelationPrior,
        MAX(AvgPairwiseCorrelation) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MaxCorrelationPrior,
        MIN(AvgVolume24hUSD) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MinVolumePrior,
        MAX(AvgVolume24hUSD) OVER (
            ORDER BY FullDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS MaxVolumePrior
    FROM health_inputs
), normalized AS (
    SELECT
        FullDate,
        MarketVolatility,
        AvgAbsReturn,
        AvgVolume24hUSD,
        AvgPairwiseCorrelation,
        COALESCE(
            ROUND(
                ((1 - ((MarketVolatility - MinVolatilityPrior)
                    / NULLIF(MaxVolatilityPrior - MinVolatilityPrior, 0))) * 100)::NUMERIC,
                2
            ),
            50
        ) AS VolatilityScore,
        COALESCE(
            ROUND(
                ((1 - ((AvgPairwiseCorrelation - MinCorrelationPrior)
                    / NULLIF(MaxCorrelationPrior - MinCorrelationPrior, 0))) * 100)::NUMERIC,
                2
            ),
            50
        ) AS CorrelationScore,
        COALESCE(
            ROUND(
                (((AvgVolume24hUSD - MinVolumePrior)
                    / NULLIF(MaxVolumePrior - MinVolumePrior, 0)) * 100)::NUMERIC,
                2
            ),
            50
        ) AS VolumeScore
    FROM historical_bounds
), scored_health AS (
    SELECT
        FullDate,
        MarketVolatility,
        AvgAbsReturn,
        AvgPairwiseCorrelation,
        AvgVolume24hUSD,
        VolatilityScore,
        CorrelationScore,
        VolumeScore,
        (VolatilityScore * 0.40) + (CorrelationScore * 0.30) + (VolumeScore * 0.30) AS RawMarketHealthScore
    FROM normalized
)
SELECT
    FullDate,
    MarketVolatility,
    AvgAbsReturn,
    AvgPairwiseCorrelation,
    AvgVolume24hUSD,
    VolatilityScore,
    CorrelationScore,
    VolumeScore,
    ROUND(RawMarketHealthScore, 2) AS MarketHealthScore,
    CASE
        WHEN RawMarketHealthScore >= 75 THEN 'ROBUST'
        WHEN RawMarketHealthScore >= 50 THEN 'STABLE'
        ELSE 'FRAGILE'
    END AS MarketHealthState
FROM scored_health
;

-- Stored Procedures for Crypto-Warehouse

-- Helper procedure to populate Dim_Date
CREATE OR REPLACE PROCEDURE sp_PopulateDateDim(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO Dim_Date (DateID, FullDate, Day, Month, Quarter, Year, DayOfWeek, DayName, MonthName)
    SELECT
        EXTRACT(YEAR FROM d) * 10000 + EXTRACT(MONTH FROM d) * 100 + EXTRACT(DAY FROM d) AS DateID,
        d AS FullDate,
        EXTRACT(DAY FROM d) AS Day,
        EXTRACT(MONTH FROM d) AS Month,
        EXTRACT(QUARTER FROM d) AS Quarter,
        EXTRACT(YEAR FROM d) AS Year,
        EXTRACT(ISODOW FROM d) AS DayOfWeek,
        TO_CHAR(d, 'Day') AS DayName,
        TO_CHAR(d, 'Month') AS MonthName
    FROM generate_series(start_date, end_date, '1 day'::interval) d
    ON CONFLICT (DateID) DO NOTHING;
END;
$$;

-- Main Procedure to parse JSON and update warehouse
CREATE OR REPLACE PROCEDURE sp_ParseRawData()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_raw_json JSONB;
    v_currency_item JSONB;
    v_currency_id INT;
    v_date_id INT;
    v_timestamp TIMESTAMP;
BEGIN
    -- Loop through unprocessed records in Staging
    -- In the future, maybe mark processed records or use a cursor. 
    -- For simplicity, process all and rely on unique constraints to avoid duplicates,
    -- or better, delete from Staging after successful processing
    
    FOR rec IN SELECT ResponseID, IngestedAt, RawJSON FROM Staging_API_Response
    LOOP
        v_raw_json := rec.RawJSON;
        v_timestamp := rec.IngestedAt;
        
        -- Calculate DateID from Timestamp
        v_date_id := EXTRACT(YEAR FROM v_timestamp) * 10000 + EXTRACT(MONTH FROM v_timestamp) * 100 + EXTRACT(DAY FROM v_timestamp);

        -- Ensure Date exists in Dim_Date
        INSERT INTO Dim_Date (DateID, FullDate, Day, Month, Quarter, Year, DayOfWeek, DayName, MonthName)
        VALUES (
            v_date_id,
            v_timestamp::DATE,
            EXTRACT(DAY FROM v_timestamp),
            EXTRACT(MONTH FROM v_timestamp),
            EXTRACT(QUARTER FROM v_timestamp),
            EXTRACT(YEAR FROM v_timestamp),
            EXTRACT(ISODOW FROM v_timestamp),
            TO_CHAR(v_timestamp, 'Day'),
            TO_CHAR(v_timestamp, 'Month')
        )
        ON CONFLICT (DateID) DO NOTHING;

        -- Iterate through each coin in the JSON array
        -- Assuming CoinGecko format: [{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", ...}, ...]
        FOR v_currency_item IN SELECT * FROM jsonb_array_elements(v_raw_json)
        LOOP
            BEGIN
                -- Upsert Dimension: Dim_Currency
                INSERT INTO Dim_Currency (CoinGeckoID, Symbol, Name, MaxSupply, LastUpdated)
                VALUES (
                    v_currency_item->>'id',
                    v_currency_item->>'symbol',
                    v_currency_item->>'name',
                    (v_currency_item->>'max_supply')::NUMERIC,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (CoinGeckoID) 
                DO UPDATE SET 
                    Symbol = EXCLUDED.Symbol,
                    Name = EXCLUDED.Name,
                    MaxSupply = EXCLUDED.MaxSupply,
                    LastUpdated = CURRENT_TIMESTAMP
                RETURNING CurrencyID INTO v_currency_id;
                
                -- If not returned (because no update needed?), fetch ID
                IF v_currency_id IS NULL THEN
                    SELECT CurrencyID INTO v_currency_id FROM Dim_Currency WHERE CoinGeckoID = v_currency_item->>'id';
                END IF;

                -- Validate Data Quality (Null Check)
                IF (v_currency_item->>'current_price') IS NULL THEN
                    INSERT INTO Data_Quality_Logs (ErrorLevel, Message, RawRecordID)
                    VALUES ('ERROR', 'Skipping currency ' || (v_currency_item->>'id') || ': Price is NULL', rec.ResponseID);
                ELSE
                    -- Insert Fact: Fact_Market_Metrics
                    INSERT INTO Fact_Market_Metrics (CurrencyID, DateID, Timestamp, PriceUSD, MarketCapUSD, Volume24hUSD)
                    VALUES (
                        v_currency_id,
                        v_date_id,
                        v_timestamp,
                        (v_currency_item->>'current_price')::NUMERIC,
                        (v_currency_item->>'market_cap')::NUMERIC,
                        (v_currency_item->>'total_volume')::NUMERIC
                    )
                    ON CONFLICT (CurrencyID, Timestamp) DO NOTHING; -- Deduplication logic
                END IF;

            EXCEPTION WHEN OTHERS THEN
                -- Log error to Data_Quality_Logs
                INSERT INTO Data_Quality_Logs (ErrorLevel, Message, RawRecordID)
                VALUES ('ERROR', 'Failed to process currency: ' || (v_currency_item->>'id') || '. Error: ' || SQLERRM, rec.ResponseID);
            END;
        END LOOP;

        -- Cleanup Staging (Optional: Move to Archive or Delete)
        DELETE FROM Staging_API_Response WHERE ResponseID = rec.ResponseID;
        
    END LOOP;
END;
$$;

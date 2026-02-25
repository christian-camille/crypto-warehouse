import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
    )


def coingecko_get(path, params, retries=4, timeout=30):
    url = f"{COINGECKO_BASE_URL}{path}"
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code == 429 and attempt < retries:
                wait_seconds = min(20, attempt * 3)
                print(f"Rate limited on {path}. Waiting {wait_seconds}s before retry...")
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt == retries:
                raise RuntimeError(f"Failed GET {url}: {exc}") from exc
            wait_seconds = min(20, attempt * 2)
            print(f"Request failed on {path} (attempt {attempt}/{retries}). Retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)


def get_top_market_coins(vs_currency, top_n):
    print(f"Fetching top {top_n} coins by market cap...")
    try:
        data = coingecko_get(
            "/coins/markets",
            {
                "vs_currency": vs_currency,
                "order": "market_cap_desc",
                "per_page": top_n,
                "page": 1,
                "sparkline": "false",
            },
        )
    except RuntimeError as exc:
        print(f"Falling back to Dim_Currency coin list due to API error: {exc}")
        return get_coins_from_db(top_n)

    coins = []
    for item in data:
        coins.append(
            {
                "id": item.get("id"),
                "symbol": item.get("symbol"),
                "name": item.get("name"),
                "max_supply": item.get("max_supply"),
            }
        )
    return [coin for coin in coins if coin.get("id")]


def get_coins_from_db(limit):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT CoinGeckoID, Symbol, Name, MaxSupply
                FROM Dim_Currency
                WHERE CoinGeckoID IS NOT NULL
                ORDER BY CurrencyID ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

        coins = [
            {
                "id": row[0],
                "symbol": row[1],
                "name": row[2],
                "max_supply": row[3],
            }
            for row in rows
        ]

        if not coins:
            raise RuntimeError("No coins available in Dim_Currency for fallback list.")
        print(f"Using {len(coins)} coins from Dim_Currency fallback list.")
        return coins
    finally:
        conn.close()


def get_market_chart_range(coin_id, vs_currency, start_dt, end_dt):
    return coingecko_get(
        f"/coins/{coin_id}/market_chart/range",
        {
            "vs_currency": vs_currency,
            "from": int(start_dt.timestamp()),
            "to": int(end_dt.timestamp()),
        },
    )


def build_timestamped_snapshots(coins, vs_currency, days_back, pause_seconds):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    by_timestamp = defaultdict(list)

    print(
        f"Building history snapshots for {len(coins)} coins "
        f"from {start_dt.isoformat()} to {end_dt.isoformat()}..."
    )

    skipped_coins = []

    for index, coin in enumerate(coins, start=1):
        coin_id = coin["id"]
        print(f"[{index}/{len(coins)}] Fetching history for {coin_id}...")

        try:
            payload = get_market_chart_range(
                coin_id=coin_id,
                vs_currency=vs_currency,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        except RuntimeError as exc:
            print(f"Skipping {coin_id} due to API error: {exc}")
            skipped_coins.append(coin_id)
            if pause_seconds > 0:
                time.sleep(pause_seconds)
            continue

        price_by_ts = {int(point[0]): point[1] for point in payload.get("prices", []) if len(point) >= 2}
        market_cap_by_ts = {int(point[0]): point[1] for point in payload.get("market_caps", []) if len(point) >= 2}
        volume_by_ts = {int(point[0]): point[1] for point in payload.get("total_volumes", []) if len(point) >= 2}

        all_timestamps = sorted(set(price_by_ts) | set(market_cap_by_ts) | set(volume_by_ts))

        for ts_ms in all_timestamps:
            snapshot = {
                "id": coin_id,
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "max_supply": coin.get("max_supply"),
                "current_price": price_by_ts.get(ts_ms),
                "market_cap": market_cap_by_ts.get(ts_ms),
                "total_volume": volume_by_ts.get(ts_ms),
            }
            by_timestamp[ts_ms].append(snapshot)

        if pause_seconds > 0:
            time.sleep(pause_seconds)

    if skipped_coins:
        print(f"Skipped {len(skipped_coins)} coins due to API errors: {', '.join(skipped_coins)}")

    return by_timestamp


def insert_snapshots_to_staging(by_timestamp):
    if not by_timestamp:
        print("No snapshots to insert.")
        return 0

    conn = get_connection()
    inserted_rows = 0
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO Staging_API_Response (IngestedAt, RawJSON)
                VALUES (%s, %s::jsonb)
            """
            for ts_ms in sorted(by_timestamp.keys()):
                ingested_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                raw_json = json.dumps(by_timestamp[ts_ms])
                cur.execute(sql, (ingested_at, raw_json))
                inserted_rows += 1

        conn.commit()
        print(f"Inserted {inserted_rows} staged historical snapshots.")
        return inserted_rows
    finally:
        conn.close()


def trigger_transformation():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            print("Triggering transformation (sp_ParseRawData)...")
            cur.execute("CALL sp_ParseRawData();")
        conn.commit()
        print("Transformation complete.")
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill historical CoinGecko data into staging and parse into warehouse fact tables."
    )
    parser.add_argument("--days", type=int, default=90, help="How many days of history to backfill (default: 90).")
    parser.add_argument(
        "--top-coins",
        type=int,
        default=20,
        help="Number of top market-cap coins to backfill (default: 20).",
    )
    parser.add_argument(
        "--vs-currency",
        default="usd",
        help="Quote currency for CoinGecko market data (default: usd).",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.2,
        help="Sleep between coin history API calls to reduce rate limit risk (default: 1.2).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.days < 1:
        raise ValueError("--days must be >= 1")
    if args.top_coins < 1:
        raise ValueError("--top-coins must be >= 1")

    coins = get_top_market_coins(args.vs_currency, args.top_coins)
    if not coins:
        raise RuntimeError("No coins returned from CoinGecko /coins/markets.")

    by_timestamp = build_timestamped_snapshots(
        coins=coins,
        vs_currency=args.vs_currency,
        days_back=args.days,
        pause_seconds=args.pause_seconds,
    )

    inserted = insert_snapshots_to_staging(by_timestamp)
    if inserted == 0:
        print("No historical snapshots inserted; skipping transformation.")
        return

    trigger_transformation()
    print(
        f"Backfill complete: {inserted} staged snapshots for "
        f"{len(coins)} coins over last {args.days} days."
    )


if __name__ == "__main__":
    main()

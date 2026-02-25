import argparse
import csv
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        cursor_factory=RealDictCursor,
    )


def fetch_rows(sql, params=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def to_json_safe(value):
    if isinstance(value, (date, datetime, timedelta)):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_rows(rows):
    return [
        {key: to_json_safe(value) for key, value in dict(row).items()}
        for row in rows
    ]


def write_csv(rows, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_rows(rows)
    if not normalized:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = list(normalized[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in normalized:
            writer.writerow(row)


def write_json(rows, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_rows(rows)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(normalized, file, indent=2)


def export_dataset(name, rows, export_dir, formats):
    generated = []
    if "csv" in formats:
        csv_path = export_dir / f"{name}.csv"
        write_csv(rows, csv_path)
        generated.append(csv_path)
    if "json" in formats:
        json_path = export_dir / f"{name}.json"
        write_json(rows, json_path)
        generated.append(json_path)
    return generated


def fetch_view_outputs(limit_per_view):
    datasets = {
        "vw_moving_averages": fetch_rows(
            """
            SELECT *
            FROM vw_MovingAverages
            ORDER BY FullDate DESC, Currency ASC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_volatility": fetch_rows(
            """
            SELECT *
            FROM vw_Volatility
            ORDER BY Timestamp DESC, Currency ASC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_daily_volume_rank": fetch_rows(
            """
            SELECT *
            FROM vw_DailyVolumeRank
            ORDER BY FullDate DESC, VolumeRank ASC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_market_cap_trends": fetch_rows(
            """
            SELECT *
            FROM vw_MarketCapTrends
            ORDER BY MonthStart DESC, MarketCapRank ASC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_price_correlation": fetch_rows(
            """
            SELECT *
            FROM vw_PriceCorrelation
            ORDER BY BaseMarketCapRank, ComparedMarketCapRank
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_anomaly_detection": fetch_rows(
            """
            SELECT *
            FROM vw_AnomalyDetection
            ORDER BY Timestamp DESC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
        "vw_market_health": fetch_rows(
            """
            SELECT *
            FROM vw_MarketHealth
            ORDER BY FullDate DESC
            LIMIT %s;
            """,
            (limit_per_view,),
        ),
    }
    return datasets


def get_top_movers(limit_each=5):
    gainers = fetch_rows(
        """
        WITH latest_month AS (
            SELECT MAX(MonthStart) AS month_start
            FROM vw_MarketCapTrends
        )
        SELECT Currency, MoMMarketCapChangePct, YoYMarketCapChangePct, MarketCapRank
        FROM vw_MarketCapTrends v
        JOIN latest_month lm ON v.MonthStart = lm.month_start
        WHERE MoMMarketCapChangePct IS NOT NULL
        ORDER BY MoMMarketCapChangePct DESC
        LIMIT %s;
        """,
        (limit_each,),
    )

    losers = fetch_rows(
        """
        WITH latest_month AS (
            SELECT MAX(MonthStart) AS month_start
            FROM vw_MarketCapTrends
        )
        SELECT Currency, MoMMarketCapChangePct, YoYMarketCapChangePct, MarketCapRank
        FROM vw_MarketCapTrends v
        JOIN latest_month lm ON v.MonthStart = lm.month_start
        WHERE MoMMarketCapChangePct IS NOT NULL
        ORDER BY MoMMarketCapChangePct ASC
        LIMIT %s;
        """,
        (limit_each,),
    )

    latest_month = fetch_rows("SELECT MAX(MonthStart) AS month_start FROM vw_MarketCapTrends;")
    return {
        "month": latest_month[0]["month_start"] if latest_month else None,
        "gainers": gainers,
        "losers": losers,
    }


def get_market_risk_summary():
    latest_health = fetch_rows(
        """
        SELECT *
        FROM vw_MarketHealth
        ORDER BY FullDate DESC
        LIMIT 1;
        """
    )

    anomalies_24h = fetch_rows(
        """
        SELECT
            COUNT(*) FILTER (WHERE IsAnomaly = TRUE) AS anomaly_count,
            COUNT(*) FILTER (WHERE AnomalySeverity = 'CRITICAL') AS critical_count,
            COUNT(*) FILTER (WHERE AnomalySeverity = 'WARNING') AS warning_count
        FROM vw_AnomalyDetection
        WHERE Timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
        """
    )

    avg_corr = fetch_rows(
        """
        SELECT
            ROUND(AVG(ABS(CorrelationValue))::NUMERIC, 4) AS avg_abs_corr,
            ROUND(AVG(OverlappingObservations)::NUMERIC, 1) AS avg_overlap_obs,
            MIN(OverlappingObservations) AS min_overlap_obs
        FROM vw_PriceCorrelation
        WHERE BaseCurrencyID <> ComparedCurrencyID
          AND BaseMarketCapRank < ComparedMarketCapRank
          AND CorrelationValue IS NOT NULL
          AND OverlappingObservations IS NOT NULL;
        """
    )

    history_window = fetch_rows(
        """
        SELECT
            COUNT(DISTINCT MonthStart) AS market_cap_months,
            COUNT(*) FILTER (WHERE MoMMarketCapChangePct IS NOT NULL) AS mom_points
        FROM vw_MarketCapTrends;
        """
    )

    health_row = latest_health[0] if latest_health else {}
    anomaly_row = anomalies_24h[0] if anomalies_24h else {}
    corr_row = avg_corr[0] if avg_corr else {}
    history_row = history_window[0] if history_window else {}

    score = float(health_row.get("markethealthscore", 0) or 0)
    state = health_row.get("markethealthstate", "UNKNOWN")
    critical_count = int(anomaly_row.get("critical_count", 0) or 0)
    warning_count = int(anomaly_row.get("warning_count", 0) or 0)
    corr_value = float(corr_row.get("avg_abs_corr", 0) or 0)
    avg_overlap_obs = float(corr_row.get("avg_overlap_obs", 0) or 0)
    min_overlap_obs = int(corr_row.get("min_overlap_obs", 0) or 0)
    market_cap_months = int(history_row.get("market_cap_months", 0) or 0)
    mom_points = int(history_row.get("mom_points", 0) or 0)

    low_corr_history = avg_overlap_obs < 24
    low_mom_history = market_cap_months < 2 or mom_points == 0
    low_history = low_corr_history or low_mom_history

    risk_level = "LOW"
    if state == "FRAGILE" or critical_count >= 5 or score < 45 or corr_value >= 0.85:
        risk_level = "HIGH"
    elif state == "STABLE" or critical_count > 0 or warning_count >= 10 or corr_value >= 0.70:
        risk_level = "MEDIUM"

    return {
        "latest_health": health_row,
        "anomaly_24h": anomaly_row,
        "avg_abs_corr": corr_row.get("avg_abs_corr"),
        "avg_overlap_obs": corr_row.get("avg_overlap_obs"),
        "min_overlap_obs": corr_row.get("min_overlap_obs"),
        "market_cap_months": market_cap_months,
        "mom_points": mom_points,
        "low_corr_history": low_corr_history,
        "low_mom_history": low_mom_history,
        "low_history": low_history,
        "risk_level": risk_level,
    }


def render_markdown_report(top_movers, risk_summary, dataset_counts, generated_at):
    latest_health = risk_summary.get("latest_health", {})
    anomaly_24h = risk_summary.get("anomaly_24h", {})

    correlation_line = (
        f"- Avg abs pairwise correlation (top-20, 90d hourly returns): "
        f"**{risk_summary.get('avg_abs_corr', 'N/A')}**"
    )
    if risk_summary.get("low_corr_history"):
        correlation_line = (
            "- Avg abs pairwise correlation (top-20, 90d hourly returns): "
            "**N/A (insufficient overlap history)**"
        )

    lines = [
        "# Crypto Market Insights Report",
        "",
        f"Generated at: {generated_at.isoformat()}",
        "",
        "## Snapshot",
        f"- Market risk level: **{risk_summary.get('risk_level', 'UNKNOWN')}**",
        f"- Latest market health state: **{latest_health.get('markethealthstate', 'N/A')}**",
        f"- Market health score: **{latest_health.get('markethealthscore', 'N/A')}**",
        correlation_line,
        (
            f"- Correlation sample size (overlapping observations per pair): "
            f"avg **{risk_summary.get('avg_overlap_obs', 'N/A')}**, "
            f"min **{risk_summary.get('min_overlap_obs', 'N/A')}**"
        ),
        (
            f"- Market-cap trend history: **{risk_summary.get('market_cap_months', 0)}** months "
            f"(**{risk_summary.get('mom_points', 0)}** rows with MoM change)"
        ),
        f"- 24h anomalies: **{anomaly_24h.get('anomaly_count', 0)}** (critical: {anomaly_24h.get('critical_count', 0)}, warning: {anomaly_24h.get('warning_count', 0)})",
        "",
    ]

    if risk_summary.get("low_history"):
        lines.append(
            "- Data sufficiency warning: limited history may overstate correlation and suppress MoM gainers/losers."
        )

    lines.append(
        "- Risk level combines health state with anomaly/correlation overrides, so it can be higher than the health state."
    )
    lines.extend(["", "## Top Gainers (MoM Market Cap)"])

    if top_movers.get("gainers"):
        for row in top_movers["gainers"]:
            lines.append(
                f"- {row.get('currency')}: {row.get('mommarketcapchangepct', 'N/A')}% MoM | "
                f"{row.get('yoymarketcapchangepct', 'N/A')}% YoY | Rank #{row.get('marketcaprank', 'N/A')}"
            )
    elif risk_summary.get("low_mom_history"):
        lines.append(
            "- Insufficient market-cap history for MoM movers (need at least 2 months and non-null MoM points)."
        )
    else:
        lines.append("- No gainers data available.")

    lines.extend(["", "## Top Losers (MoM Market Cap)"])
    if top_movers.get("losers"):
        for row in top_movers["losers"]:
            lines.append(
                f"- {row.get('currency')}: {row.get('mommarketcapchangepct', 'N/A')}% MoM | "
                f"{row.get('yoymarketcapchangepct', 'N/A')}% YoY | Rank #{row.get('marketcaprank', 'N/A')}"
            )
    elif risk_summary.get("low_mom_history"):
        lines.append(
            "- Insufficient market-cap history for MoM movers (need at least 2 months and non-null MoM points)."
        )
    else:
        lines.append("- No losers data available.")

    lines.extend(["", "## Export Coverage"])
    for dataset_name, count in dataset_counts.items():
        lines.append(f"- {dataset_name}: {count} rows exported")

    lines.append("")
    return "\n".join(lines)


def generate_reports(output_dir, formats, limit_per_view):
    generated_at = datetime.now(timezone.utc)
    report_dir = output_dir / "reports"
    export_dir = output_dir / "exports"

    datasets = fetch_view_outputs(limit_per_view=limit_per_view)
    dataset_counts = {k: len(v) for k, v in datasets.items()}

    generated_exports = []
    for dataset_name, rows in datasets.items():
        generated_exports.extend(export_dataset(dataset_name, rows, export_dir, formats))

    top_movers = get_top_movers(limit_each=5)
    risk_summary = get_market_risk_summary()
    markdown = render_markdown_report(top_movers, risk_summary, dataset_counts, generated_at)

    report_path = report_dir / f"insights_{generated_at.strftime('%Y%m%d_%H%M%S')}.md"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path.write_text(markdown, encoding="utf-8")

    return {
        "report_path": report_path,
        "export_paths": generated_exports,
        "dataset_counts": dataset_counts,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Generate crypto market insights and export warehouse view data.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where reports and exports are written.",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["csv", "json"],
        default=["csv", "json"],
        help="Export formats to generate for each dataset.",
    )
    parser.add_argument(
        "--limit-per-view",
        type=int,
        default=5000,
        help="Maximum number of rows exported per view.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    result = generate_reports(
        output_dir=output_dir,
        formats=args.formats,
        limit_per_view=args.limit_per_view,
    )

    print(f"Insights report generated: {result['report_path']}")
    for export_path in result["export_paths"]:
        print(f"Export generated: {export_path}")


if __name__ == "__main__":
    main()

const API_BASE_URL = "http://127.0.0.1:8000";

const setText = (id, value) => {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value ?? "—";
  }
};

const setPipelineStatus = (statusText, isHealthy) => {
  const node = document.getElementById("pipeline-status");
  if (!node) {
    return;
  }
  node.textContent = statusText;
  node.classList.remove("is-ok", "is-warn");
  node.classList.add(isHealthy ? "is-ok" : "is-warn");
};

const formatTrend = (current, baseline, unit = "%") => {
  if (baseline === null || baseline === undefined || baseline === 0) {
    return "—";
  }
  const delta = ((current - baseline) / Math.abs(baseline)) * 100;
  const sign = delta > 0 ? "+" : "";
  return `${sign}${delta.toFixed(1)}${unit}`;
};

const formatDiff = (current, baseline) => {
  if (baseline === null || baseline === undefined) {
    return "—";
  }
  const delta = current - baseline;
  const sign = delta > 0 ? "+" : "";
  return `${sign}${Math.round(delta)}`;
};

const formatDateTime = (isoString) => {
  if (!isoString) {
    return "—";
  }
  const value = new Date(isoString);
  if (Number.isNaN(value.getTime())) {
    return "—";
  }
  return value.toISOString().replace("T", " ").slice(0, 16);
};

const parseDateValue = (value) => {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
};

const formatMinutes = (minutes) => {
  if (minutes === null || minutes === undefined) {
    return "—";
  }
  if (minutes < 1) {
    return `${Math.round(minutes * 60)}s`;
  }
  const wholeMinutes = Math.floor(minutes);
  const seconds = Math.round((minutes - wholeMinutes) * 60);
  return `${wholeMinutes}m ${seconds}s`;
};

const formatBytes = (bytes) => {
  if (bytes === null || bytes === undefined) {
    return "—";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = Number(bytes);
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)} ${units[unitIndex]}`;
};

const sparklinePath = (series, width = 240, height = 80) => {
  if (!series || series.length === 0) {
    return "";
  }
  if (series.length === 1) {
    const value = Number(series[0]) || 0;
    const y = height - Math.min(Math.max(value, 0), height - 6);
    return `M4,${y.toFixed(2)} L236,${y.toFixed(2)}`;
  }
  const max = Math.max(...series);
  const min = Math.min(...series);
  const span = max - min || 1;
  return series
    .map((value, index) => {
      const x = (index / (series.length - 1)) * (width - 8) + 4;
      const y = height - ((value - min) / span) * (height - 12) - 6;
      return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
};

const renderSparklines = (seriesConfig) => {
  seriesConfig.forEach(([id, series]) => {
    const path = document.getElementById(id);
    if (path) {
      path.setAttribute("d", sparklinePath(series));
    }
  });
};

const loadDashboard = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/metrics/dashboard`);
    if (!response.ok) {
      throw new Error(`Request failed (${response.status})`);
    }
    const dashboard = await response.json();

    const pipeline = dashboard?.pipeline || {};
    const dataQuality = dashboard?.data_quality || {};
    const performance = dashboard?.performance || {};

    const lastRunAt = parseDateValue(pipeline.last_run_at);
    const latestFactAt = parseDateValue(performance?.data_freshness?.latest_fact_timestamp);
    const latestDataTimestamp = [lastRunAt, latestFactAt]
      .filter(Boolean)
      .sort((a, b) => b.getTime() - a.getTime())[0] || null;

    const durationSeries = pipeline.duration_trend_minutes || [];
    const pipelineIsHealthy = String(pipeline.last_run_status || "").toUpperCase() === "SUCCESS";
    const avgInterval = pipeline.avg_interval_minutes;
    const averageDuration = pipeline.avg_run_seconds
      ? Number(pipeline.avg_run_seconds) / 60
      : null;

    setPipelineStatus(pipelineIsHealthy ? "Healthy" : "Degraded", pipelineIsHealthy);
    setText("pipeline-last-run", formatDateTime(lastRunAt ? lastRunAt.toISOString() : null));
    setText("pipeline-frequency", avgInterval ? `Every ${Math.round(avgInterval)} min` : "—");
    setText("pipeline-runs", pipeline.runs_today ?? "—");
    setText("pipeline-run-status", pipeline.last_run_status || "—");
    setText("pipeline-cadence", avgInterval ? `${avgInterval} min` : "—");
    setText("pipeline-missed", pipeline.failed_runs ?? "—");
    setText("pipeline-duration", averageDuration !== null ? formatMinutes(averageDuration) : "—");
    setText(
      "pipeline-trend",
      durationSeries.length >= 2
        ? formatTrend(durationSeries[durationSeries.length - 1], durationSeries[0])
        : "—"
    );

    const completenessSeries = (dataQuality.completeness_trend || []).map(
      (item) => Number(item.completeness_pct)
    );
    const outliersSeries = (dataQuality.outliers_trend || []).map((item) => Number(item.outliers));
    const latestOutliers = outliersSeries.length ? outliersSeries[outliersSeries.length - 1] : 0;

    setText("dq-completeness", `${Number(dataQuality.completeness_pct || 0).toFixed(2)}%`);
    setText(
      "completeness-trend",
      completenessSeries.length >= 2
        ? formatTrend(completenessSeries[completenessSeries.length - 1], completenessSeries[0])
        : "—"
    );
    setText("dq-outliers", latestOutliers);
    setText(
      "outliers-trend",
      outliersSeries.length >= 2 ? formatDiff(outliersSeries[outliersSeries.length - 1], outliersSeries[0]) : "—"
    );

    const avgProcessingSeconds = Number(performance?.processing_time?.avg_processing_seconds || 0);
    const latestProcessingSeconds = Number(performance?.processing_time?.last_processing_seconds || 0);
    const responseSeries = [avgProcessingSeconds, latestProcessingSeconds].filter((value) => value > 0);
    const stagingBytes = Number(performance?.staging?.staging_bytes || 0);
    const last24Rows = Number(performance?.row_counts?.last_24h_rows || 0);
    const totalRows = Number(performance?.row_counts?.total_fact_rows || 0);
    const stagingSeries = [Math.max(totalRows - last24Rows, 0), totalRows, totalRows, totalRows];

    setText("kpi-response", avgProcessingSeconds ? `${Math.round(avgProcessingSeconds * 1000)} ms` : "—");
    setText(
      "response-trend",
      responseSeries.length >= 2 ? formatTrend(responseSeries[1], responseSeries[0]) : "—"
    );
    setText("kpi-staging", formatBytes(stagingBytes));
    setText(
      "staging-trend",
      totalRows > 0 ? `${((last24Rows / totalRows) * 100).toFixed(1)}% 24h` : "—"
    );

    setText(
      "last-refresh",
      latestDataTimestamp ? formatDateTime(latestDataTimestamp.toISOString()) : "—"
    );

    renderSparklines([
      ["pipeline-spark", durationSeries],
      ["completeness-spark", completenessSeries],
      ["outliers-spark", outliersSeries],
      ["response-spark", responseSeries],
      ["staging-spark", stagingSeries]
    ]);
  } catch (error) {
    setPipelineStatus("Unavailable", false);
    setText("last-refresh", `Error: ${error.message}`);
  }
};

loadDashboard();
setInterval(loadDashboard, 60_000);

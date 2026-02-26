import numpy as np
import pandas as pd

def generate_raw_metrics(start_date="2025-11-01", days=180, seed=42, spike_days=(95, 140)):
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start_date, periods=days, freq="D")

    # Trend + weekly seasonality + monthly wave
    trend = np.linspace(0, 40, days)
    weekday = dates.weekday
    weekly_multiplier = np.where(np.isin(weekday, [4, 5]), 1.20, 1.00)
    monthly_wave = 1.0 + 0.06 * np.sin(np.linspace(0, 6 * np.pi, days))

    orders = (180 + trend) * weekly_multiplier * monthly_wave + rng.normal(0, 15, size=days)
    orders = np.clip(orders, 10, None)

    aov = 55 + 0.02 * trend + 2.5 * np.sin(np.linspace(0, 4 * np.pi, days)) + rng.normal(0, 3.5, size=days)
    aov = np.clip(aov, 5, None)

    revenue = orders * aov + rng.normal(0, 250, size=days)
    revenue = np.clip(revenue, 0, None)

    df = pd.DataFrame({
        "date": dates.astype(str),
        "orders": orders.round(2),
        "avg_order_value": aov.round(2),
        "revenue": revenue.round(2),
    })

    # Inject anomalies (spikes)
    for d in spike_days:
        if 0 <= d < len(df):
            df.loc[d, "orders"] = float(df.loc[d, "orders"]) * 2.2
            df.loc[d, "avg_order_value"] = float(df.loc[d, "avg_order_value"]) * 1.35
            df.loc[d, "revenue"] = float(df.loc[d, "orders"]) * float(df.loc[d, "avg_order_value"])

    # Inject data quality issues ON PURPOSE (raw data)
    miss_idx = rng.choice(days, size=max(1, days // 50), replace=False)  # ~2%
    df.loc[miss_idx, "avg_order_value"] = np.nan

    bad_idx = rng.choice(days, size=max(1, days // 90), replace=False)
    df.loc[bad_idx, "orders"] = -abs(df.loc[bad_idx, "orders"].astype(float))

    dup_rows = df.sample(n=2, random_state=seed)
    df_dirty = pd.concat([df, dup_rows], ignore_index=True)

    return df_dirty
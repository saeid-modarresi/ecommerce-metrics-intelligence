import math
import numpy as np
import pandas as pd
from fastapi import FastAPI
from raw_data_generator import generate_raw_metrics

app = FastAPI(title="eCommerce Metrics Intelligence - Raw API")


def sanitize_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


def sanitize_dataframe(df: pd.DataFrame):
    records = []
    for _, row in df.iterrows():
        clean_row = {k: sanitize_value(v) for k, v in row.items()}
        records.append(clean_row)
    return records


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/metrics")
def metrics(days: int = 180, seed: int = 42):
    df = generate_raw_metrics(days=days, seed=seed)

    return {
        "source": {
            "platform": "synthetic",
            "store_id": "demo-store",
            "environment": "local-dev"
        },
        "schema": ["date", "orders", "avg_order_value", "revenue"],
        "rows": int(len(df)),
        "data": sanitize_dataframe(df)
    }
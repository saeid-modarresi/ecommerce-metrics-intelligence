import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional

def emit_emf(namespace: str, dimensions: Dict[str, str], metrics: Dict[str, float]) -> None:
    """Emit CloudWatch metrics via Embedded Metric Format (EMF) in logs."""
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    emf = {
        "_aws": {
            "Timestamp": ts,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [list(dimensions.keys())] if dimensions else [[]],
                    "Metrics": [{"Name": k, "Unit": "Count"} for k in metrics.keys()],
                }
            ],
        },
        **(dimensions or {}),
        **metrics,
    }
    print(json.dumps(emf))


def default_namespace() -> str:
    return os.getenv("EMI_METRICS_NAMESPACE", "EMI/Normalize")

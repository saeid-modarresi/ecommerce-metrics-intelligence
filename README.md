# eCommerce Metrics Intelligence (EMI)
eCommerce Metrics Intelligence (EMI) is a serverless AWS pipeline that simulates real-world eCommerce data ingestion and performs automated anomaly detection on revenue and performance metrics.

The project uses synthetic API-generated commerce data to design, test, and validate a production-style data pipeline, from ingestion and normalization to statistical anomaly detection and alerting.

## What problem does EMI solve?
In eCommerce platforms, revenue and performance metrics can sometimes exhibit unexpected spikes, drops, or irregular patterns.

These anomalies may indicate:
- Tracking issues
- Pricing errors
- Fraud or abnormal activity
- Campaign misconfiguration
- Data pipeline corruption
- Sudden market shifts

However, reacting to every fluctuation is not practical.
EMI provides a structured way to:
- Continuously ingest commerce data
- Normalize and clean raw inputs from different platforms
- Apply statistical anomaly detection (rolling Mahalanobis distance)
- Generate alerts only when behavior significantly deviates from expected patterns
- Monitor the entire pipeline with cloud-native observability

The goal is to detect meaningful anomalies while reducing noise and false positives.

## Architecture (Flow)
EMI is an event-driven, serverless pipeline on AWS:

Hosted Raw JSON (API/File) → <br>
Lambda Ingest →  <br>
S3 (raw/) →  <br>
Lambda Normalize →  <br>
S3 (normalized/) →  <br>
Lambda Detect →  <br>
S3 (processed/) → <br>
SNS Alerts + CloudWatch Metrics

## Components
1- Raw Source (Hosted JSON/API): Synthetic eCommerce metrics exposed via URL.

2- Ingest Lambda: Fetches raw JSON from the source URL and writes it into S3 raw/.

3- S3 (raw/): Durable storage for replay/debugging and pipeline decoupling.

4- Normalize Lambda: Cleans and maps raw data into a canonical schema, then writes to normalized/.

5- Detect Lambda (Mahalanobis): Runs rolling Mahalanobis distance anomaly detection and produces processed/ output.

6- SNS + CloudWatch: Sends anomaly alerts and publishes metrics like AnomalyCount for dashboards/alarms.


## Detection Method
<strong>Mahalanobis Distance</strong>

EMI currently uses rolling Mahalanobis distance for multivariate anomaly detection.
Mahalanobis distance measures how far a data point deviates from the expected distribution while accounting for the correlation between features.

Unlike simple univariate methods (e.g., Z-score), Mahalanobis considers multiple metrics simultaneously, such as:
- Orders
- Average Order Value
- Revenue

This makes it more suitable for eCommerce monitoring, where anomalies often appear as unusual combinations of metrics rather than isolated spikes in a single value.

Advantages:

- Lightweight and computationally efficient
- No complex training phase required
- Works well for structured tabular data
- Easy to interpret and debug
- Suitable for serverless execution

## Future Upgrade: Isolation Forest

While Mahalanobis works well for normally distributed and linearly correlated data, it assumes a Gaussian distribution and may struggle with complex, non-linear patterns.

For more advanced anomaly detection, EMI is designed to support Isolation Forest, a machine learning–based algorithm that:

- Detects anomalies without distribution assumptions
- Handles non-linear relationships between features
- Works better with high-dimensional data
- Is more robust to irregular and complex behavioral shifts

Isolation Forest can improve detection performance in real-world eCommerce environments where behavior patterns are not strictly Gaussian.
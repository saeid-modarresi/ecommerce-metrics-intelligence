# EMI Data Contract (Raw Metrics API)

## Purpose
This contract defines the **raw input format** for EMI.  
Raw data can be incomplete or messy (nulls, duplicates, string numbers), but it must be **valid JSON**.

---

## Endpoint
`GET /metrics`

---

## Top-level schema
```json
{
  "source": {
    "platform": "synthetic | shopify | woocommerce | custom",
    "store_id": "string",
    "timezone": "IANA timezone string", //America/St_Johns, America/Toronto, Europe/Berlin, Asia/Tehran, UTC
    "currency": "optional string"
  },
  "schema": ["date", "orders", "avg_order_value", "revenue"],
  "rows": 180,
  "data": []
}
```

## Record schema (data)
Each record represents one day.
```json
{
  "date": "YYYY-MM-DD",
  "orders": 210,
  "avg_order_value": 58.2,
  "revenue": 12222.0,

  "dimensions": {
    "channel": "optional string",
    "country": "optional string",
    "currency": "optional string"
  },

  "meta": {
    "raw_fields": "optional object for source-specific fields"
  }
}
```

## Required fields
- date
- orders
- avg_order_value
- revenue

## Allowed raw issues (expected)
- null values
- numeric values as strings (e.g., "210", "12,222.10")
- duplicates by date
- negative or impossible values (to be corrected in AWS normalization)

## Normalized output target (produced in AWS)
After normalization/cleaning, EMI produces this strict format:
```json
{
  "date": "YYYY-MM-DD",
  "orders": 210,
  "avg_order_value": 58.2,
  "revenue": 12222.0
}
```

## Notes
Raw API must always return JSON-compliant output (no NaN/Infinity).
Normalization & cleaning happen in AWS.
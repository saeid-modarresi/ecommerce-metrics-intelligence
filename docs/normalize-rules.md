# EMI Normalize Rules
The Normalize Lambda is responsible for transforming raw, messy metrics data into a strict, clean, analytics-ready schema defined in the EMI Data Contract.

Raw data may contain inconsistencies, null values, string numbers, duplicates, and invalid records.
Normalization ensures deterministic, predictable output.

## 1. Input Expectations
The raw object must:
- Be valid JSON
- Follow the EMI Raw Data Contract
- Contain a top-level data array
- Include at minimum the required fields:
  - date
  - orders
  - avg_order_value
  - revenue

If the structure is invalid, the object is rejected.

## 2. Output Schema (Strict)
Each normalized record contains exactly:
```
{
  "date": "YYYY-MM-DD",
  "orders": 210,
  "avg_order_value": 58.2,
  "revenue": 12222.0
}
```
No additional fields are allowed in normalized output.

## 3. Field-Level Normalization Rules
### 3.1 Date
- Must follow YYYY-MM-DD
- Invalid or missing dates → record discarded
- Date format is not auto-corrected

### 3.2 Numeric Coercion
The following fields must be numeric:
- orders → integer
- avg_order_value → float
- revenue → float
- If values are strings (e.g. "210", "12,222.50"):
- Commas are removed
- Strings are trimmed
- Safely converted to numeric types
- If conversion fails → value defaults to 0

### 3.3 Null and Missing Values
- Missing numeric values → 0
- Explicit null → 0

### 3.4 Negative Values
Negative values are considered invalid and are clamped to 0.

Example:
- orders = -5 → 0
- revenue = -1200 → 0

### 4. Duplicate Handling
Duplicate records are identified by identical date values.

Resolution strategy:
If multiple records exist for the same date:
- The record with the highest revenue is kept.
- All other duplicates are discarded.
This ensures deterministic and consistent normalization behavior.

### 5. Deterministic Output
Normalization must be:

- Idempotent
- Deterministic
- Stateless

Given the same input object, the output must always be identical.

### 6. Failure Strategy
If the raw object:
- Is not valid JSON
- Does not contain data
- Has zero valid normalized rows

The Normalize Lambda:
- Logs the error
- Does not write output to normalized/

### 7. Output Format
Normalized output is written as:
- NDJSON (newline-delimited JSON)
- One normalized record per line

Output path pattern:
```
normalized/YYYY/MM/DD/<original_filename>.ndjson
```
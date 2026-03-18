# Glassdoor Culture Dashboard — API Documentation

**Base URL (production):** `https://glassdoor-culture-replit-4e40ef269b11.herokuapp.com`

**Authentication:** None required. All endpoints are publicly accessible.

**Format:** All responses are JSON. Successful responses include `"success": true`; errors include `"success": false` and an `"error"` string.

---

## Table of Contents

1. [Company Lookup by ISIN](#1-company-lookup-by-isin)
2. [Company Search](#2-company-search)
3. [Culture Profile by Name](#3-culture-profile-by-name)
4. [Companies List](#4-companies-list)
5. [Sectors & GICS Hierarchy](#5-sectors--gics-hierarchy)
6. [Industry Average Culture Scores](#6-industry-average-culture-scores)
7. [Culture Benchmarking](#7-culture-benchmarking)
8. [Culture Comparison (multi-company)](#8-culture-comparison-multi-company)
9. [Culture Trends](#9-culture-trends)
10. [Performance Correlation](#10-performance-correlation)
11. [Company Performance Rankings](#11-company-performance-rankings)
12. [Dataset Statistics](#12-dataset-statistics)
13. [Data Exports (CSV)](#13-data-exports-csv)
14. [Data Models Reference](#14-data-models-reference)
15. [Code Examples](#15-code-examples)

---

## 1. Company Lookup by ISIN

The primary endpoint for programmatic access. Returns Glassdoor ratings and culture scores for a company identified by its ISIN.

**`GET /api/company/isin/{isin}`**

### Path Parameters

| Parameter | Description |
|-----------|-------------|
| `isin` | 12-character ISIN (e.g. `US46625H1005`). Case-insensitive. |

### Query Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `include_ratings` | `true` | Include Glassdoor category ratings |
| `include_culture` | `true` | Include Hofstede and MIT Big 9 culture scores |

### Example Request

```
GET /api/company/isin/US46625H1005
```

### Example Response

```json
{
  "success": true,
  "isin": "US46625H1005",
  "issuer_name": "JPMORGAN CHASE & CO.",
  "glassdoor_name": "JPMorgan Chase",
  "ticker": "JPM",
  "gics": {
    "sector": "Financials",
    "industry": "Banks",
    "sub_industry": "Diversified Banks"
  },
  "extraction_status": {
    "status": "completed",
    "match_confidence": "high",
    "reviews_extracted": 16284
  },
  "data_availability": "full",
  "glassdoor_ratings": {
    "review_count": 16284,
    "overall": 3.92,
    "work_life_balance": 3.71,
    "career_opportunities": 3.85,
    "culture_and_values": 3.76,
    "compensation_and_benefits": 3.95,
    "senior_management": 3.48,
    "diversity_and_inclusion": 4.12,
    "rating_period": {
      "earliest": "2008-05-14T00:00:00",
      "latest": "2024-11-30T00:00:00"
    }
  },
  "culture_scores": {
    "hofstede": {
      "power_distance": {
        "value": 0.21,
        "confidence": 94,
        "confidence_level": "High"
      },
      "individualism": { "value": 0.35, "confidence": 94, "confidence_level": "High" },
      "uncertainty_avoidance": { "value": -0.08, "confidence": 91, "confidence_level": "High" },
      "long_term_orientation": { "value": 0.18, "confidence": 89, "confidence_level": "High" },
      "indulgence": { "value": 0.09, "confidence": 86, "confidence_level": "High" },
      "masculinity": { "value": 0.27, "confidence": 92, "confidence_level": "High" }
    },
    "mit_big_9": {
      "collaboration": { "value": 6.8, "raw_value": 0.042, "confidence": 94, "confidence_level": "High" },
      "customer_orientation": { "value": 7.1, "raw_value": 0.031, "confidence": 91, "confidence_level": "High" },
      "diversity": { "value": 8.2, "raw_value": 0.058, "confidence": 94, "confidence_level": "High" },
      "execution": { "value": 5.4, "raw_value": 0.021, "confidence": 89, "confidence_level": "High" },
      "innovation": { "value": 5.1, "raw_value": 0.018, "confidence": 87, "confidence_level": "High" },
      "integrity": { "value": 6.3, "raw_value": 0.038, "confidence": 93, "confidence_level": "High" },
      "performance": { "value": 6.9, "raw_value": 0.044, "confidence": 91, "confidence_level": "High" },
      "respect": { "value": 7.4, "raw_value": 0.052, "confidence": 90, "confidence_level": "High" },
      "agility": { "value": 4.8, "raw_value": 0.015, "confidence": 85, "confidence_level": "High" }
    }
  }
}
```

### `data_availability` Values

| Value | Meaning |
|-------|---------|
| `full` | Both Glassdoor ratings and culture scores are available |
| `ratings_only` | Glassdoor ratings present but not enough reviews for culture scoring |
| `culture_only` | Culture scores available but no category ratings |
| `matched_no_reviews` | Company was matched on Glassdoor but no reviews extracted yet |
| `not_matched` | Company is in the index but hasn't been matched to a Glassdoor profile yet |

### Error Responses

```json
{ "success": false, "error": "ISIN GB0031348658 not found. Only MSCI-listed companies (2,442) are indexed.", "isin": "GB0031348658" }
```
HTTP 404 — ISIN not in the index.

---

## 2. Company Search

Search for companies by name, ticker, or ISIN. Useful for discovering the correct ISIN before calling endpoint 1.

**`GET /api/company/search?q={query}`**

### Query Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `q` | *(required)* | Search term — matched against company name, ticker, ISIN, and Glassdoor name. Min 2 characters. |
| `limit` | `20` | Max results (capped at 100) |

### Example Request

```
GET /api/company/search?q=HSBC&limit=5
```

### Example Response

```json
{
  "success": true,
  "query": "HSBC",
  "count": 2,
  "results": [
    {
      "isin": "GB0005405286",
      "issuer_name": "HSBC HOLDINGS PLC",
      "ticker": "HSBA",
      "glassdoor_name": "HSBC",
      "gics_sector": "Financials",
      "gics_industry": "Banks",
      "status": "completed",
      "reviews_extracted": 32411
    },
    {
      "isin": "HK0000004158",
      "issuer_name": "HSBC BANK (CHINA) CO., LTD",
      "ticker": "3988",
      "glassdoor_name": null,
      "gics_sector": "Financials",
      "gics_industry": "Banks",
      "status": "no_match",
      "reviews_extracted": 0
    }
  ]
}
```

---

## 3. Culture Profile by Name

Get culture scores for a company using the Glassdoor display name (as returned by the search or ISIN endpoint).

**`GET /api/culture-profile/{company_name}`**

### Path Parameters

| Parameter | Description |
|-----------|-------------|
| `company_name` | Glassdoor company name, URL-encoded (e.g. `JPMorgan%20Chase`) |

### Example Request

```
GET /api/culture-profile/HSBC
```

### Response

Same culture scores structure as the `culture_scores` block in endpoint 1, plus `metadata`:

```json
{
  "success": true,
  "company_name": "HSBC",
  "hofstede": { ... },
  "mit": { ... },
  "metadata": {
    "review_count": 32411,
    "overall_rating": 3.81,
    "overall_confidence": 100.0,
    "overall_confidence_level": "High",
    "analysis_date": "2025-01-15T10:23:11"
  }
}
```

---

## 4. Companies List

List all companies that have reviews in the database, with optional GICS filtering.

**`GET /api/companies-list`**

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `sector` | Filter by GICS sector name (e.g. `Financials`) |
| `gics_level` | `sector`, `industry`, or `sub_industry` |
| `gics_value` | Value matching the chosen level (e.g. `Banks` for `gics_level=industry`) |

### Example

```
GET /api/companies-list?gics_level=industry&gics_value=Banks
```

### Response

```json
{
  "companies": ["HSBC", "Barclays", "JPMorgan Chase", "Deutsche Bank", ...],
  "count": 47
}
```

---

## 5. Sectors & GICS Hierarchy

### List Sectors

**`GET /api/sectors`**

Returns the 11 GICS sectors plus "Asset Management".

```json
{ "sectors": ["Financials", "Information Technology", "Health Care", "Utilities", ...] }
```

### Full GICS Hierarchy

**`GET /api/gics-hierarchy`**

Returns the full three-level hierarchy for companies that have reviews.

```json
{
  "hierarchy": {
    "Financials": {
      "Banks": {
        "sub_industries": ["Diversified Banks", "Regional Banks"],
        "company_count": 47
      },
      "Insurance": { ... }
    }
  }
}
```

---

## 6. Industry Average Culture Scores

Get the average culture scores across all companies in a sector or industry, for benchmarking.

**`GET /api/industry-average`**

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `sector` | GICS sector name |
| `gics_level` | `sector`, `industry`, or `sub_industry` |
| `gics_value` | Value at the chosen level |

### Example

```
GET /api/industry-average?gics_level=industry&gics_value=Banks
```

### Response

```json
{
  "success": true,
  "sector": "Banks",
  "company_count": 47,
  "hofstede": {
    "power_distance": { "value": 0.18, "std_dev": 0.09 },
    "individualism":  { "value": 0.28, "std_dev": 0.12 },
    ...
  },
  "mit_big_9": {
    "collaboration": { "value": 6.2, "std_dev": 1.1 },
    ...
  }
}
```

---

## 7. Culture Benchmarking

Compare a single company's culture scores against the sector or industry average.

**`GET /api/culture-benchmarking/{company_name}`**

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `sector` | GICS sector to benchmark against |
| `gics_level` | `sector`, `industry`, or `sub_industry` |
| `gics_value` | Value at the chosen level |

### Example

```
GET /api/culture-benchmarking/HSBC?gics_level=industry&gics_value=Banks
```

### Response

```json
{
  "success": true,
  "company_name": "HSBC",
  "benchmark_group": "Banks",
  "company_count_in_group": 47,
  "hofstede": {
    "power_distance": {
      "company_value": 0.21,
      "sector_average": 0.18,
      "percentile": 62,
      "z_score": 0.33
    }
  },
  "mit_big_9": { ... }
}
```

---

## 8. Culture Comparison (multi-company)

Compare culture scores across a list of named companies side by side.

**`POST /api/culture-comparison`**

### Request Body

```json
{
  "companies": ["HSBC", "Barclays", "JPMorgan Chase"],
  "sector": "Financials"
}
```

### Response

```json
{
  "success": true,
  "companies": {
    "HSBC": { "hofstede": { ... }, "mit_big_9": { ... }, "review_count": 32411 },
    "Barclays": { ... },
    "JPMorgan Chase": { ... }
  }
}
```

---

## 9. Culture Trends

### Quarterly Score Trend for a Company

**`GET /api/culture-trends/{company_name}`**

Returns how culture scores have changed per quarter over time.

### Score Trend (Alternative)

**`GET /api/company-culture-score-trend/{company_name}`**

Returns culture scores aggregated by year/quarter with review counts per period.

### Rating Trend

**`GET /api/company-culture-trend/{company_name}`**

Returns average Glassdoor star ratings per quarter.

---

## 10. Performance Correlation

Pearson correlations between the 15 culture dimensions and financial performance metrics.

**`GET /api/performance-correlation`**

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `sector` | Filter to a specific GICS sector |
| `gics_level` | `sector`, `industry`, or `sub_industry` |
| `gics_value` | Value at the chosen level |

### Response

```json
{
  "success": true,
  "correlations": {
    "collaboration": {
      "roe_5y_avg":        { "correlation": 0.23, "p_value": 0.03, "sample_size": 44, "significant": true },
      "op_margin_5y_avg":  { "correlation": 0.18, "p_value": 0.11, "sample_size": 44, "significant": false },
      "tsr_cagr_5y":       { "correlation": 0.31, "p_value": 0.02, "sample_size": 41, "significant": true },
      "composite_score":   { "correlation": 0.21, "p_value": 0.04, "sample_size": 44, "significant": true }
    },
    "power_distance": { ... }
  }
}
```

### Financial Metrics Explained

| Field | Description |
|-------|-------------|
| `roe_5y_avg` | 5-year average Return on Equity |
| `op_margin_5y_avg` | 5-year average Operating Margin |
| `tsr_cagr_5y` | 5-year Total Shareholder Return (CAGR) |
| `aum_cagr_5y` | 5-year AUM growth CAGR (Asset Management only) |
| `composite_score` | Normalised composite of all available metrics |

---

## 11. Company Performance Rankings

**`GET /api/performance-rankings`**

### Query Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sector` | *(all)* | GICS sector |
| `metric` | `composite_score` | One of `roe_5y_avg`, `op_margin_5y_avg`, `tsr_cagr_5y`, `composite_score` |
| `limit` | `20` | Number of companies to return |

### Response

```json
{
  "success": true,
  "metric": "composite_score",
  "rankings": [
    { "rank": 1, "company_name": "HSBC", "sector": "Financials", "composite_score": 0.82, "roe_5y_avg": 0.094 },
    { "rank": 2, "company_name": "JPMorgan Chase", ... }
  ]
}
```

---

## 12. Dataset Statistics

**`GET /api/stats`**

Returns high-level database statistics.

```json
{
  "total_reviews": 3144810,
  "total_companies": 1760,
  "sectors": 12,
  "latest_review": "2024-11-30",
  "coverage": {
    "with_culture_scores": 1726,
    "with_performance_data": 44
  }
}
```

---

## 13. Data Exports (CSV)

These endpoints return CSV files for offline analysis.

| Endpoint | Description |
|----------|-------------|
| `GET /api/export/company-reviews/{company_name}` | All reviews for one company |
| `GET /api/export/all-reviews` | Full review dataset (large — use with caution) |
| `GET /api/export/extraction-summary` | Extraction status for all 2,442 companies |
| `GET /api/export/companies` | All companies with their culture scores |

**Example:**
```
GET /api/export/company-reviews/HSBC
```
Returns a CSV with columns: `review_id`, `review_date`, `job_title`, `employment_status`, `rating`, `work_life_balance_rating`, `career_opportunities_rating`, `culture_and_values_rating`, `compensation_and_benefits_rating`, `senior_management_rating`, `diversity_and_inclusion_rating`, `summary`, `pros`, `cons`.

---

## 14. Data Models Reference

### Glassdoor Ratings

All ratings are on a **1–5 scale** (1 = worst, 5 = best). Values are averages across all extracted reviews.

| Field | Description |
|-------|-------------|
| `overall` | Overall company rating |
| `work_life_balance` | Work-life balance |
| `career_opportunities` | Career opportunities and growth |
| `culture_and_values` | Culture and values |
| `compensation_and_benefits` | Compensation and benefits |
| `senior_management` | Senior management quality |
| `diversity_and_inclusion` | Diversity and inclusion |

### Hofstede Dimensions

Six bipolar dimensions scored on a **−1 to +1 scale**. Positive values indicate the first-named pole; negative values indicate the opposite pole.

| Dimension | Positive pole (→ +1) | Negative pole (→ −1) |
|-----------|---------------------|---------------------|
| `power_distance` | Hierarchical, top-down | Egalitarian, flat |
| `individualism` | Individual focus | Collective / team focus |
| `uncertainty_avoidance` | Rule-driven, risk-averse | Flexible, risk-tolerant |
| `long_term_orientation` | Long-term planning | Short-term / quarterly thinking |
| `indulgence` | Employee wellbeing valued | Restrained, work-hard culture |
| `masculinity` | Competitive, performance-driven | Caring, consensus-seeking |

### MIT Big 9 Dimensions

Nine unipolar dimensions scored on a **0–10 scale** (higher = more of the attribute). Scores are rescaled relative to the highest-scoring company in the dataset.

| Dimension | Description |
|-----------|-------------|
| `collaboration` | Teamwork and cross-functional cooperation |
| `customer_orientation` | Customer focus and service mentality |
| `diversity` | Diversity, equity and inclusion emphasis |
| `execution` | Operational efficiency and getting things done |
| `innovation` | Creativity, experimentation, new ideas |
| `integrity` | Ethics, transparency, honesty |
| `performance` | Results orientation and accountability |
| `respect` | Respect for people at all levels |
| `agility` | Adaptability and speed of change |

### Confidence Scores

| Level | Reviews | Confidence % |
|-------|---------|--------------|
| `High` | ≥ 50 | 100% |
| `Medium` | 20–49 | 40–99% |
| `Low` | < 20 | < 40% |

---

## 15. Code Examples

### Python

```python
import requests

BASE_URL = "https://glassdoor-culture-replit-4e40ef269b11.herokuapp.com"

# Look up a company by ISIN
isin = "GB0005405286"  # HSBC Holdings
response = requests.get(f"{BASE_URL}/api/company/isin/{isin}")
data = response.json()

if data["success"]:
    ratings = data["glassdoor_ratings"]
    culture = data["culture_scores"]
    
    print(f"Company: {data['glassdoor_name']}")
    print(f"Overall rating: {ratings['overall']}/5")
    print(f"Work-life balance: {ratings['work_life_balance']}/5")
    print(f"Reviews: {ratings['review_count']}")
    
    hof = culture["hofstede"]
    print(f"Power distance: {hof['power_distance']['value']}")
    print(f"Individualism: {hof['individualism']['value']}")
    
    mit = culture["mit_big_9"]
    print(f"Innovation score: {mit['innovation']['value']}/10")


# Search for a company by name
results = requests.get(f"{BASE_URL}/api/company/search?q=Goldman+Sachs").json()
for r in results["results"]:
    print(r["isin"], r["issuer_name"], r["status"])


# Get all companies in the Banks industry with culture scores
companies = requests.get(
    f"{BASE_URL}/api/companies-list",
    params={"gics_level": "industry", "gics_value": "Banks"}
).json()["companies"]

profiles = []
for company in companies[:10]:
    p = requests.get(f"{BASE_URL}/api/culture-profile/{company}").json()
    if p["success"]:
        profiles.append({
            "company": company,
            "individualism": p["hofstede"]["individualism"]["value"],
            "innovation": p["mit"]["innovation"]["value"],
            "overall_rating": p["metadata"]["overall_rating"]
        })

import pandas as pd
df = pd.DataFrame(profiles)
print(df.sort_values("innovation", ascending=False))
```

### JavaScript / Node.js

```javascript
const BASE_URL = "https://glassdoor-culture-replit-4e40ef269b11.herokuapp.com";

// Look up by ISIN
async function getCompanyByISIN(isin) {
  const res = await fetch(`${BASE_URL}/api/company/isin/${isin}`);
  const data = await res.json();
  
  if (!data.success) {
    throw new Error(data.error);
  }
  
  return {
    name: data.glassdoor_name,
    ratings: data.glassdoor_ratings,
    culture: data.culture_scores,
    sector: data.gics.sector
  };
}

// Search for a company
async function searchCompany(query) {
  const res = await fetch(`${BASE_URL}/api/company/search?q=${encodeURIComponent(query)}`);
  return (await res.json()).results;
}

// Usage
const company = await getCompanyByISIN("US46625H1005");
console.log(company.ratings.overall);           // e.g. 3.92
console.log(company.culture.hofstede.power_distance.value);  // e.g. 0.21
console.log(company.culture.mit_big_9.innovation.value);     // e.g. 5.1
```

### curl

```bash
BASE="https://glassdoor-culture-replit-4e40ef269b11.herokuapp.com"

# Get company by ISIN
curl "$BASE/api/company/isin/GB0005405286" | python3 -m json.tool

# Search for a company
curl "$BASE/api/company/search?q=Barclays" | python3 -m json.tool

# Get culture profile
curl "$BASE/api/culture-profile/HSBC" | python3 -m json.tool

# Get Banks industry average
curl "$BASE/api/industry-average?gics_level=industry&gics_value=Banks" | python3 -m json.tool

# Get performance correlations for Financials sector
curl "$BASE/api/performance-correlation?sector=Financials" | python3 -m json.tool

# Only ratings, no culture scores
curl "$BASE/api/company/isin/GB0005405286?include_culture=false"

# Only culture scores, no ratings
curl "$BASE/api/company/isin/GB0005405286?include_ratings=false"

# Export company reviews as CSV
curl "$BASE/api/export/company-reviews/HSBC" -o hsbc_reviews.csv
```

---

## Coverage Notes

- **2,442 companies** indexed across 11 GICS sectors and 73 industries
- **~1,760 companies** with Glassdoor reviews extracted (as of March 2026)
- **~3.1 million reviews** in the database
- **~1,726 companies** with computed culture scores
- Glassdoor ratings cover reviews from **2008–2024**
- Companies are matched to their official Glassdoor profile; the `glassdoor_name` field reflects the name as shown on Glassdoor (which may differ slightly from the formal issuer name)
- 14 additional unlisted asset managers (e.g. Vanguard, PIMCO, Fidelity) are accessible by name through `/api/culture-profile/{name}` and `/api/company/search` but do not have ISINs

---

## Errors Reference

| HTTP Code | Meaning |
|-----------|---------|
| `200` | Success |
| `400` | Bad request (missing or invalid parameters) |
| `404` | Company / ISIN not found |
| `500` | Server error |
| `503` | Database unavailable |

All error responses include:
```json
{ "success": false, "error": "description of the problem" }
```

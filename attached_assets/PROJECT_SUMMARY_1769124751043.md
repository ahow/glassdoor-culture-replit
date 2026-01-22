# Project Summary - Glassdoor Trends Dashboard

## Executive Overview

The **Glassdoor Trends Dashboard** is a comprehensive web application that analyzes employee reviews from Glassdoor to extract and visualize organizational culture insights using two established frameworks: **Hofstede's Cultural Dimensions** and **MIT's Big 9 Values**.

The dashboard provides financial services companies with data-driven insights into their organizational culture, enabling HR departments and executives to understand how their company culture compares to industry peers and how it has evolved over time.

---

## Project Objectives

1. **Extract and Analyze Reviews** - Collect employee reviews from Glassdoor via RapidAPI
2. **Culture Assessment** - Score reviews using Hofstede and MIT frameworks
3. **Visualization** - Display culture metrics through interactive dashboards
4. **Benchmarking** - Compare companies against industry averages
5. **Trend Analysis** - Track culture changes over time with quarterly trends
6. **Confidence Metrics** - Provide evidence-based confidence scores for all analyses

---

## Key Features

### 1. Overview Dashboard
- Total reviews count (187,000+)
- Number of companies analyzed (44)
- Industry average rating (3.76/5.0)
- Company list with review counts and ratings
- Data update status indicator

### 2. Hofstede Framework Analysis
- **6 Bipolar Dimensions** (-1 to +1 scale):
  - Process vs Results
  - Job vs Employee
  - Professional vs Parochial
  - Open vs Closed
  - Tight vs Loose
  - Pragmatic vs Normative
- Visual spectrum display with company positioning
- Evidence-based confidence scores (0-100%)
- Comparison between two companies

### 3. MIT Big 9 Framework Analysis
- **9 Unipolar Dimensions** (0-10 scale):
  - Agility
  - Collaboration
  - Customer Orientation
  - Diversity
  - Execution
  - Innovation
  - Integrity
  - Performance
  - Respect
- Bar chart comparison
- Confidence scores for each dimension
- Company-to-company comparison

### 4. Quarterly Trends Analysis
- Historical data from 2023 to present
- Multiple companies on single chart
- Dimension selection (Overall Rating, Hofstede dimensions, MIT dimensions)
- Industry average comparison
- Trend visualization with line charts

### 5. Industry Benchmarking
- Compare individual company metrics to industry averages
- Percentile rankings
- Status indicators (Above/Below Average)
- Strengths and weaknesses identification

---

## Technology Stack

### Frontend
- **HTML5** - Markup
- **CSS3** - Styling
- **JavaScript (Vanilla)** - Interactivity
- **Chart.js** - Data visualization
- **Responsive Design** - Mobile-friendly

### Backend
- **Python 3.9+** - Programming language
- **Flask 2.3** - Web framework
- **Flask-CORS** - Cross-origin requests
- **Gunicorn** - WSGI server

### Database
- **PostgreSQL 12+** - Primary database
- **JSONB** - For storing metrics
- **Connection Pooling** - Performance optimization

### External APIs
- **RapidAPI** - Glassdoor reviews data source
- **Glassdoor API** - Review extraction

### Deployment
- **Heroku** - Cloud hosting
- **GitHub** - Version control & CI/CD
- **Git** - Local version control

---

## Data Flow Architecture

```
┌─────────────────────┐
│   RapidAPI          │
│  (Glassdoor Data)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Extraction Worker   │
│ (extraction_worker  │
│     .py)            │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  PostgreSQL DB      │
│  (reviews table)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Culture Scoring     │
│ (culture_scoring    │
│     .py)            │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Metrics Cache      │
│  (PostgreSQL JSONB) │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Flask API          │
│  (app.py)           │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Web Dashboard      │
│  (HTML/JS/CSS)      │
└─────────────────────┘
```

---

## Database Schema

### Reviews Table
```sql
CREATE TABLE reviews (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    review_text TEXT,
    rating FLOAT,
    review_date DATE,
    job_title VARCHAR(255),
    location VARCHAR(255),
    employment_status VARCHAR(50),
    recommend_to_friend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Company Metrics Cache Table
```sql
CREATE TABLE company_metrics_cache (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE,
    metrics JSONB,  -- Contains hofstede, mit_big_9, metadata
    last_updated TIMESTAMP,
    review_count INTEGER
);
```

### Quarterly Trends Table
```sql
CREATE TABLE quarterly_trends (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    quarter VARCHAR(10),
    year INTEGER,
    dimension VARCHAR(100),
    value FLOAT,
    review_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Analysis Methodologies

### Hofstede Framework Scoring

**Process**: For each review, count keywords matching each pole of each dimension, then calculate:

```
Score = (Pole_B_keywords - Pole_A_keywords) / (Pole_A_keywords + Pole_B_keywords)
```

**Result**: Score ranges from -1 (Pole A) to +1 (Pole B)

**Example**:
- Process (bureaucratic keywords): 15 matches
- Results (entrepreneurial keywords): 25 matches
- Score = (25 - 15) / (15 + 25) = 0.25 (slightly results-oriented)

### MIT Big 9 Framework Scoring

**Process**: For each review, count keywords matching each dimension:

```
Score = min(10, keyword_count × 2)
```

**Result**: Score ranges from 0 (not present) to 10 (strongly present)

**Example**:
- Collaboration keywords: 8 matches
- Score = min(10, 8 × 2) = 10 (maximum collaboration)

### Confidence Scoring

**Process**: Track keyword evidence for each dimension across all reviews:

1. Calculate total keyword evidence for each dimension
2. Find maximum evidence across all dimensions
3. Scale all dimensions relative to maximum:

```
Confidence = (dimension_evidence / max_evidence) × 100
```

**Result**: Confidence ranges from 0% (low evidence) to 100% (highest evidence)

**Example**:
- Collaboration evidence: 500 keywords
- Execution evidence: 300 keywords
- Max evidence: 500
- Collaboration confidence = 100%
- Execution confidence = (300/500) × 100 = 60%

### Recency Weighting

Recent reviews have higher influence using exponential decay:

```
Weight = exp(-days_old / 365)
```

**Result**: 
- Review from today: weight = 1.0
- Review from 1 year ago: weight = 0.37
- Review from 2 years ago: weight = 0.14

---

## API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/companies` | GET | List all companies with metrics |
| `/api/companies-list` | GET | List company names for dropdowns |
| `/api/culture-profile/<name>` | GET | Get Hofstede & MIT analysis |
| `/api/quarterly-trends` | GET | Get quarterly trend data |
| `/api/industry-average` | GET | Get industry average metrics |
| `/api/culture-comparison` | GET | Compare two companies |
| `/api/culture-trends/<name>` | GET | Get dimension trends over time |
| `/api/claude-insights/<name>` | GET | Get AI insights about company |
| `/api/culture-benchmarking/<name>` | GET | Compare to industry averages |

---

## Key Calculations

### Overall Confidence
```
overall_confidence = average(all_dimension_confidences)
```

### Industry Average
```
industry_average_score = mean(all_companies_scores_for_dimension)
```

### Percentile Ranking
```
percentile = (companies_below / total_companies) × 100
```

### Quarterly Average
```
quarterly_avg = mean(all_reviews_in_quarter)
```

---

## Performance Optimizations

1. **Database Caching** - Metrics cached for 24 hours
2. **Connection Pooling** - Reuse database connections
3. **Database Indexes** - Fast queries on company_name and review_date
4. **JSONB Storage** - Efficient metric storage and querying
5. **Recency Weighting** - Process only relevant recent reviews
6. **Batch Processing** - Score reviews in batches

---

## Security Considerations

1. **Environment Variables** - Sensitive data in .env (not in Git)
2. **CORS Configuration** - Restrict cross-origin requests
3. **Input Validation** - Sanitize company names and parameters
4. **Database Credentials** - Use connection strings, not hardcoded
5. **API Key Protection** - RapidAPI key in environment variables
6. **HTTPS** - All production traffic encrypted

---

## Scalability

### Current Capacity
- 187,000+ reviews
- 44 companies
- Quarterly data from 2023-2025

### Scaling Strategies
1. **Horizontal Scaling** - Add more Heroku dynos
2. **Database Optimization** - Upgrade PostgreSQL tier
3. **Caching Layer** - Add Redis for faster queries
4. **CDN** - Serve static assets from CDN
5. **Async Processing** - Use Celery for background jobs

---

## Future Enhancements

1. **Real-time Updates** - WebSocket for live data
2. **Advanced Analytics** - Sentiment analysis, NLP
3. **Predictive Models** - Machine learning for trend forecasting
4. **Custom Reports** - Generate PDF/Excel reports
5. **User Authentication** - Login system for saved preferences
6. **Mobile App** - Native iOS/Android applications
7. **API Monetization** - Premium API access tiers
8. **Data Export** - Download data in multiple formats
9. **Alerts** - Notify users of significant changes
10. **Integrations** - Connect with HR systems (Workday, SAP, etc.)

---

## Maintenance Schedule

| Task | Frequency | Owner |
|------|-----------|-------|
| Database Backup | Daily | DevOps |
| Cache Refresh | Every 6 hours | Automated |
| Metrics Recalculation | Weekly | Scheduled Job |
| Log Review | Daily | DevOps |
| Security Updates | As needed | DevOps |
| Performance Monitoring | Continuous | DevOps |
| Data Quality Check | Weekly | Data Team |

---

## Known Limitations

1. **Data Freshness** - Reviews updated daily, not real-time
2. **Company Coverage** - Limited to 44 financial services companies
3. **Review Availability** - Depends on RapidAPI availability
4. **Keyword Dictionaries** - Limited to predefined keywords
5. **Language** - English reviews only
6. **Historical Data** - Data available from 2023 onwards

---

## Troubleshooting Guide

### Common Issues

**Issue**: Dashboard shows "Loading data..." indefinitely
- **Cause**: Database connection timeout or slow queries
- **Solution**: Check database status, verify connection string, check Heroku logs

**Issue**: Confidence values showing as 0%
- **Cause**: Cache contains old data without confidence_score field
- **Solution**: Clear cache table and recalculate metrics

**Issue**: Quarterly trends only showing last 12 quarters
- **Cause**: LIMIT clause in SQL query
- **Solution**: Remove LIMIT to show full history

**Issue**: Company not appearing in dropdown
- **Cause**: Company name doesn't match database exactly
- **Solution**: Check exact spelling in database, use GET /api/companies-list

---

## Team Responsibilities

| Role | Responsibilities |
|------|------------------|
| **Backend Developer** | Maintain Flask API, database queries, data processing |
| **Frontend Developer** | Dashboard UI, visualizations, user interactions |
| **Data Engineer** | Data extraction, ETL pipelines, data quality |
| **DevOps Engineer** | Heroku deployment, database management, monitoring |
| **Data Analyst** | Validate calculations, ensure data accuracy |

---

## Contact & Support

For questions or issues:
1. Check documentation in `/documentation` folder
2. Review API reference in `API_REFERENCE.md`
3. Check setup guide in `SETUP_GUIDE.md`
4. Review GitHub issues: https://github.com/ahow/Glassdoor-analysis-heroku/issues
5. Contact project owner: ahow

---

**Last Updated**: January 22, 2026
**Version**: 1.0
**Status**: Production Ready

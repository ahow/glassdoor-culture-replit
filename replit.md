# Glassdoor Trends Dashboard

## Overview

This is a full-stack web application that analyzes employee reviews from Glassdoor to extract and visualize organizational culture insights for financial services companies.

**Active framework (since 2026-07-21 Schroders-only rebuild): Schroders v2** — 12 bipolar dimensions (−1 to +1) scored with a mined dictionary, plus sector-relative shrinkage, evidence tiers (A–D), a ridge-model Culture Factor per company, and reliability tiers (High/Medium/Low/Insufficient). New API endpoints: `/api/v2/factor-scores`, `/api/v2/evidence/<company>`, `/api/v2/model-weights`, `/api/v2/overlap-diagnostics`. Pipeline: `pipeline/factor_build.py`; validation report: `pipeline_output/schroders_rebuild_validation_report.md`.

**Retired frameworks** (hidden in UI, code retained; restore via `snapshots/pre_schroders_sector_relative_rebuild_2026_07_21/ROLLBACK.md`):
1. **Hofstede Framework** - 6 bipolar dimensions measuring organizational culture on a -1 to +1 scale
2. **MIT Big 9 Framework** - 9 unipolar dimensions measuring culture attributes on a 0-10 scale

The application extracts reviews via OpenWeb Ninja API (primary) or RapidAPI (fallback), scores them using keyword-based analysis, stores results in PostgreSQL, and presents insights through an interactive dashboard. It currently analyzes 50+ companies with 147,000+ reviews, with infrastructure to expand to 2,442 companies across 11 GICS sectors plus a dedicated "Asset Management" category for 14 unlisted firms.

### Unlisted Asset Managers
14 private/unlisted asset management companies are hard-coded in `UNLISTED_ASSET_MANAGERS` dict in `app.py` with sector "Asset Management". These don't have ISINs and aren't in the extraction_queue (which contains 2,442 MSCI-listed companies). They're included in the analysis for sector-specific filtering and appear in the sector dropdown as a separate category. Companies: AllianceBernstein, Dimensional Fund Advisors, Eurazeo, Federated Hermes, Fidelity International, Fidelity Investments, Franklin Templeton, Invesco, Natixis Investment Managers, Nuveen, PIMCO, Robeco, Vanguard Group, Wellington Management.

### Listed vs Asset-Manager separation (2026-07-26)
The Extraction Manager "Listed Companies / Asset Managers" toggle (`window._extractionGroup`, default `listed`) drives the global header GICS filter dropdown: in listed mode the synthetic "Asset Management" pseudo-sector is hidden; in asset-managers mode it is the only option. Switching the toggle resets an invalidated filter selection to All. The Culture Factor tab (factor table, evidence dropdown) follows the global GICS filter, and its "Model Fit & Dimension Weights" section shows per-peer-group ridge R² (in-sample + cross-validated) and dimension weights from `/api/v2/model-weights`.

### Authentication (added 2026-07-25)
Email/password login (Flask session, `SESSION_SECRET`), registration restricted to @schroders.com. `app_users` table with werkzeug hashes; seeded admin andy.howard@schroders.com. `before_request` guard: everything requires login except /login, /api/auth/*, /static; admin-only path prefixes: /api/extraction, /api/admin, /api/data-status, /api/score-company, /api/scoring, /api/rescore. Admin-only nav tabs (Data Status, Extraction Manager, Users) are Jinja-gated. Users tab lets admins grant/remove admin and delete accounts. Login page: `templates/login.html`.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Dual Stack Design

The project contains two parallel implementations that serve different purposes:

**Python/Flask Backend (Production Dashboard)**
- `app.py` - Main Flask application serving the Glassdoor dashboard
- `culture_scoring.py` - Hofstede and MIT Big 9 scoring algorithms using keyword dictionaries
- `performance_analysis.py` - Culture-performance correlation analysis module
- `extraction_worker.py` - RapidAPI data extraction for individual companies (legacy)
- `extraction_openweb.py` - OpenWeb Ninja API extraction (primary) with RapidAPI fallback, CSV export support
- `extraction_manager.py` - Dashboard-controlled sector-by-sector extraction with pause/resume, company matching, and status tracking
- `extraction_orchestrator.py` - Parallel extraction management across all companies (legacy)
- `templates/` - Jinja2 HTML templates for the dashboard UI
- Database: PostgreSQL via psycopg2 with direct SQL queries

**Performance Analysis Feature**
- Loads company performance data from Excel (ROE, AUM growth, TSR, operating margins)
- Calculates composite performance scores normalized within business model categories
- Computes Pearson correlations between 15 culture dimensions and 4 performance metrics
- API endpoints: `/api/performance-correlation`, `/api/company-performance`, `/api/performance-rankings`
- Performance Insights tab displays correlation analysis with visualizations

**FMP Performance Module** (`fmp_performance.py`)
- ISIN-to-ticker resolution via FMP API search-isin endpoint
- Financial data fetching: key metrics, income statements, ratios, stock price history
- PostgreSQL caching with 30-day expiry (tables: fmp_ticker_map, fmp_financial_cache, fmp_performance_metrics)
- Sector-specific peer statistics and composite performance scoring
- Culture-performance correlation calculation using scipy Pearson correlations

**Multi-Level GICS Filtering**
- All major API endpoints accept `?gics_level=` (sector/industry/sub_industry) and `?gics_value=` parameters
- Legacy `?sector=` parameter still works (treated as gics_level=sector)
- `/api/sectors` returns list of 11 GICS sectors
- `/api/gics-hierarchy` returns full hierarchy: sectors → industries → sub-industries for companies with reviews
- Dashboard header has cascading GICS filter: Level selector (Sector/Industry/Sub-Industry) → Value dropdown
- `_company_gics_map` stores full GICS info (sector, industry, sub_industry) for each company
- `get_companies_for_sector()` accepts `gics_level` and `gics_value` for flexible filtering
- 2,442 companies across 11 sectors / 73 industries / 158 sub-industries

**FMP Financial Performance Integration**
- `/api/fetch-fmp-performance` POST endpoint batch-fetches FMP data for companies with ISINs
- Processes companies that have reviews but no performance data yet
- `fmp_performance_metrics` table stores: ROE, operating margin, TSR, revenue growth, market cap
- Table includes `gics_industry`, `gics_sub_industry`, and `data_source` columns
- `data_source` tracks origin: 'fmp' (API) or 'excel' (uploaded spreadsheet)

**Excel Performance Data for Asset Management**
- `load_excel_performance_data()` runs on startup, loads from `attached_assets/asset_manager_comprehensive_database_1769351810411.xlsx`
- Reads 4 sheets: AUM Data, Financials & Profitability, Business Performance, Shareholder Returns
- Maps Glassdoor company names to Excel names via `GLASSDOOR_TO_EXCEL` dict
- Inserts into `fmp_performance_metrics` with `data_source='excel'`, `gics_sector='Asset Management'`
- Excel data is primary source; FMP supplements for listed asset managers

**TypeScript/Node.js Stack (Secondary/Development)**
- `server/` - Express.js server with route handling
- `client/` - React frontend with Vite, TailwindCSS, and shadcn/ui components
- `shared/schema.ts` - Drizzle ORM schema definitions
- Database: PostgreSQL via Drizzle ORM

### Data Flow

1. **Extraction**: RapidAPI Glassdoor endpoint → `extraction_worker.py` → PostgreSQL `reviews` table
2. **Scoring**: Reviews text → `culture_scoring.py` keyword matching → Culture dimension scores
3. **Aggregation**: Individual review scores → Company-level metrics with confidence scores
4. **Display**: Flask API endpoints → Chart.js/Plotly visualizations in browser

### Key Design Decisions

**Keyword-Based Scoring**
- Problem: Need to extract culture signals from unstructured review text
- Solution: Dictionary-based keyword matching for each culture dimension
- Rationale: Simple, interpretable, no ML training required
- Trade-off: Less nuanced than NLP but more transparent and maintainable

**On-Demand Database Queries**
- Problem: Large dataset (187K+ reviews) could cause memory issues
- Solution: Query database per-request rather than pre-loading all data
- Rationale: Reduces memory footprint, ensures fresh data

**Confidence Scoring**
- Reviews < 20: Low confidence
- Reviews 20-50: Medium confidence  
- Reviews > 50: High confidence

### Database Schema (PostgreSQL)

Primary tables managed by Python backend:
- `reviews` - Raw Glassdoor reviews with all metadata fields
- `extraction_status` - Tracks extraction progress per company
- `review_culture_scores` - Cached culture dimension scores per review

Drizzle schema (TypeScript side):
- `users` - Basic authentication table

## External Dependencies

### APIs
- **OpenWeb Ninja Glassdoor** (Primary) - Real-time Glassdoor data extraction
  - Base URL: `https://api.openwebninja.com/realtime-glassdoor-data`
  - Auth header: `x-api-key`
  - Environment variable: `OPENWEB_NINJA_API`
  - Endpoints: `company-reviews`, `company-search`, `company-overview`
- **RapidAPI Glassdoor** (Fallback) - Real-time Glassdoor data extraction
  - Host: `real-time-glassdoor-data.p.rapidapi.com`
  - Environment variables: `RAPIDAPI_KEY`, `RAPIDAPI_KEY_1`, `RAPIDAPI_KEY_2`

### Database
- **PostgreSQL** - Primary data store
  - Connection: `DATABASE_URL` environment variable
  - Note: URL may use `postgres://` or `postgresql://` prefix (code handles both)

### Deployment
- **Heroku** - Production hosting platform
  - Uses `gunicorn` for Python WSGI
  - `Procfile` and `runtime.txt` for configuration

### Frontend Libraries (Dashboard)
- Chart.js 4.4.0 - Interactive charts
- Plotly - Additional visualization

### Frontend Libraries (React Client)
- React with Vite build system
- TailwindCSS with shadcn/ui components
- Radix UI primitives
- TanStack React Query for data fetching
- Wouter for routing
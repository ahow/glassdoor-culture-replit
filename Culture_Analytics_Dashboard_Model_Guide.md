# Culture Analytics Dashboard — Model & User Guide

## What this dashboard is

The Culture Analytics Dashboard analyses employee reviews published on Glassdoor to extract signals about organisational culture across financial-services companies. It translates large volumes of unstructured review text into structured, quantitative culture scores, then links those scores to company financial performance to test whether culture predicts outcomes.

The dashboard covers **2,442 MSCI-listed financial-services companies** across 11 GICS sectors, plus 14 unlisted asset managers, and currently holds **147,000+ reviews**. All data is refreshed monthly.

---

## Data pipeline — how the data flows

```
Glassdoor reviews (API)
        ↓
Raw review text stored in PostgreSQL
        ↓
Keyword-based culture scoring (per review)
        ↓
Company-level aggregation (mean scores, confidence)
        ↓
Financial performance linkage (FMP API / Excel)
        ↓
Dashboard visualisations and correlation analysis
```

### Step 1 — Review extraction

Reviews are fetched from Glassdoor via the RapidAPI real-time Glassdoor endpoint (primary), with OpenWeb Ninja as a fallback. For each company the system retrieves:

- Rating (overall, and 8 sub-ratings covering career opportunities, compensation, work-life balance, senior management, culture & values, diversity, CEO approval, business outlook)
- Free-text fields: summary, pros, cons, advice to management
- Reviewer metadata: job title, employment status, current/former employee, tenure, location, date

Reviews are stored in PostgreSQL with duplicate detection by review ID. On the first run for a company, all pages are fetched. On subsequent monthly updates, only new pages are fetched — the system stops as soon as two consecutive pages contain only already-known review IDs.

### Step 2 — Culture scoring (per review)

Each review's text (pros + cons + summary + advice) is scored against two keyword dictionaries. This is a **keyword-matching approach**: the scoring engine searches the lowercased review text for specific phrases and terms that signal a particular culture dimension.

See the two framework sections below for the full logic.

### Step 3 — Company-level aggregation

Individual review scores are averaged to produce a company-level profile:

- **Mean score** across all reviews that mentioned each dimension
- **Standard deviation** measuring how consistent or divided employee opinion is
- **Evidence count** — total number of keyword matches across all reviews for that dimension

Only reviews that contain at least one keyword relevant to a dimension contribute to that dimension's score. Reviews that say nothing relevant to a dimension are excluded from that dimension's average (they do not drag the score towards zero).

### Step 4 — Confidence classification

| Reviews available | Confidence level |
|---|---|
| Fewer than 20 | Low |
| 20 – 50 | Medium |
| More than 50 | High |

Low-confidence scores are flagged in the dashboard but are not hidden. They should be interpreted cautiously.

### Step 5 — Performance linkage

Financial performance data comes from two sources:

- **FMP API (Financial Modelling Prep)** — for listed companies: return on equity (ROE), operating margin, total shareholder return (TSR), revenue growth, and market capitalisation. Data is cached in PostgreSQL for 30 days and refreshed on demand.
- **Excel dataset** — for asset managers (including unlisted firms): AUM growth, profitability, business performance, and shareholder returns from a curated spreadsheet.

A composite performance score is calculated per company, normalised within business-model peer groups (Traditional, Alternative, Insurance/Wealth) so that structurally different firms are compared to appropriate benchmarks.

---

## Framework 1 — Hofstede Organisational Culture Dimensions

The Hofstede framework describes culture along **six bipolar dimensions**. Each dimension has two opposing poles. A company's score on each dimension runs from **−1 to +1**, where −1 means the culture strongly exhibits the left pole and +1 means it strongly exhibits the right pole.

### Scoring formula (per review, per dimension)

```
score = (right-pole keyword matches − left-pole keyword matches)
        ──────────────────────────────────────────────────────────
              total keyword matches (both poles combined)
```

If a review contains no keywords for a dimension, that dimension is marked as "not discussed" and excluded from the company average for that dimension.

### The six dimensions

| Dimension | Left pole (score → −1) | Right pole (score → +1) |
|---|---|---|
| **Process vs Results** | Emphasis on procedures, compliance, documentation, sign-offs, risk-aversion | Emphasis on outcomes, targets, delivery, accountability, speed |
| **Job vs Employee** | Task-completion focus, efficiency, productivity as ends in themselves | Employee wellbeing, work-life balance, personal development, caring culture |
| **Professional vs Parochial** | Identity tied to professional standards and industry expertise | Identity tied to the company itself; strong internal culture and loyalty |
| **Open vs Closed** | Insular, insider culture, resistant to external ideas or change | Welcoming of diverse perspectives, external hires, and new thinking |
| **Tight vs Loose Control** | Hierarchical, top-down, formal approvals, micromanagement | Autonomous, flat structure, flexible, entrepreneurial, self-directed |
| **Pragmatic vs Normative** | Results-over-rules, market-driven, pragmatic flexibility | Values-driven, mission-focused, principled, ethical standards |

### How to read a Hofstede chart

A score of 0 on any dimension means either the culture sits genuinely in the middle, or that reviewers did not discuss that dimension enough to shift the needle. A high standard deviation means employees have strongly divided opinions — some see it one way, others see it the opposite way.

---

## Framework 2 — MIT Big 9 Culture Dimensions

The MIT framework describes culture along **nine unipolar dimensions**. Each dimension measures the presence or strength of a particular positive quality. Scores run from **0 to 10**, where 10 represents very strong evidence of that quality in review text.

### Scoring formula (per review, per dimension)

```
keyword matches for this dimension → converted to 0–10 scale
(each keyword hit = +1 point; capped at 5 matches = 10 points)
score = min(10,  keyword_hits × 2)
```

Unlike Hofstede, there are no opposing poles. A score of 0 means no evidence of the quality in that review — it does not imply the opposite quality is present.

### The nine dimensions

| Dimension | What it measures |
|---|---|
| **Agility** | Speed of response, adaptability, flexibility, fast decision-making |
| **Collaboration** | Teamwork, cross-functional cooperation, collective working |
| **Customer Orientation** | Focus on client and customer needs; customer-centric thinking |
| **Diversity** | Inclusivity, equal opportunity, multicultural environment |
| **Execution** | Delivery culture, accountability, following through on commitments |
| **Innovation** | Creativity, appetite for new ideas, forward-thinking |
| **Integrity** | Ethical behaviour, honesty, transparency, trustworthiness |
| **Performance** | Meritocracy, high standards, results culture |
| **Respect** | Psychological safety, dignity, supportive workplace |

### How to read a MIT Big 9 chart

Higher scores mean more explicit mentions of a quality in reviews. Because this is keyword-based, a low score can mean either that the quality is absent, or simply that reviewers did not discuss it. The evidence count (shown in tooltips) helps distinguish these cases.

---

## GICS filtering

All analysis can be filtered at three geographic/sector levels:

| Level | Examples |
|---|---|
| **Sector** (broadest) | Financials, Real Estate, Information Technology |
| **Industry** | Banks, Capital Markets, Insurance, Diversified Financials |
| **Sub-industry** (most granular) | Investment Banking & Brokerage, Life & Health Insurance |

The cascading filter in the dashboard header lets you select any combination. Changing the filter reruns all displayed calculations for that subset of companies.

---

## Dashboard pages

### Overview
Summary statistics for the full dataset (total companies, total reviews, companies scored, companies with financial data). Explains the two frameworks and provides a guide to each page.

### Company Details
Searchable table of all companies with their sector, review count, average Glassdoor rating, and confidence level. Click a company to see its full culture profile.

### Company Analysis
Detailed culture profile for a selected company: radar charts for both frameworks, individual dimension scores, and the underlying review statistics. Includes a comparison to the sector average.

### Quarterly Trends
Tracks how a company's culture scores have changed over time, calculated on a rolling quarterly basis. Useful for spotting cultural drift following events such as mergers, leadership changes, or restructuring.

### Hofstede Framework
Sector-level and industry-level view of the six Hofstede dimensions. Includes a heatmap showing how all companies in a selected group score across all six dimensions simultaneously, allowing quick spotting of outliers.

### MIT Framework
Equivalent to the Hofstede tab but for the nine MIT dimensions. Shows bar charts of dimension averages and a company ranking for each dimension.

### Performance Insights
Pearson correlation coefficients between each of the 15 culture dimensions and four performance metrics (ROE, AUM growth, TSR, operating margin). Displays significance indicators and scatter plots. Based on the subset of companies where both culture scores and financial performance data are available.

### Culture vs Performance
Scatter plot view: choose any culture dimension on one axis and any performance metric on the other. Each point is a company; hovering shows the name and values. The regression line and R² are displayed.

### Correlation Analysis
A 3×3 summary matrix showing average R² values across all combinations of GICS level (Sector / Industry / Sub-industry) and score type (Hofstede / MIT / Combined). Allows drilling down into any cell to see a detailed breakdown of which dimensions correlate most strongly with performance within a chosen group. Designed to identify where culture-performance links are strongest.

### Export Data
Download the full company-level culture and performance dataset as a CSV file for use in external analysis tools.

### Data Status
Live view of data coverage: how many of the 2,442 companies in scope have Glassdoor reviews, culture scores, and financial performance data. Shows last extraction date and coverage percentages.

### Extraction Manager
Administrative control panel for data collection. Allows manual triggering or pausing of Glassdoor review extraction on a sector-by-sector basis, running the culture scoring pipeline over unscored reviews, and triggering an incremental update that fetches only newly published reviews for all companies (also runs automatically on the 1st of each month).

---

## Limitations and interpretation notes

**Keyword matching is transparent but imprecise.** The scoring engine counts specific phrases; it does not understand context, irony, or nuance. A review that says "there is no work-life balance" and a review that says "great work-life balance" may score similarly if both contain the phrase "work-life balance" and the scoring does not separate negation effectively. The aggregation across many reviews reduces this noise but does not eliminate it.

**Review self-selection bias.** Glassdoor reviews are written disproportionately by people who feel strongly — either positively or negatively. The sample may not represent average employee opinion, particularly for companies with very few reviews.

**Recency.** Reviews span several years. Culture evolves. A company's score reflects its aggregated history, which may not capture recent changes. Use the Quarterly Trends page to assess how current the pattern is.

**Correlation is not causation.** The culture-performance correlations identify statistical associations. They do not prove that a particular culture dimension causes a company to outperform or underperform.

**Coverage varies by company size.** Large, well-known companies have hundreds of reviews; smaller or less prominent companies may have fewer than 20, producing low-confidence scores.

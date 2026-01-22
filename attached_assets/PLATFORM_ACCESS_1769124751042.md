# Platform Access & Credentials Guide

## Overview

This document contains all information needed to access and manage the external platforms and services used by the Glassdoor Trends Dashboard.

---

## 1. GitHub Repository

### Repository Details
- **Owner**: ahow
- **Repository Name**: Glassdoor-analysis-heroku
- **URL**: https://github.com/ahow/Glassdoor-analysis-heroku
- **Visibility**: Private
- **Type**: Flask Python application

### Access Setup
1. Request access from repository owner (ahow)
2. Add SSH key to GitHub account
3. Clone repository:
   ```bash
   gh repo clone ahow/Glassdoor-analysis-heroku
   ```

### Key Files in Repository
- `app.py` - Main Flask application
- `culture_scoring.py` - Scoring algorithms
- `templates/index.html` - Dashboard frontend
- `requirements.txt` - Python dependencies
- `Procfile` - Heroku deployment configuration
- `.env` - Environment variables (not in repo, create locally)

### Git Workflow
```bash
# Clone
gh repo clone ahow/Glassdoor-analysis-heroku

# Create feature branch
git checkout -b feature/your-feature

# Make changes and commit
git add .
git commit -m "Description of changes"

# Push to GitHub
git push origin feature/your-feature

# Create pull request on GitHub
```

### Automatic Deployment
- GitHub is connected to Heroku with automatic deployments
- Pushing to `main` branch automatically triggers Heroku deployment
- Deployment takes 2-5 minutes
- Check Heroku dashboard for deployment status

---

## 2. Heroku Application

### App Details
- **App Name**: glassdoor-extraction-system
- **URL**: https://glassdoor-extraction-system-76c88205adba.herokuapp.com/
- **Region**: US
- **Dyno Type**: Free tier (or paid, depending on current setup)

### Access Setup
1. Create Heroku account at https://www.heroku.com
2. Request access from app owner
3. Install Heroku CLI:
   ```bash
   curl https://cli-assets.heroku.com/install.sh | sh
   ```
4. Login to Heroku:
   ```bash
   heroku login
   ```

### Useful Heroku Commands
```bash
# View app logs
heroku logs --app glassdoor-extraction-system --tail

# View environment variables
heroku config --app glassdoor-extraction-system

# Set environment variable
heroku config:set VAR_NAME=value --app glassdoor-extraction-system

# Manual deployment
heroku deploy --app glassdoor-extraction-system

# Access Heroku PostgreSQL
heroku pg:psql --app glassdoor-extraction-system
```

### Environment Variables (Set in Heroku)
```
DATABASE_URL=postgresql://[user]:[password]@[host]:[port]/[database]
RAPIDAPI_KEY=[your-rapidapi-key]
RAPIDAPI_HOST=glassdoor-api.p.rapidapi.com
FLASK_ENV=production
```

### Database Connection
- **Type**: PostgreSQL
- **Managed by**: Heroku Postgres
- **Access**: Via `heroku pg:psql` or connection string
- **Backups**: Automatic daily backups (check Heroku dashboard)

---

## 3. RapidAPI Glassdoor Source

### API Details
- **Service**: Glassdoor Reviews API
- **Platform**: RapidAPI
- **URL**: https://rapidapi.com/
- **Endpoint**: `glassdoor-api.p.rapidapi.com`

### Access Setup
1. Create RapidAPI account at https://rapidapi.com
2. Search for "Glassdoor" API
3. Subscribe to the API (free tier available)
4. Copy API key from dashboard
5. Add to Heroku environment variables:
   ```bash
   heroku config:set RAPIDAPI_KEY=your-api-key --app glassdoor-extraction-system
   ```

### API Endpoints Used
The application uses the following RapidAPI Glassdoor endpoints:

#### 1. **Company Reviews Endpoint**
```
GET https://glassdoor-api.p.rapidapi.com/reviews
Parameters:
  - companyId: [company-id]
  - limit: [number-of-reviews]
  - sort: [sort-order]
```

#### 2. **Company Search Endpoint**
```
GET https://glassdoor-api.p.rapidapi.com/companies
Parameters:
  - name: [company-name]
```

#### 3. **Company Details Endpoint**
```
GET https://glassdoor-api.p.rapidapi.com/company
Parameters:
  - companyId: [company-id]
```

### Rate Limits
- **Free Tier**: Limited requests per month
- **Paid Tier**: Higher rate limits
- **Current Usage**: Monitor in RapidAPI dashboard
- **Best Practice**: Cache results to minimize API calls

### API Response Format
```json
{
  "success": true,
  "data": {
    "reviews": [
      {
        "id": "review-id",
        "rating": 4.5,
        "title": "Review title",
        "summary": "Review text...",
        "date": "2024-01-15",
        "pros": "...",
        "cons": "...",
        "company_id": "123456"
      }
    ]
  }
}
```

### Data Collection Strategy
- **Initial Load**: Collect all available reviews for each company
- **Ongoing**: Periodically check for new reviews
- **Storage**: Store in PostgreSQL for analysis
- **Caching**: Use application cache to reduce API calls

---

## 4. PostgreSQL Database

### Database Details
- **Provider**: Heroku Postgres
- **Type**: PostgreSQL 12+
- **Size**: Varies (check Heroku dashboard)
- **Backups**: Automatic daily

### Connection Details
```
Host: [provided-by-heroku]
Port: 5432
Database: [database-name]
User: [username]
Password: [password]
SSL: Required (sslmode=require)
```

### Connection Methods

#### Via Heroku CLI
```bash
heroku pg:psql --app glassdoor-extraction-system
```

#### Via Connection String (Python)
```python
import psycopg2
conn = psycopg2.connect(
    host="[host]",
    database="[database]",
    user="[user]",
    password="[password]",
    port=5432,
    sslmode="require"
)
```

#### Via Connection String (Direct)
```
postgresql://[user]:[password]@[host]:[port]/[database]?sslmode=require
```

### Database Tables
See `DATA_MODEL.md` for complete schema details.

Main tables:
- `reviews` - Glassdoor reviews
- `company_metrics_cache` - Cached analysis results
- `quarterly_data` - Quarterly trend data

### Backup & Restore

#### Create Backup
```bash
heroku pg:backups:capture --app glassdoor-extraction-system
```

#### List Backups
```bash
heroku pg:backups --app glassdoor-extraction-system
```

#### Download Backup
```bash
heroku pg:backups:download --app glassdoor-extraction-system
```

#### Restore from Backup
```bash
heroku pg:backups:restore [backup-id] --app glassdoor-extraction-system
```

---

## 5. Local Development Setup

### Prerequisites
- Python 3.11+
- PostgreSQL 12+ (local or remote)
- Git
- Virtual environment (recommended)

### Environment Variables (.env file)
Create a `.env` file in the project root:
```
DATABASE_URL=postgresql://user:password@localhost:5432/glassdoor_dev
RAPIDAPI_KEY=your-rapidapi-key
RAPIDAPI_HOST=glassdoor-api.p.rapidapi.com
FLASK_ENV=development
DEBUG=True
```

### Installation
```bash
# Clone repository
gh repo clone ahow/Glassdoor-analysis-heroku
cd Glassdoor-analysis-heroku

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize database
python3 -c "from app import init_cache_table; init_cache_table()"

# Run application
python3 app.py
```

### Access Local Application
- URL: http://localhost:5000
- Dashboard: http://localhost:5000/
- API: http://localhost:5000/api/companies

---

## 6. Security Best Practices

### API Keys
- **Never commit** API keys to GitHub
- **Use environment variables** for all sensitive data
- **Rotate keys** periodically
- **Use .gitignore** to exclude `.env` files

### Database
- **Use SSL** for all connections
- **Regular backups** (automated by Heroku)
- **Access control** - limit who has database access
- **Monitor** unusual access patterns

### Heroku
- **Enable two-factor authentication**
- **Use strong passwords**
- **Regularly review** who has access
- **Monitor logs** for errors

---

## 7. Monitoring & Maintenance

### Heroku Dashboard
- Monitor dyno usage
- Check application logs
- Review database size
- Manage environment variables
- View deployment history

### RapidAPI Dashboard
- Monitor API usage
- Check rate limits
- Review billing
- Update subscription as needed

### GitHub
- Review pull requests
- Check deployment status
- Monitor commit history
- Manage collaborators

---

## 8. Troubleshooting

### Can't Connect to Database
```bash
# Check connection string
heroku config:get DATABASE_URL --app glassdoor-extraction-system

# Test connection
heroku pg:psql --app glassdoor-extraction-system
```

### API Rate Limit Exceeded
- Check RapidAPI dashboard for usage
- Upgrade to paid tier if needed
- Implement better caching strategy
- Reduce API call frequency

### Heroku Deployment Failed
```bash
# Check logs
heroku logs --app glassdoor-extraction-system --tail

# Check for errors
git push heroku main  # Manual push
```

### Application Errors
- Check Heroku logs
- Check local development environment
- Verify environment variables are set
- Check database connection

---

## 9. Contact Information

For access issues, contact:
- **Repository Owner**: ahow (GitHub)
- **Heroku App Owner**: [contact-info]
- **RapidAPI Account**: [your-email]

---

**Last Updated**: January 22, 2026
**Status**: Current

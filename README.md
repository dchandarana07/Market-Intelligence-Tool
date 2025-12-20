# Market Intelligence Tool

A comprehensive market intelligence automation tool that aggregates data from multiple sources to provide insights into job markets, skills demand, educational resources, and market trends.

## Features

### 📊 Data Collection Modules

1. **Job Postings & Labor Data**
   - Real-time job postings via SerpAPI
   - Bureau of Labor Statistics (BLS) employment and wage data
   - Automated skills extraction from job descriptions
   - Salary range analysis

2. **Online Courses**
   - Automated course discovery from Coursera and EdX
   - Course metadata (duration, price, ratings, certificates)
   - Provider and difficulty level information

3. **Google Trends Analysis**
   - Interest over time for skills and technologies
   - Trend direction indicators (Rising/Falling/Stable)
   - Regional interest patterns

4. **Skills Enrichment (Lightcast)**
   - Skills normalization using Lightcast API
   - Skill taxonomy mapping
   - Skill type classification (Hard/Soft/Certification)

### 🎯 Key Capabilities

- **Wizard-style Web Interface**: Easy-to-use step-by-step workflow
- **Multi-module Pipeline**: Run multiple data collection modules in parallel
- **Google Sheets Integration**: Automatic export to Google Sheets with OAuth
- **Real-time Progress Tracking**: Live status updates during execution
- **Flexible Configuration**: Customize each module's parameters

## Architecture

```
market-intelligence/
├── app/
│   ├── main.py                 # FastAPI application entry point
│   ├── modules/               # Data collection modules
│   │   ├── base.py           # Base module interface
│   │   ├── jobs.py           # Jobs + BLS data module
│   │   ├── courses.py        # Courses scraping module
│   │   ├── trends.py         # Google Trends module
│   │   └── lightcast.py      # Lightcast skills API module
│   ├── services/             # Core services
│   │   ├── orchestrator.py   # Pipeline coordination
│   │   ├── google_sheets.py  # Sheets output service
│   │   └── email.py          # Email notifications
│   ├── templates/            # HTML templates
│   └── static/              # CSS, JS assets
├── config/
│   ├── settings.py          # Configuration management
│   └── google-credentials.json  # Google OAuth credentials
├── requirements.txt
└── run.py                   # Simple run script
```

## Prerequisites

- Python 3.10+
- Google Cloud Project with Google Sheets & Drive API enabled
- API Keys:
  - SerpAPI (for job postings)
  - Lightcast (for skills enrichment) - optional
  - BLS API key - optional (higher rate limits)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd market-intelligence
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
# Google Cloud / Drive / Sheets
GOOGLE_CREDENTIALS_PATH=./config/google-credentials.json
GOOGLE_DRIVE_FOLDER_ID=your_folder_id_here

# SerpAPI (Required for Jobs module)
SERPAPI_KEY=your_serpapi_key_here

# BLS API (Optional - improves rate limits)
BLS_API_KEY=your_bls_key_here

# Lightcast API (Optional - for skills enrichment)
LIGHTCAST_CLIENT_ID=your_client_id_here
LIGHTCAST_CLIENT_SECRET=your_client_secret_here

# Email Configuration (Optional)
EMAIL_SENDER=your_email@gmail.com
EMAIL_APP_PASSWORD=your_app_password_here
```

### 5. Setup Google OAuth

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable Google Sheets API and Google Drive API
4. Create OAuth 2.0 credentials (Desktop app)
5. Download credentials JSON and save as `config/google-credentials.json`
6. Run the OAuth flow:

```bash
python scripts/setup_google_oauth.py
```

This will open a browser for authorization and save the token.

### 6. Get API Keys

**SerpAPI** (Required):
- Sign up at https://serpapi.com/
- Get free API key (100 searches/month)

**Lightcast** (Optional):
- Apply at https://lightcast.io/open-skills/access
- Free tier: 50 skill normalizations/month

**BLS API** (Optional):
- Register at https://www.bls.gov/developers/
- Free: 500 queries/day vs 25 without key

## Usage

### Running the Application

```bash
# Using the run script
python run.py

# Or directly with uvicorn
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Access the application at: http://localhost:8000

### Workflow

1. **Enter Email & Topic**: Provide your email and research topic
2. **Select Modules**: Choose which data sources to query
3. **Configure Modules**: Set parameters for each module
4. **Review & Run**: Confirm settings and execute pipeline
5. **View Results**: Access Google Sheets output with all data

## Output Format

The tool creates a Google Sheets spreadsheet with multiple tabs:

- **Job Postings**: Job listings with salaries, skills, descriptions
- **BLS Labor Data**: Employment statistics and wage data
- **Skills Summary**: Extracted skills with frequency analysis
- **Courses**: Relevant online courses from multiple platforms
- **Trends Data**: Interest over time for key skills/technologies
- **Skills Normalized**: Lightcast-normalized skill taxonomy

## Configuration

### Module Availability

Modules automatically check for required credentials:

| Module | Required | Optional |
|--------|----------|----------|
| Jobs | SERPAPI_KEY | BLS_API_KEY |
| Courses | Chrome browser | - |
| Trends | SERPAPI_KEY | - |
| Lightcast | LIGHTCAST credentials | - |

### Settings

Edit `config/settings.py` or use environment variables:

- `DEBUG`: Enable detailed logging
- `ENVIRONMENT`: development/production
- `DEFAULT_SHARING_MODE`: restricted/anyone
- Rate limits and delays

## Development

### Project Structure

- **Modules**: Extend `BaseModule` class in `app/modules/base.py`
- **Services**: Core services in `app/services/`
- **Configuration**: Centralized in `config/settings.py`
- **Templates**: Jinja2 templates in `app/templates/`

### Adding a New Module

1. Create new file in `app/modules/`
2. Extend `BaseModule` class
3. Implement required methods
4. Register in `orchestrator.py`

### Testing

```bash
# Run tests
pytest tests/

# Test specific module
pytest tests/test_jobs.py
```

## Troubleshooting

### ChromeDriver Issues (Courses Module)

If you see ChromeDriver errors:

```bash
# Clear ChromeDriver cache
rm -rf ~/.wdm/
```

The module will automatically re-download the correct version.

### Google Sheets Permission Errors

Ensure your Google Drive folder ID is correct and the OAuth token has appropriate scopes:
- `https://www.googleapis.com/auth/spreadsheets`
- `https://www.googleapis.com/auth/drive.file`

### API Rate Limits

- SerpAPI: 100 searches/month (free tier)
- Lightcast: 50 normalizations/month (free tier)
- BLS: 25/day without key, 500/day with key

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Acknowledgments

- SerpAPI for job search data
- Bureau of Labor Statistics for employment data
- Lightcast for skills taxonomy
- Coursera and EdX for educational content

## Support

For issues and questions:
- Create an issue on GitHub
- Contact: [Your contact information]

---

Built with ❤️ for ASU Learning Enterprise

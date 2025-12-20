"""
Jobs Module - Google Jobs via SerpAPI + BLS Labor Statistics

Collects job postings from Google Jobs and enriches with BLS employment/wage data.

Free tier limits:
- SerpAPI: 100 searches/month
- BLS API v1: 25 queries/day (no key), v2: 500 queries/day (with key)
"""

import logging
import re
from datetime import datetime
from typing import Any, Optional
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.modules.base import (
    BaseModule,
    InputField,
    OutputColumn,
    ValidationResult,
    ModuleResult,
    ModuleStatus,
)
from config.settings import settings

logger = logging.getLogger(__name__)


# Common skills keywords for extraction (basic extraction without Lightcast)
COMMON_SKILLS = [
    # Technical
    "python", "java", "javascript", "typescript", "sql", "nosql", "aws", "azure", "gcp",
    "docker", "kubernetes", "react", "angular", "vue", "node.js", "django", "flask",
    "machine learning", "deep learning", "data science", "data analysis", "statistics",
    "excel", "tableau", "power bi", "salesforce", "sap", "oracle",
    # Soft skills
    "communication", "leadership", "project management", "problem solving",
    "teamwork", "analytical", "critical thinking", "time management",
    # Certifications
    "pmp", "cpa", "cissp", "aws certified", "azure certified", "six sigma",
    "scrum", "agile", "itil",
]


class JobsModule(BaseModule):
    """
    Jobs module for collecting job postings and labor market data.

    Data sources:
    - Google Jobs via SerpAPI (free tier: 100 searches/month)
    - BLS Occupational Employment and Wage Statistics
    """

    @property
    def name(self) -> str:
        return "jobs"

    @property
    def display_name(self) -> str:
        return "Job Postings & Labor Data"

    @property
    def description(self) -> str:
        return (
            "Search Google Jobs for current postings and enrich with "
            "BLS employment and wage statistics."
        )

    @property
    def input_fields(self) -> list[InputField]:
        return [
            InputField(
                name="query",
                label="Search Keywords",
                field_type="text",
                required=True,
                placeholder="e.g., data analyst, cybersecurity engineer",
                help_text="Job title or keywords to search for",
            ),
            InputField(
                name="location",
                label="Location",
                field_type="text",
                required=False,
                default="United States",
                placeholder="e.g., Phoenix, AZ or United States",
                help_text="City, state, or country",
            ),
            InputField(
                name="results_limit",
                label="Maximum Results",
                field_type="number",
                required=False,
                default=20,
                min_value=5,
                max_value=100,
                help_text="Number of job postings to retrieve (5-100)",
            ),
            InputField(
                name="employment_type",
                label="Employment Type",
                field_type="select",
                required=False,
                default="all",
                options=[
                    {"value": "all", "label": "All Types"},
                    {"value": "FULLTIME", "label": "Full-time"},
                    {"value": "PARTTIME", "label": "Part-time"},
                    {"value": "CONTRACTOR", "label": "Contractor"},
                    {"value": "INTERN", "label": "Internship"},
                ],
                is_advanced=True,
                help_text="Filter by employment type",
            ),
            InputField(
                name="date_posted",
                label="Date Posted",
                field_type="select",
                required=False,
                default="month",
                options=[
                    {"value": "today", "label": "Past 24 hours"},
                    {"value": "3days", "label": "Past 3 days"},
                    {"value": "week", "label": "Past week"},
                    {"value": "month", "label": "Past month"},
                ],
                is_advanced=True,
                help_text="Filter by posting date",
            ),
            InputField(
                name="include_bls",
                label="Include BLS Data",
                field_type="checkbox",
                required=False,
                default=True,
                help_text="Include employment and wage statistics from BLS",
                is_advanced=True,
            ),
            InputField(
                name="extract_skills",
                label="Extract Skills from Descriptions",
                field_type="checkbox",
                required=False,
                default=True,
                help_text="Parse job descriptions to extract mentioned skills",
                is_advanced=True,
            ),
        ]

    @property
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        return {
            "Jobs": [
                OutputColumn("job_title", "Job title from posting", "string"),
                OutputColumn("company", "Company name", "string"),
                OutputColumn("location", "Job location", "string"),
                OutputColumn("posted_date", "When the job was posted", "string"),
                OutputColumn("employment_type", "Full-time, part-time, etc.", "string"),
                OutputColumn("salary_min", "Minimum salary (if available)", "string"),
                OutputColumn("salary_max", "Maximum salary (if available)", "string"),
                OutputColumn("description", "Job description snippet", "string"),
                OutputColumn("skills_extracted", "Skills mentioned in posting", "string"),
                OutputColumn("source", "Job board source", "string"),
                OutputColumn("apply_url", "Link to apply", "url"),
            ],
            "BLS Data": [
                OutputColumn("soc_code", "Standard Occupational Classification code", "string"),
                OutputColumn("occupation_title", "Official occupation title", "string"),
                OutputColumn("employment", "Total employment count", "number"),
                OutputColumn("median_hourly_wage", "Median hourly wage ($)", "number"),
                OutputColumn("mean_annual_wage", "Mean annual wage ($)", "number"),
            ],
            "Skills Summary": [
                OutputColumn("skill", "Skill name", "string"),
                OutputColumn("frequency", "Number of job postings mentioning this skill", "number"),
                OutputColumn("percentage", "Percentage of postings with this skill", "number"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        # Required: query
        query = inputs.get("query", "").strip()
        if not query:
            result.add_error("query", "Search keywords are required")
        elif len(query) < 2:
            result.add_error("query", "Search keywords must be at least 2 characters")

        # Optional: results_limit
        results_limit = inputs.get("results_limit", 20)
        if not isinstance(results_limit, int) or results_limit < 5 or results_limit > 100:
            result.add_error("results_limit", "Results limit must be between 5 and 100")

        return result

    def is_available(self) -> bool:
        return settings.serpapi_available

    def get_availability_message(self) -> Optional[str]:
        if not settings.serpapi_available:
            return (
                "SerpAPI key not configured. Please add SERPAPI_KEY to your .env file. "
                "Get a free API key (100 searches/month) at https://serpapi.com"
            )
        return None

    async def execute(self, inputs: dict[str, Any]) -> ModuleResult:
        """Execute the jobs module."""
        started_at = datetime.now()
        errors = []
        warnings = []

        query = inputs.get("query", "")
        location = inputs.get("location", "United States")
        results_limit = inputs.get("results_limit", 20)
        employment_type = inputs.get("employment_type", "all")
        date_posted = inputs.get("date_posted", "month")
        include_bls = inputs.get("include_bls", True)
        extract_skills = inputs.get("extract_skills", True)

        data = {}
        all_skills = []

        # Step 1: Fetch jobs from SerpAPI
        try:
            jobs_df, job_skills = await self._fetch_google_jobs(
                query=query,
                location=location,
                limit=results_limit,
                employment_type=employment_type if employment_type != "all" else None,
                date_posted=date_posted,
                extract_skills=extract_skills,
            )
            data["Jobs"] = jobs_df
            all_skills = job_skills

            if jobs_df.empty:
                warnings.append(f"No job postings found for '{query}' in '{location}'")

        except Exception as e:
            logger.error(f"Error fetching Google Jobs: {e}")
            errors.append(f"Failed to fetch job postings: {str(e)}")
            data["Jobs"] = pd.DataFrame()

        # Step 2: Fetch BLS data (if enabled)
        if include_bls:
            try:
                bls_df = await self._fetch_bls_data(query)
                data["BLS Data"] = bls_df

                if bls_df.empty:
                    warnings.append(f"No BLS occupation data found matching '{query}'")

            except Exception as e:
                logger.error(f"Error fetching BLS data: {e}")
                warnings.append(f"BLS data unavailable: {str(e)}")
                data["BLS Data"] = pd.DataFrame()

        # Step 3: Create skills summary
        if extract_skills and all_skills:
            skills_df = self._create_skills_summary(all_skills, len(data.get("Jobs", pd.DataFrame())))
            data["Skills Summary"] = skills_df

        # Determine final status
        completed_at = datetime.now()

        if errors and not any(not df.empty for df in data.values()):
            return ModuleResult(
                status=ModuleStatus.FAILED,
                errors=errors,
                started_at=started_at,
                completed_at=completed_at,
            )
        elif errors or warnings:
            return ModuleResult(
                status=ModuleStatus.PARTIAL if errors else ModuleStatus.COMPLETED,
                data=data,
                errors=errors,
                warnings=warnings,
                metadata={
                    "query": query,
                    "location": location,
                    "jobs_found": len(data.get("Jobs", pd.DataFrame())),
                },
                started_at=started_at,
                completed_at=completed_at,
            )
        else:
            return ModuleResult.success(
                data=data,
                metadata={
                    "query": query,
                    "location": location,
                    "jobs_found": len(data.get("Jobs", pd.DataFrame())),
                },
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
    )
    async def _fetch_google_jobs(
        self,
        query: str,
        location: str,
        limit: int,
        employment_type: Optional[str],
        date_posted: str,
        extract_skills: bool,
    ) -> tuple[pd.DataFrame, list[str]]:
        """Fetch jobs from Google Jobs via SerpAPI."""
        logger.info(f"[SerpAPI] Starting job search: query='{query}', location='{location}', limit={limit}")

        # SerpAPI Google Jobs endpoint
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "api_key": settings.serpapi_key,
            "num": min(limit, 100),  # SerpAPI max is typically around 100
        }

        # Add employment type filter
        if employment_type:
            params["chips"] = f"employment_type:{employment_type}"

        # Add date filter
        date_mapping = {
            "today": "date_posted:today",
            "3days": "date_posted:3days",
            "week": "date_posted:week",
            "month": "date_posted:month",
        }
        if date_posted in date_mapping:
            if "chips" in params:
                params["chips"] += f",{date_mapping[date_posted]}"
            else:
                params["chips"] = date_mapping[date_posted]

        logger.debug(f"[SerpAPI] Request params: {params}")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get("https://serpapi.com/search", params=params)
            logger.debug(f"[SerpAPI] Response status: {response.status_code}")
            response.raise_for_status()
            data = response.json()

        # Log response structure
        logger.debug(f"[SerpAPI] Response keys: {list(data.keys())}")

        jobs = data.get("jobs_results", [])
        logger.info(f"[SerpAPI] Retrieved {len(jobs)} job postings")

        if not jobs and "error" in data:
            logger.error(f"[SerpAPI] API error: {data.get('error')}")

        if not jobs:
            return pd.DataFrame(), []

        # Parse jobs into DataFrame
        all_skills = []
        rows = []

        for job in jobs:
            # Extract salary information
            salary_min, salary_max = self._parse_salary(job)

            # Extract skills if enabled
            skills = []
            if extract_skills:
                description = job.get("description", "")
                skills = self._extract_skills(description)
                all_skills.extend(skills)

            rows.append({
                "job_title": job.get("title", ""),
                "company": job.get("company_name", ""),
                "location": job.get("location", ""),
                "posted_date": job.get("detected_extensions", {}).get("posted_at", ""),
                "employment_type": job.get("detected_extensions", {}).get("schedule_type", ""),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "description": job.get("description", "")[:1000],  # Truncate long descriptions
                "skills_extracted": ", ".join(skills) if skills else "",
                "source": job.get("via", ""),
                "apply_url": job.get("share_link", job.get("related_links", [{}])[0].get("link", "") if job.get("related_links") else ""),
            })

        df = pd.DataFrame(rows)
        return df, all_skills

    def _parse_salary(self, job: dict) -> tuple[str, str]:
        """Extract salary min/max from job data."""
        salary_min = ""
        salary_max = ""

        # Check detected extensions for salary
        extensions = job.get("detected_extensions", {})
        salary_str = extensions.get("salary", "")

        if salary_str:
            # Try to parse salary range
            # Common formats: "$50,000 - $70,000", "$25 - $35 an hour", "$60K-$80K"
            numbers = re.findall(r'\$?([\d,]+\.?\d*)\s*[kK]?', salary_str)
            if len(numbers) >= 2:
                salary_min = numbers[0].replace(",", "")
                salary_max = numbers[1].replace(",", "")
                # Handle K notation
                if "k" in salary_str.lower():
                    try:
                        salary_min = str(float(salary_min) * 1000)
                        salary_max = str(float(salary_max) * 1000)
                    except ValueError:
                        pass
            elif len(numbers) == 1:
                salary_min = numbers[0].replace(",", "")

        return salary_min, salary_max

    def _extract_skills(self, text: str) -> list[str]:
        """Extract skills mentioned in text using keyword matching."""
        if not text:
            return []

        text_lower = text.lower()
        found_skills = []

        for skill in COMMON_SKILLS:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, text_lower):
                found_skills.append(skill)

        return found_skills

    def _create_skills_summary(self, all_skills: list[str], total_jobs: int) -> pd.DataFrame:
        """Create a summary DataFrame of skill frequencies."""
        if not all_skills or total_jobs == 0:
            return pd.DataFrame()

        # Count skill occurrences
        skill_counts = {}
        for skill in all_skills:
            skill_counts[skill] = skill_counts.get(skill, 0) + 1

        # Create DataFrame sorted by frequency
        rows = []
        for skill, count in sorted(skill_counts.items(), key=lambda x: -x[1]):
            rows.append({
                "skill": skill.title(),
                "frequency": count,
                "percentage": round((count / total_jobs) * 100, 1),
            })

        return pd.DataFrame(rows)

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def _fetch_bls_data(self, query: str) -> pd.DataFrame:
        """
        Fetch BLS Occupational Employment and Wage Statistics.

        Uses the BLS public API to get occupation data.
        Series ID format: OEUN + area(7) + industry(6) + occupation(6) + datatype(2)
        - OE = Occupational Employment
        - U = Unadjusted
        - N = National
        - Datatypes: 01=employment, 04=mean annual wage, 13=median hourly wage
        """
        logger.info(f"[BLS] Starting BLS data fetch for query: '{query}'")

        # First, we need to map the query to SOC codes
        # For now, we'll use a simple approach: search the OEWS data
        # In a more complete implementation, we'd use the SOC crosswalk

        # BLS OEWS series ID format: OEUM + area + industry + occupation + datatype
        # National: area = 0000000
        # All industries: industry = 000000
        # Datatype: 01 = employment, 03 = hourly mean wage, 04 = annual mean wage
        #           13 = hourly median wage, 12 = annual median wage

        # For simplicity, we'll fetch data for common occupation groups
        # that might match the query
        soc_mappings = self._get_relevant_soc_codes(query)

        if not soc_mappings:
            logger.warning(f"No SOC codes found matching query: {query}")
            return pd.DataFrame()

        # Build series IDs for each occupation
        # Format: OEUN + area(7 zeros for national) + industry(6 zeros for all) + soc(6) + datatype(2)
        series_ids = []
        for soc_code in soc_mappings.keys():
            soc_clean = soc_code.replace("-", "")
            # OEUN = OE(survey) + U(unadjusted) + N(national)
            # Area: 0000000 (national)
            # Industry: 000000 (all industries)
            base = f"OEUN0000000000000{soc_clean}"
            series_ids.extend([
                base + "01",  # Employment
                base + "04",  # Mean annual wage
                base + "13",  # Median hourly wage
            ])
            logger.debug(f"[BLS] Built series IDs for SOC {soc_code}: {base}01, {base}04, {base}13")

        # Limit to avoid hitting API limits
        series_ids = series_ids[:30]  # Max 25-50 series per request
        logger.info(f"[BLS] Requesting {len(series_ids)} series IDs")

        # BLS API request
        api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        headers = {"Content-Type": "application/json"}

        payload = {
            "seriesid": series_ids,
            "startyear": "2023",
            "endyear": "2024",
        }

        # Add API key if available (increases rate limit)
        if settings.bls_api_key:
            payload["registrationkey"] = settings.bls_api_key
            logger.debug("[BLS] Using registered API key")

        logger.debug(f"[BLS] Making request to {api_url}")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(api_url, json=payload, headers=headers)
            logger.debug(f"[BLS] Response status: {response.status_code}")
            response.raise_for_status()
            data = response.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            logger.error(f"[BLS] API error: {data.get('message', 'Unknown error')}")
            return pd.DataFrame()

        logger.info(f"[BLS] API request succeeded, parsing {len(data.get('Results', {}).get('series', []))} series")

        # Parse the results
        results = {}
        for series in data.get("Results", {}).get("series", []):
            series_id = series.get("seriesID", "")
            series_data = series.get("data", [])

            if not series_data:
                logger.debug(f"[BLS] No data for series {series_id}")
                continue

            # Get the most recent value
            latest = series_data[0]
            value = latest.get("value", "")
            logger.debug(f"[BLS] Series {series_id}: value={value}")

            # Parse series ID to extract SOC code and data type
            # Format: OEUN (4) + area (7) + industry (6) + SOC (6) + datatype (2) = 25 chars
            if len(series_id) >= 25:
                soc_raw = series_id[17:23]  # Extract 6-char SOC code
                soc_code = f"{soc_raw[:2]}-{soc_raw[2:]}"  # Format as XX-XXXX
                datatype = series_id[23:25]  # Last 2 chars

                if soc_code not in results:
                    results[soc_code] = {
                        "soc_code": soc_code,
                        "occupation_title": soc_mappings.get(soc_code, ""),
                    }

                if datatype == "01":
                    results[soc_code]["employment"] = value
                elif datatype == "04":  # Mean annual wage
                    results[soc_code]["mean_annual_wage"] = value
                elif datatype == "13":  # Median hourly wage
                    results[soc_code]["median_hourly_wage"] = value
            else:
                logger.warning(f"[BLS] Unexpected series ID format: {series_id}")

        if not results:
            logger.warning("[BLS] No results parsed from API response")
            return pd.DataFrame()

        # Convert to DataFrame
        rows = list(results.values())
        df = pd.DataFrame(rows)
        logger.info(f"[BLS] Created DataFrame with {len(df)} occupations")

        # Ensure all expected columns exist
        for col in ["employment", "median_hourly_wage", "mean_annual_wage"]:
            if col not in df.columns:
                df[col] = ""

        # Log the data we found
        for _, row in df.iterrows():
            logger.debug(f"[BLS] {row.get('occupation_title', 'Unknown')}: "
                        f"employment={row.get('employment', 'N/A')}, "
                        f"wage=${row.get('mean_annual_wage', 'N/A')}")

        return df

    def _get_relevant_soc_codes(self, query: str) -> dict[str, str]:
        """
        Map a search query to relevant SOC codes.

        This is a simplified mapping. In production, you'd use
        a more sophisticated matching algorithm or the full SOC crosswalk.
        """
        query_lower = query.lower()

        # Common SOC code mappings for popular job searches
        soc_database = {
            # Computer and Mathematical
            "15-1252": "Software Developers",
            "15-1251": "Computer Programmers",
            "15-1211": "Computer Systems Analysts",
            "15-1212": "Information Security Analysts",
            "15-2051": "Data Scientists",
            "15-1241": "Computer Network Architects",
            "15-1244": "Network and Computer Systems Administrators",
            "15-1232": "Computer User Support Specialists",
            "15-1299": "Computer Occupations, All Other",

            # Business and Financial
            "13-2011": "Accountants and Auditors",
            "13-1111": "Management Analysts",
            "13-2051": "Financial Analysts",
            "13-1161": "Market Research Analysts",
            "13-2072": "Loan Officers",
            "11-3031": "Financial Managers",

            # Healthcare
            "29-1141": "Registered Nurses",
            "29-1071": "Physician Assistants",
            "29-2061": "Licensed Practical Nurses",
            "31-1120": "Home Health and Personal Care Aides",

            # Management
            "11-1021": "General and Operations Managers",
            "11-2021": "Marketing Managers",
            "11-3021": "Computer and Information Systems Managers",
            "11-9111": "Medical and Health Services Managers",

            # Sales
            "41-3091": "Sales Representatives, Services",
            "41-4012": "Sales Representatives, Wholesale",

            # Education
            "25-1099": "Postsecondary Teachers",
            "25-2031": "Secondary School Teachers",
        }

        # Find matching occupations based on query keywords
        matches = {}
        keywords = query_lower.split()

        for soc_code, title in soc_database.items():
            title_lower = title.lower()
            # Check if any keyword matches the title
            if any(kw in title_lower for kw in keywords):
                matches[soc_code] = title
            # Also check common keyword mappings
            elif "data" in query_lower and ("data" in title_lower or "analyst" in title_lower):
                matches[soc_code] = title
            elif "software" in query_lower and "software" in title_lower:
                matches[soc_code] = title
            elif "security" in query_lower and "security" in title_lower:
                matches[soc_code] = title
            elif "cyber" in query_lower and "security" in title_lower:
                matches[soc_code] = title
            elif "nurse" in query_lower and "nurse" in title_lower:
                matches[soc_code] = title
            elif "manager" in query_lower and "manager" in title_lower:
                matches[soc_code] = title

        # Limit to top 5 matches to stay within API limits
        return dict(list(matches.items())[:5])

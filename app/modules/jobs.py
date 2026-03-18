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
    # Programming Languages
    "python", "java", "javascript", "typescript", "sql", "nosql", "r",
    "c\\+\\+", "c#", "go", "rust", "scala", "ruby", "swift", "kotlin", "php",
    # Cloud & Infrastructure
    "aws", "azure", "gcp", "docker", "kubernetes", "terraform", "ansible",
    "ci/cd", "devops", "linux",
    # Frameworks & Libraries
    "react", "angular", "vue", "node.js", "django", "flask", "spring",
    "tensorflow", "pytorch", "spark", "hadoop",
    # AI/ML & Data
    "machine learning", "deep learning", "natural language processing",
    "computer vision", "large language models", "generative ai",
    "artificial intelligence", "data science", "data analysis",
    "data engineering", "data visualization", "statistics",
    "neural networks", "reinforcement learning",
    # Tools & Platforms
    "excel", "tableau", "power bi", "salesforce", "sap", "oracle",
    "git", "jira", "confluence",
    # Soft skills
    "communication", "leadership", "project management", "problem solving",
    "teamwork", "analytical", "critical thinking", "time management",
    "strategic planning", "stakeholder management",
    # Certifications & Methodologies
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
                    {"value": "month", "label": "Past month (Recommended)"},
                    {"value": "week", "label": "Past week"},
                    {"value": "3days", "label": "Past 3 days"},
                    {"value": "today", "label": "Past 24 hours"},
                ],
                is_advanced=True,
                help_text="Filter by posting date (Past month recommended for better results)",
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
                OutputColumn("median_annual_wage", "Median annual wage ($)", "number"),
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
        elif len(query) > 200:
            result.add_error("query", "Search keywords must be less than 200 characters")

        # Optional but validate if provided: location
        location = inputs.get("location", "").strip()
        if location and len(location) < 2:
            result.add_error("location", "Location must be at least 2 characters if provided")
        elif location and len(location) > 100:
            result.add_error("location", "Location must be less than 100 characters")

        # Optional: results_limit
        results_limit = inputs.get("results_limit", 20)
        if not isinstance(results_limit, int):
            result.add_error("results_limit", "Results limit must be a number")
        elif results_limit < 5 or results_limit > 100:
            result.add_error("results_limit", "Results limit must be between 5 and 100")

        # Validate employment_type
        employment_type = inputs.get("employment_type", "all")
        valid_types = ["all", "FULLTIME", "PARTTIME", "CONTRACTOR", "INTERN"]
        if employment_type not in valid_types:
            result.add_error("employment_type", f"Employment type must be one of: {', '.join(valid_types)}")

        # Validate date_posted
        date_posted = inputs.get("date_posted", "month")
        valid_dates = ["today", "3days", "week", "month"]
        if date_posted not in valid_dates:
            result.add_error("date_posted", f"Date posted must be one of: {', '.join(valid_dates)}")

        # Validate boolean fields
        include_bls = inputs.get("include_bls", True)
        if not isinstance(include_bls, bool):
            result.add_error("include_bls", "Include BLS must be a checkbox value (true/false)")

        extract_skills = inputs.get("extract_skills", True)
        if not isinstance(extract_skills, bool):
            result.add_error("extract_skills", "Extract skills must be a checkbox value (true/false)")

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
        """Fetch jobs from Google Jobs via SerpAPI with pagination."""
        logger.info(f"[SerpAPI] Starting job search: query='{query}', location='{location}', limit={limit}")

        all_jobs = []
        start = 0

        while len(all_jobs) < limit:
            # SerpAPI Google Jobs endpoint
            # Note: google_jobs does NOT support the 'num' parameter.
            # It returns ~10 results per page. Use 'start' for pagination.
            params = {
                "engine": "google_jobs",
                "q": query,
                "location": location,
                "api_key": settings.serpapi_key,
            }

            if start > 0:
                params["start"] = start

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

            logger.debug(f"[SerpAPI] Request params (start={start}): {params}")
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get("https://serpapi.com/search", params=params)
                logger.debug(f"[SerpAPI] Response status: {response.status_code}")
                response.raise_for_status()
                data = response.json()

            jobs = data.get("jobs_results", [])
            logger.info(f"[SerpAPI] Retrieved {len(jobs)} job postings (page start={start})")

            if not jobs:
                if "error" in data:
                    logger.warning(f"[SerpAPI] API message: {data.get('error')}")
                break

            all_jobs.extend(jobs)
            start += 10  # Google Jobs returns ~10 per page

            # Stop if we got fewer than a full page (no more results)
            if len(jobs) < 10:
                break

        logger.info(f"[SerpAPI] Total jobs retrieved: {len(all_jobs)}")

        if not all_jobs:
            return pd.DataFrame(), []

        # Limit to requested amount
        all_jobs = all_jobs[:limit]

        # Parse jobs into DataFrame
        all_skills = []
        rows = []

        for job in all_jobs:
            # Extract salary information
            salary_min, salary_max = self._parse_salary(job)

            # Extract skills if enabled
            skills = []
            if extract_skills:
                description = job.get("description", "")
                skills = self._extract_skills(description)
                all_skills.extend(skills)

            # Clean up the source field (remove "via " prefix)
            source = job.get("via", "")
            if source.lower().startswith("via "):
                source = source[4:]

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
                "source": source,
                "apply_url": job.get("share_link", ""),
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

        if not salary_str:
            # Also check job_highlights for salary info
            highlights = job.get("job_highlights", [])
            for highlight in highlights:
                items = highlight.get("items", [])
                for item in items:
                    if "$" in item and any(w in item.lower() for w in ["salary", "pay", "compensation", "wage"]):
                        salary_str = item
                        break
                if salary_str:
                    break

        if salary_str:
            # Try to parse salary range
            # Common formats: "$50,000 - $70,000", "$25 - $35 an hour", "$60K-$80K"
            # Match dollar amounts with optional K/k suffix
            numbers = re.findall(r'\$\s*([\d,]+\.?\d*)\s*([kK])?', salary_str)
            if len(numbers) >= 2:
                val1 = numbers[0][0].replace(",", "")
                val2 = numbers[1][0].replace(",", "")
                # Handle K notation per-number
                try:
                    salary_min = str(float(val1) * 1000) if numbers[0][1] else val1
                    salary_max = str(float(val2) * 1000) if numbers[1][1] else val2
                except ValueError:
                    salary_min = val1
                    salary_max = val2

                # If these look like hourly wages (< $500), convert to annual estimates
                try:
                    min_val = float(salary_min)
                    max_val = float(salary_max)
                    if "hour" in salary_str.lower() or (min_val < 500 and max_val < 500):
                        salary_min = str(int(min_val * 2080))  # 40hr/week * 52 weeks
                        salary_max = str(int(max_val * 2080))
                except ValueError:
                    pass

            elif len(numbers) == 1:
                val = numbers[0][0].replace(",", "")
                try:
                    salary_min = str(float(val) * 1000) if numbers[0][1] else val
                except ValueError:
                    salary_min = val

        # Format for display
        try:
            if salary_min:
                salary_min = f"${int(float(salary_min)):,}"
            if salary_max:
                salary_max = f"${int(float(salary_max)):,}"
        except (ValueError, TypeError):
            pass

        return salary_min, salary_max

    def _extract_skills(self, text: str) -> list[str]:
        """Extract skills mentioned in text using keyword matching."""
        if not text:
            return []

        text_lower = text.lower()
        found_skills = []

        for skill in COMMON_SKILLS:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + skill + r'\b'
            if re.search(pattern, text_lower):
                # Normalize the display name (remove regex escapes)
                display_name = skill.replace("\\+", "+").replace("\\#", "#")
                found_skills.append(display_name)

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
        - OE = Occupational Employment survey
        - U = Unadjusted
        - N = National
        - Datatypes: 01=employment, 04=mean annual wage, 08=median hourly wage, 13=annual median wage
        """
        logger.info(f"[BLS] Starting BLS data fetch for query: '{query}'")

        soc_mappings = self._get_relevant_soc_codes(query)

        if not soc_mappings:
            logger.warning(f"No SOC codes found matching query: {query}")
            return pd.DataFrame()

        # Build series IDs for each occupation
        # Format: OEUN + area(7 zeros for national) + industry(6 zeros for all) + soc(6) + datatype(2)
        series_ids = []
        for soc_code in soc_mappings.keys():
            soc_clean = soc_code.replace("-", "")
            base = f"OEUN0000000000000{soc_clean}"
            series_ids.extend([
                base + "01",  # Employment
                base + "04",  # Mean annual wage
                base + "08",  # Median hourly wage (FIXED: was 13 which is annual median)
                base + "13",  # Annual median wage
            ])
            logger.debug(f"[BLS] Built series IDs for SOC {soc_code}: {base}01, {base}04, {base}08, {base}13")

        # Limit to avoid hitting API limits (max 50 series per request)
        series_ids = series_ids[:48]
        logger.info(f"[BLS] Requesting {len(series_ids)} series IDs")

        # Use current year dynamically
        current_year = datetime.now().year
        start_year = str(current_year - 2)
        end_year = str(current_year)

        # BLS API request
        api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        headers = {"Content-Type": "application/json"}

        payload = {
            "seriesid": series_ids,
            "startyear": start_year,
            "endyear": end_year,
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
                soc_raw = series_id[17:23]
                soc_code = f"{soc_raw[:2]}-{soc_raw[2:]}"
                datatype = series_id[23:25]

                if soc_code not in results:
                    results[soc_code] = {
                        "soc_code": soc_code,
                        "occupation_title": soc_mappings.get(soc_code, ""),
                    }

                if datatype == "01":
                    results[soc_code]["employment"] = value
                elif datatype == "04":
                    results[soc_code]["mean_annual_wage"] = value
                elif datatype == "08":
                    results[soc_code]["median_hourly_wage"] = value
                elif datatype == "13":
                    results[soc_code]["median_annual_wage"] = value
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
        for col in ["employment", "median_hourly_wage", "mean_annual_wage", "median_annual_wage"]:
            if col not in df.columns:
                df[col] = ""

        # Format data for stakeholder readability
        if not df.empty:
            # Format employment with commas
            if "employment" in df.columns:
                df["employment"] = df["employment"].apply(
                    lambda x: f"{int(float(x)):,}" if x and x != "" else "N/A"
                )

            # Format mean annual wage
            if "mean_annual_wage" in df.columns:
                df["mean_annual_wage"] = df["mean_annual_wage"].apply(
                    lambda x: f"${int(float(x)):,}" if x and x != "" else "N/A"
                )

            # Format median hourly wage (data type 08 - actual hourly rate)
            if "median_hourly_wage" in df.columns:
                df["median_hourly_wage"] = df["median_hourly_wage"].apply(
                    lambda x: f"${float(x):,.2f}/hr" if x and x != "" else "N/A"
                )

            # Format median annual wage
            if "median_annual_wage" in df.columns:
                df["median_annual_wage"] = df["median_annual_wage"].apply(
                    lambda x: f"${int(float(x)):,}" if x and x != "" else "N/A"
                )

            # Rename columns for clarity
            df = df.rename(columns={
                "soc_code": "SOC Code",
                "occupation_title": "Occupation Title",
                "employment": "Total Employment (US)",
                "mean_annual_wage": "Mean Annual Salary",
                "median_hourly_wage": "Median Hourly Wage",
                "median_annual_wage": "Median Annual Salary",
            })

        # Log the data we found
        for _, row in df.iterrows():
            logger.debug(f"[BLS] {row.get('Occupation Title', 'Unknown')}: "
                        f"employment={row.get('Total Employment (US)', 'N/A')}, "
                        f"wage={row.get('Mean Annual Salary', 'N/A')}")

        return df

    def _get_relevant_soc_codes(self, query: str) -> dict[str, str]:
        """
        Map a search query to relevant SOC codes using word-boundary matching
        and semantic keyword mapping.
        """
        query_lower = query.lower()
        query_words = set(re.findall(r'\b\w+\b', query_lower))

        # Comprehensive SOC code database
        soc_database = {
            # Computer and Mathematical
            "15-1252": "Software Developers",
            "15-1251": "Computer Programmers",
            "15-1211": "Computer Systems Analysts",
            "15-1212": "Information Security Analysts",
            "15-2051": "Data Scientists",
            "15-2041": "Statisticians",
            "15-1241": "Computer Network Architects",
            "15-1244": "Network and Computer Systems Administrators",
            "15-1232": "Computer User Support Specialists",
            "15-1231": "Computer Network Support Specialists",
            "15-1245": "Database Administrators",
            "15-1256": "Software Quality Assurance Analysts",
            "15-1255": "Web and Digital Interface Designers",
            "15-1299": "Computer Occupations, All Other",
            "15-2031": "Operations Research Analysts",

            # Business and Financial
            "13-2011": "Accountants and Auditors",
            "13-1111": "Management Analysts",
            "13-2051": "Financial Analysts",
            "13-1161": "Market Research Analysts",
            "13-2072": "Loan Officers",
            "13-1071": "Human Resources Specialists",
            "13-1081": "Logisticians",
            "11-3031": "Financial Managers",

            # Healthcare
            "29-1141": "Registered Nurses",
            "29-1071": "Physician Assistants",
            "29-2061": "Licensed Practical Nurses",
            "29-1171": "Nurse Practitioners",
            "29-2010": "Clinical Laboratory Technologists and Technicians",
            "31-1120": "Home Health and Personal Care Aides",
            "29-1228": "Physicians, All Other",

            # Management
            "11-1021": "General and Operations Managers",
            "11-2021": "Marketing Managers",
            "11-3021": "Computer and Information Systems Managers",
            "11-9111": "Medical and Health Services Managers",
            "11-2022": "Sales Managers",
            "11-3111": "Compensation and Benefits Managers",
            "11-9041": "Architectural and Engineering Managers",
            "11-9121": "Natural Sciences Managers",

            # Engineering
            "17-2199": "Engineers, All Other",
            "17-2061": "Computer Hardware Engineers",
            "17-2071": "Electrical Engineers",
            "17-2112": "Industrial Engineers",
            "17-2141": "Mechanical Engineers",

            # Sales
            "41-3091": "Sales Representatives, Services",
            "41-4012": "Sales Representatives, Wholesale",

            # Education
            "25-1099": "Postsecondary Teachers",
            "25-2031": "Secondary School Teachers",
            "25-1021": "Computer Science Teachers, Postsecondary",
            "25-9031": "Instructional Coordinators",

            # Arts, Design, Media
            "27-1024": "Graphic Designers",
            "27-3042": "Technical Writers",
            "15-1257": "Web Developers",
        }

        # Semantic keyword mappings: query terms → relevant SOC codes
        # This replaces the buggy substring matching
        keyword_to_socs = {
            # AI / Machine Learning / Data Science
            "ai": ["15-2051", "15-1252", "15-1299", "15-2031", "11-3021"],
            "artificial intelligence": ["15-2051", "15-1252", "15-1299", "15-2031", "11-3021"],
            "machine learning": ["15-2051", "15-1252", "15-1299", "15-2031"],
            "deep learning": ["15-2051", "15-1252", "15-1299"],
            "data science": ["15-2051", "15-2041", "15-2031", "15-1299"],
            "data scientist": ["15-2051", "15-2041", "15-2031"],
            "data analyst": ["15-2051", "15-2031", "13-1161"],
            "data engineer": ["15-2051", "15-1252", "15-1245"],
            "data": ["15-2051", "15-2031", "15-1245", "13-1161"],
            "applied ai": ["15-2051", "15-1252", "15-1299", "15-2031", "11-3021"],
            "nlp": ["15-2051", "15-1252", "15-1299"],
            "computer vision": ["15-2051", "15-1252", "15-1299"],
            "generative ai": ["15-2051", "15-1252", "15-1299", "11-3021"],

            # Software Engineering
            "software": ["15-1252", "15-1256", "15-1299"],
            "software engineer": ["15-1252", "15-1256"],
            "developer": ["15-1252", "15-1257", "15-1251"],
            "web developer": ["15-1257", "15-1255"],
            "full stack": ["15-1252", "15-1257"],
            "backend": ["15-1252", "15-1251"],
            "frontend": ["15-1257", "15-1255"],
            "devops": ["15-1244", "15-1252"],
            "cloud": ["15-1244", "15-1241", "15-1252"],
            "programmer": ["15-1251", "15-1252"],

            # Security
            "security": ["15-1212", "11-3021"],
            "cybersecurity": ["15-1212", "15-1244", "11-3021"],
            "cyber": ["15-1212", "15-1244"],
            "infosec": ["15-1212"],
            "information security": ["15-1212", "11-3021"],

            # Business & Finance
            "business": ["13-1111", "11-1021"],
            "consultant": ["13-1111"],
            "analyst": ["13-1161", "15-2031", "13-2051"],
            "finance": ["13-2051", "11-3031", "13-2072"],
            "financial": ["13-2051", "11-3031"],
            "accounting": ["13-2011"],
            "accountant": ["13-2011"],
            "marketing": ["11-2021", "13-1161"],
            "project manager": ["13-1111", "11-1021"],
            "product manager": ["11-1021", "13-1111"],
            "hr": ["13-1071"],
            "human resources": ["13-1071", "11-3111"],

            # Healthcare
            "nurse": ["29-1141", "29-1171", "29-2061"],
            "nursing": ["29-1141", "29-1171", "29-2061"],
            "healthcare": ["29-1141", "11-9111", "31-1120"],
            "medical": ["29-1228", "11-9111", "29-2010"],
            "physician": ["29-1228", "29-1071"],
            "health": ["11-9111", "29-1141"],

            # Management
            "manager": ["11-1021", "11-2021", "11-3021"],
            "management": ["13-1111", "11-1021"],
            "director": ["11-1021", "11-3021"],
            "executive": ["11-1021"],
            "cto": ["11-3021"],
            "cio": ["11-3021"],

            # Engineering
            "engineer": ["15-1252", "17-2199", "17-2061"],
            "engineering": ["17-2199", "11-9041"],
            "electrical": ["17-2071"],
            "mechanical": ["17-2141"],
            "industrial": ["17-2112"],
            "hardware": ["17-2061"],

            # Education
            "teacher": ["25-2031", "25-1099"],
            "professor": ["25-1099", "25-1021"],
            "education": ["25-1099", "25-9031"],
            "instructor": ["25-1099", "25-9031"],

            # Design
            "designer": ["27-1024", "15-1255"],
            "ux": ["15-1255", "27-1024"],
            "ui": ["15-1255", "27-1024"],
        }

        # Score each SOC code based on how well it matches the query
        soc_scores: dict[str, float] = {}

        # Method 1: Check semantic keyword mappings (highest priority)
        # Try multi-word phrases first, then individual words
        for phrase_len in range(len(query_words), 0, -1):
            # Try the full query as a phrase
            if phrase_len == len(query_words):
                phrase = query_lower
                if phrase in keyword_to_socs:
                    for i, soc in enumerate(keyword_to_socs[phrase]):
                        # Higher score for earlier entries (more relevant)
                        score = 10.0 - (i * 0.5)
                        soc_scores[soc] = max(soc_scores.get(soc, 0), score)

        # Try individual words
        for word in query_words:
            if word in keyword_to_socs:
                for i, soc in enumerate(keyword_to_socs[word]):
                    score = 5.0 - (i * 0.3)
                    soc_scores[soc] = max(soc_scores.get(soc, 0), score)

        # Method 2: Direct word-boundary match against SOC titles (lower priority)
        for soc_code, title in soc_database.items():
            title_words = set(re.findall(r'\b\w+\b', title.lower()))
            # Require full word match (not substring!)
            matching_words = query_words & title_words
            # Filter out very common/short words that cause false matches
            meaningful_matches = {w for w in matching_words if len(w) >= 3 and w not in {"and", "the", "all", "for", "other"}}

            if meaningful_matches:
                # Score based on proportion of query words that match
                score = len(meaningful_matches) / len(query_words) * 3.0
                soc_scores[soc_code] = max(soc_scores.get(soc_code, 0), score)

        # Sort by score and take top 5
        sorted_socs = sorted(soc_scores.items(), key=lambda x: -x[1])
        top_socs = sorted_socs[:5]

        matches = {}
        for soc_code, score in top_socs:
            if soc_code in soc_database:
                matches[soc_code] = soc_database[soc_code]
                logger.debug(f"[BLS] Matched SOC {soc_code} ({soc_database[soc_code]}) with score {score:.1f}")

        if not matches:
            logger.warning(f"[BLS] No SOC codes matched for query: '{query}'")
        else:
            logger.info(f"[BLS] Found {len(matches)} matching SOC codes for '{query}': {list(matches.values())}")

        return matches

"""
Google Trends Module - Skills Demand Tracking

Tracks search interest trends for skills and topics.
Can auto-populate skills from the Jobs module output.

Uses SerpAPI Google Trends (preferred) or pytrends as fallback.
SerpAPI is more reliable as it handles Google's rate limiting.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Optional
import pandas as pd
import httpx

# Fix urllib3 2.x compatibility with pytrends (fallback)
# The pytrends library uses 'method_whitelist' which was renamed to 'allowed_methods' in urllib3 2.0
import urllib3.util.retry as urllib3_retry
_original_retry_init = urllib3_retry.Retry.__init__

def _patched_retry_init(self, *args, **kwargs):
    # Rename method_whitelist to allowed_methods for urllib3 2.x compatibility
    if 'method_whitelist' in kwargs:
        kwargs['allowed_methods'] = kwargs.pop('method_whitelist')
    return _original_retry_init(self, *args, **kwargs)

urllib3_retry.Retry.__init__ = _patched_retry_init

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError, TooManyRequestsError

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

# Thread pool for pytrends (synchronous library, used as fallback)
executor = ThreadPoolExecutor(max_workers=1)


class TrendsModule(BaseModule):
    """
    Google Trends module for tracking skills demand.

    Uses pytrends to query Google Trends data.
    Can auto-populate search terms from Jobs module output.
    """

    @property
    def name(self) -> str:
        return "trends"

    @property
    def display_name(self) -> str:
        return "Google Trends (Skills)"

    @property
    def description(self) -> str:
        return (
            "Track Google search interest for skills and topics over time. "
            "Can use skills extracted from job postings."
        )

    @property
    def input_fields(self) -> list[InputField]:
        return [
            InputField(
                name="terms",
                label="Search Terms",
                field_type="text",
                required=True,  # Required - user must provide terms
                placeholder="e.g., python, machine learning, data science",
                help_text="Comma-separated list of search terms to track (max 5)",
            ),
            InputField(
                name="max_terms",
                label="Max Terms to Track",
                field_type="number",
                required=False,
                default=5,
                min_value=1,
                max_value=5,
                help_text="Maximum number of terms to compare (1-5)",
            ),
            InputField(
                name="timeframe",
                label="Time Period",
                field_type="select",
                required=False,
                default="today 12-m",
                options=[
                    {"value": "today 1-m", "label": "Past month"},
                    {"value": "today 3-m", "label": "Past 3 months"},
                    {"value": "today 12-m", "label": "Past 12 months"},
                    {"value": "today 5-y", "label": "Past 5 years"},
                ],
                help_text="Time period for trend data",
            ),
            InputField(
                name="geo",
                label="Geographic Region",
                field_type="select",
                required=False,
                default="US",
                options=[
                    {"value": "US", "label": "United States"},
                    {"value": "", "label": "Worldwide"},
                    {"value": "GB", "label": "United Kingdom"},
                    {"value": "CA", "label": "Canada"},
                    {"value": "AU", "label": "Australia"},
                ],
                is_advanced=True,
                help_text="Geographic region for trend data",
            ),
            InputField(
                name="include_related",
                label="Include Related Queries",
                field_type="checkbox",
                required=False,
                default=True,
                is_advanced=True,
                help_text="Include related search queries and topics",
            ),
        ]

    @property
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        return {
            "Trends Summary": [
                OutputColumn("term", "Search term", "string"),
                OutputColumn("avg_interest", "Average interest", "number"),
                OutputColumn("peak_interest", "Peak interest", "number"),
                OutputColumn("current_interest", "Most recent interest", "number"),
                OutputColumn("trend_direction", "Trending direction", "string"),
            ],
            # Individual trend data sheets created dynamically per term
            "Trend Over Time": [
                OutputColumn("date", "Date", "date"),
                OutputColumn("interest", "Search interest (0-100)", "number"),
            ],
            "Related Queries": [
                OutputColumn("term", "Original term", "string"),
                OutputColumn("related_query", "Related search query", "string"),
                OutputColumn("query_type", "Top or Rising", "string"),
                OutputColumn("relevance", "Relevance score/growth", "string"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        terms = inputs.get("terms", "").strip()

        # Terms are required
        if not terms:
            result.add_error("terms", "Search terms are required")
            return result

        # Validate term count
        term_list = [t.strip() for t in terms.split(",") if t.strip()]
        if len(term_list) == 0:
            result.add_error("terms", "At least one search term is required")
        elif len(term_list) > 5:
            result.add_error("terms", "Maximum 5 terms can be compared at once")

        return result

    def is_available(self) -> bool:
        return True  # pytrends is always available

    async def execute(
        self,
        inputs: dict[str, Any],
        job_skills: Optional[list[str]] = None,  # Skills from Jobs module
    ) -> ModuleResult:
        """Execute the trends module."""
        started_at = datetime.now()
        errors = []
        warnings = []

        # Get search terms
        terms_input = inputs.get("terms", "").strip()
        max_terms = inputs.get("max_terms", 5)
        timeframe = inputs.get("timeframe", "today 12-m")
        geo = inputs.get("geo", "US")
        include_related = inputs.get("include_related", True)

        # Parse and validate terms
        terms = [t.strip() for t in terms_input.split(",") if t.strip()]
        if not terms:
            errors.append("No valid search terms provided")
            return ModuleResult.failure(errors)

        # Limit to max_terms
        terms = terms[:max_terms]
        logger.info(f"[Trends] Tracking {len(terms)} terms: {terms}")

        # Fetch trends data
        try:
            trend_data, trend_summary, related_queries = await self._fetch_trends(
                terms=terms,
                timeframe=timeframe,
                geo=geo,
                include_related=include_related,
            )
        except Exception as e:
            logger.error(f"Error fetching Google Trends: {e}")
            errors.append(f"Failed to fetch Google Trends data: {str(e)}")
            return ModuleResult.failure(errors)

        # Assemble output data - create separate sheets for each term
        data = {}

        # Add summary first
        if not trend_summary.empty:
            data["Trends Summary"] = trend_summary

        # Create separate sheet for each term's time series data (better for charts)
        if not trend_data.empty:
            term_list = [t.strip() for t in terms.split(",") if t.strip()]
            for term in term_list:
                term_df = trend_data[trend_data["term"] == term].copy()
                if not term_df.empty:
                    # Drop the term column since it's redundant (sheet name has it)
                    term_df = term_df[["date", "interest"]]
                    sheet_name = f"Trend - {term.title()}"
                    data[sheet_name] = term_df

        # Add related queries if available
        if include_related and not related_queries.empty:
            data["Related Queries"] = related_queries

        completed_at = datetime.now()

        if warnings:
            return ModuleResult(
                status=ModuleStatus.COMPLETED,
                data=data,
                warnings=warnings,
                metadata={
                    "terms": terms,
                    "timeframe": timeframe,
                    "geo": geo,
                    "terms_analyzed": len(terms.split(",")),
                },
                started_at=started_at,
                completed_at=completed_at,
            )

        return ModuleResult.success(
            data=data,
            metadata={
                "terms": terms,
                "timeframe": timeframe,
                "geo": geo,
                "terms_analyzed": len(terms.split(",")),
            },
        )

    async def _fetch_trends(
        self,
        terms: list[str],
        timeframe: str,
        geo: str,
        include_related: bool,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Fetch trends data from Google Trends.

        Uses SerpAPI if available (more reliable), falls back to pytrends.
        """
        # Prefer SerpAPI if available (more reliable, handles rate limiting)
        if settings.serpapi_available:
            logger.info("[Trends] Using SerpAPI for Google Trends (more reliable)")
            try:
                return await self._fetch_trends_serpapi(terms, timeframe, geo)
            except Exception as e:
                logger.warning(f"[Trends] SerpAPI failed, falling back to pytrends: {e}")

        # Fallback to pytrends (may hit rate limits)
        logger.info("[Trends] Using pytrends for Google Trends")
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            executor,
            self._fetch_trends_sync,
            terms,
            timeframe,
            geo,
            include_related,
        )

    async def _fetch_trends_serpapi(
        self,
        terms: list[str],
        timeframe: str,
        geo: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Fetch trends data using SerpAPI (more reliable than pytrends)."""
        logger.info(f"[Trends/SerpAPI] Fetching trends for: {terms}")

        # Map timeframe to SerpAPI date parameter
        date_map = {
            "today 1-m": "today 1-m",
            "today 3-m": "today 3-m",
            "today 12-m": "today 12-m",
            "today 5-y": "today 5-y",
        }
        date_param = date_map.get(timeframe, "today 12-m")

        params = {
            "engine": "google_trends",
            "q": ",".join(terms),
            "data_type": "TIMESERIES",
            "date": date_param,
            "api_key": settings.serpapi_key,
        }

        if geo:
            params["geo"] = geo

        logger.debug(f"[Trends/SerpAPI] Request params: {params}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get("https://serpapi.com/search", params=params)
            logger.debug(f"[Trends/SerpAPI] Response status: {response.status_code}")
            response.raise_for_status()
            data = response.json()

        # Parse interest over time
        interest_data = data.get("interest_over_time", {})
        timeline_data = interest_data.get("timeline_data", [])

        logger.info(f"[Trends/SerpAPI] Retrieved {len(timeline_data)} data points")

        if not timeline_data:
            logger.warning("[Trends/SerpAPI] No timeline data returned")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Convert to our format - structure by term for better visualization
        trend_rows = []
        for point in timeline_data:
            date = point.get("date", "")

            for value_info in point.get("values", []):
                term = value_info.get("query", "")
                interest = value_info.get("extracted_value", 0)
                trend_rows.append({
                    "date": date,
                    "term": term,
                    "interest": interest,
                })

        trend_data = pd.DataFrame(trend_rows)

        # Create summary statistics
        summary_rows = []
        for term in terms:
            term_data = trend_data[trend_data["term"] == term]
            if not term_data.empty:
                interests = term_data["interest"].astype(float)
                current = int(interests.iloc[-1]) if len(interests) > 0 else 0
                avg = round(interests.mean(), 1)

                # Determine trend direction
                if len(interests) >= 8:
                    recent_avg = interests.tail(4).mean()
                    older_avg = interests.head(4).mean()
                    if recent_avg > older_avg * 1.1:
                        direction = "Rising"
                    elif recent_avg < older_avg * 0.9:
                        direction = "Declining"
                    else:
                        direction = "Stable"
                else:
                    direction = "Unknown"

                summary_rows.append({
                    "term": term,
                    "avg_interest": avg,
                    "peak_interest": int(interests.max()),
                    "current_interest": current,
                    "trend_direction": direction,
                })

        trend_summary = pd.DataFrame(summary_rows)

        # SerpAPI doesn't provide related queries in the same call
        # Would need separate API calls for related queries
        related_df = pd.DataFrame()

        logger.info(f"[Trends/SerpAPI] Created summary for {len(summary_rows)} terms")
        return trend_data, trend_summary, related_df

    def _fetch_trends_sync(
        self,
        terms: list[str],
        timeframe: str,
        geo: str,
        include_related: bool,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Synchronous Google Trends fetching."""
        logger.info(f"[Trends] Starting Google Trends fetch for: {terms}")
        logger.info(f"[Trends] Parameters: timeframe={timeframe}, geo={geo}")

        # Initialize pytrends with retry settings
        logger.debug("[Trends] Initializing pytrends TrendReq")
        pytrends = TrendReq(
            hl="en-US",
            tz=360,
            timeout=(10, 25),
            retries=2,
            backoff_factor=1.0,
        )

        # Build payload with rate limit handling
        max_retries = 3
        retry_count = 0
        base_wait_time = 10  # Start with 10 seconds

        while retry_count < max_retries:
            try:
                logger.debug(f"[Trends] Building payload (attempt {retry_count + 1}/{max_retries})")
                pytrends.build_payload(
                    kw_list=terms,
                    timeframe=timeframe,
                    geo=geo,
                )
                logger.debug("[Trends] Payload built successfully")
                break
            except TooManyRequestsError as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = base_wait_time * (2 ** retry_count)  # Exponential backoff
                    logger.warning(f"[Trends] Rate limited (429). Waiting {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[Trends] Rate limit exceeded after {max_retries} attempts: {e}")
                    raise
            except ResponseError as e:
                retry_count += 1
                if "429" in str(e) and retry_count < max_retries:
                    wait_time = base_wait_time * (2 ** retry_count)
                    logger.warning(f"[Trends] Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[Trends] Response error: {e}")
                    raise
            except Exception as e:
                logger.error(f"[Trends] Unexpected error building payload: {type(e).__name__}: {e}")
                raise

        # Get interest over time
        logger.debug("[Trends] Fetching interest over time data")
        interest_df = pytrends.interest_over_time()

        if interest_df.empty:
            logger.warning("[Trends] No trend data returned from Google Trends")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        logger.info(f"[Trends] Retrieved data with shape: {interest_df.shape}")

        # Convert to long format for our output
        trend_rows = []
        for date_idx in interest_df.index:
            for term in terms:
                if term in interest_df.columns:
                    trend_rows.append({
                        "date": date_idx.strftime("%Y-%m-%d"),
                        "term": term,
                        "interest": int(interest_df.loc[date_idx, term]),
                    })

        trend_data = pd.DataFrame(trend_rows)

        # Create summary statistics
        summary_rows = []
        for term in terms:
            if term in interest_df.columns:
                series = interest_df[term]
                current = int(series.iloc[-1])
                avg = round(series.mean(), 1)

                # Determine trend direction
                recent_avg = series.tail(4).mean()  # Last ~month
                older_avg = series.head(4).mean()  # First ~month
                if recent_avg > older_avg * 1.1:
                    direction = "Rising"
                elif recent_avg < older_avg * 0.9:
                    direction = "Declining"
                else:
                    direction = "Stable"

                summary_rows.append({
                    "term": term,
                    "avg_interest": avg,
                    "peak_interest": int(series.max()),
                    "current_interest": current,
                    "trend_direction": direction,
                })

        trend_summary = pd.DataFrame(summary_rows)

        # Get related queries if requested
        related_df = pd.DataFrame()
        if include_related:
            time.sleep(2)  # Brief pause before next request

            try:
                related = pytrends.related_queries()
                related_rows = []

                for term in terms:
                    if term in related and related[term]:
                        # Top related queries
                        top = related[term].get("top")
                        if top is not None and not top.empty:
                            for _, row in top.head(5).iterrows():
                                related_rows.append({
                                    "term": term,
                                    "related_query": row["query"],
                                    "query_type": "Top",
                                    "value": str(row["value"]),
                                })

                        # Rising related queries
                        rising = related[term].get("rising")
                        if rising is not None and not rising.empty:
                            for _, row in rising.head(5).iterrows():
                                related_rows.append({
                                    "term": term,
                                    "related_query": row["query"],
                                    "query_type": "Rising",
                                    "value": str(row["value"]),
                                })

                if related_rows:
                    related_df = pd.DataFrame(related_rows)

            except Exception as e:
                logger.warning(f"Could not fetch related queries: {e}")

        return trend_data, trend_summary, related_df

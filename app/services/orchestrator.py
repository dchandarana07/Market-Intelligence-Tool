"""
Pipeline Orchestrator - Coordinates module execution and output generation.

Handles:
- Module execution in correct order (Jobs first for skill dependencies)
- Partial failure handling (continue if one module fails)
- Progress tracking
- Executive Summary scoring and synthesis
- Output aggregation to Google Sheets
- Email notification
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Callable
import pandas as pd

from app.modules.base import BaseModule, ModuleResult, ModuleStatus
from app.modules.jobs import JobsModule
from app.modules.courses import CoursesModule
from app.modules.trends import TrendsModule
from app.modules.lightcast import LightcastModule
from app.services.google_sheets import GoogleSheetsService, get_sheets_service
from config.settings import settings

logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    """Overall pipeline status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"  # Some modules succeeded, some failed
    FAILED = "failed"


@dataclass
class ModuleProgress:
    """Progress tracking for a single module."""
    name: str
    display_name: str
    status: ModuleStatus = ModuleStatus.PENDING
    message: str = ""
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[ModuleResult] = None


@dataclass
class PipelineRun:
    """Represents a complete pipeline execution."""
    run_id: str
    user_email: str
    topic: str
    selected_modules: list[str]
    module_inputs: dict[str, dict[str, Any]]
    sharing_mode: str = "restricted"
    status: PipelineStatus = PipelineStatus.PENDING
    progress: dict[str, ModuleProgress] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    output_url: Optional[str] = None
    output_folder_url: Optional[str] = None
    errors: list[str] = field(default_factory=list)


# Available modules registry
MODULES = {
    "jobs": JobsModule,
    "courses": CoursesModule,
    "trends": TrendsModule,
    "lightcast": LightcastModule,
}

# Module execution order (jobs first to provide skills to others)
MODULE_ORDER = ["jobs", "courses", "trends", "lightcast"]


# =============================================================================
# Executive Summary Scoring
# =============================================================================

def _score_job_volume(count: int) -> int:
    """Score job posting volume on 1-5 scale."""
    if count < 5:
        return 1
    elif count < 15:
        return 2
    elif count < 30:
        return 3
    elif count < 50:
        return 4
    else:
        return 5


def _score_salary(avg_salary: float) -> int:
    """Score salary competitiveness vs national median (~$60K)."""
    national_median = 60000
    ratio = avg_salary / national_median if national_median > 0 else 0
    if ratio < 0.8:
        return 1
    elif ratio < 1.0:
        return 2
    elif ratio < 1.3:
        return 3
    elif ratio < 1.7:
        return 4
    else:
        return 5


def _score_search_interest(avg_interest: float) -> int:
    """Score search interest (0-100 scale) on 1-5."""
    if avg_interest < 20:
        return 1
    elif avg_interest < 40:
        return 2
    elif avg_interest < 60:
        return 3
    elif avg_interest < 80:
        return 4
    else:
        return 5


def _score_trend_momentum(trend_data: pd.DataFrame) -> tuple[int, str]:
    """Score trend momentum. Returns (score, description)."""
    if trend_data is None or trend_data.empty:
        return 3, "Insufficient data"

    # Try to detect momentum from time series data
    # Look for columns with numeric data that represent interest over time
    numeric_cols = trend_data.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_cols:
        return 3, "Stable (no numeric trend data)"

    # Use the first numeric column as the trend indicator
    values = trend_data[numeric_cols[0]].dropna().tolist()
    if len(values) < 2:
        return 3, "Stable (insufficient data points)"

    # Compare recent vs earlier values
    mid = len(values) // 2
    early_avg = sum(values[:mid]) / max(len(values[:mid]), 1)
    recent_avg = sum(values[mid:]) / max(len(values[mid:]), 1)

    if early_avg == 0:
        if recent_avg > 0:
            return 5, f"Strong growth (new interest)"
        return 3, "Stable (no activity)"

    change_pct = ((recent_avg - early_avg) / early_avg) * 100

    if change_pct < -10:
        desc = f"Declining ({change_pct:.0f}%)"
        return 1, desc
    elif change_pct < 0:
        desc = f"Slight decline ({change_pct:.0f}%)"
        return 2, desc
    elif change_pct < 10:
        desc = f"Stable ({change_pct:+.0f}%)"
        return 3, desc
    elif change_pct < 20:
        desc = f"Growing ({change_pct:+.0f}%)"
        return 4, desc
    else:
        desc = f"Strong growth ({change_pct:+.0f}%)"
        return 5, desc


def _score_skills_breadth(skill_count: int) -> int:
    """Score distinct skills found on 1-5."""
    if skill_count < 5:
        return 1
    elif skill_count < 10:
        return 2
    elif skill_count < 15:
        return 3
    elif skill_count < 25:
        return 4
    else:
        return 5


def _score_course_competition(course_count: int) -> int:
    """Score course competition (inverted — fewer = more opportunity)."""
    if course_count <= 3:
        return 5  # Blue ocean
    elif course_count <= 8:
        return 4
    elif course_count <= 15:
        return 3
    elif course_count <= 25:
        return 2
    else:
        return 1  # Saturated


def _parse_salary(salary_str: str) -> Optional[float]:
    """Extract numeric salary from string like '$85,000' or '$85K'."""
    if not salary_str:
        return None
    # Remove $ and commas
    cleaned = str(salary_str).replace("$", "").replace(",", "").strip()
    # Handle K suffix
    match = re.match(r'([\d.]+)\s*[Kk]', cleaned)
    if match:
        return float(match.group(1)) * 1000
    # Try plain number
    match = re.match(r'([\d.]+)', cleaned)
    if match:
        return float(match.group(1))
    return None


def _extract_bls_employment(jobs_result: Optional[ModuleResult]) -> tuple[int, str]:
    """Extract total BLS employment count and top occupation from jobs result.

    Returns (total_employment, top_occupation_title).
    """
    if not jobs_result or jobs_result.status not in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        return 0, ""

    for sheet_name, df in jobs_result.data.items():
        if "bls" not in sheet_name.lower() and "labor" not in sheet_name.lower():
            continue
        for col in df.columns:
            if any(kw in col.lower() for kw in ["employment", "workers", "total"]):
                vals = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").dropna()
                if not vals.empty:
                    total = int(vals.sum())
                    # Get title of top occupation
                    top_title = ""
                    for tcol in df.columns:
                        if "title" in tcol.lower() or "occupation" in tcol.lower():
                            top_title = str(df[tcol].iloc[0]) if not df[tcol].empty else ""
                            break
                    return total, top_title
    return 0, ""


def build_executive_summary(
    topic: str,
    results: dict[str, ModuleResult],
    selected_modules: list[str],
) -> pd.DataFrame:
    """
    Build the Executive Summary DataFrame with market demand scores.

    Returns a DataFrame formatted for display as the first sheet in Google Sheets.
    """
    rows = []
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    sources_used = [m.replace("_", " ").title() for m in selected_modules
                    if m in results and results[m].status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]]

    # --- Section 1: Market Overview ---
    rows.append({"Section": "MARKET OVERVIEW", "Metric": "", "Value": "", "Score": "", "Methodology": ""})
    rows.append({"Section": "", "Metric": "Field Analyzed", "Value": topic, "Score": "", "Methodology": "User Input"})
    rows.append({"Section": "", "Metric": "Report Generated", "Value": report_date, "Score": "", "Methodology": "System"})
    rows.append({"Section": "", "Metric": "Data Sources Used", "Value": ", ".join(sources_used) if sources_used else "None", "Score": "", "Methodology": "System"})
    rows.append({"Section": "", "Metric": "", "Value": "", "Score": "", "Methodology": ""})

    # --- Gather cross-module data first ---
    jobs_result = results.get("jobs")
    trends_result = results.get("trends")
    courses_result = results.get("courses")
    lightcast_result = results.get("lightcast")

    # BLS employment (used as fallback for job volume)
    bls_employment, bls_top_occupation = _extract_bls_employment(jobs_result)

    # --- Section 2: Market Demand Scores ---
    rows.append({"Section": "MARKET DEMAND SCORES (1-5)", "Metric": "", "Value": "", "Score": "", "Methodology": ""})

    scores = {}
    weights = {"jobs": 0.25, "salary": 0.20, "seo": 0.15, "trend": 0.15, "skills": 0.15, "courses": 0.10}

    # Job Posting Volume — use Google Jobs postings, but fallback to BLS employment
    job_count = 0
    if jobs_result and jobs_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in jobs_result.data.items():
            if "job" in sheet_name.lower() and "skill" not in sheet_name.lower() and "bls" not in sheet_name.lower():
                job_count += len(df)

    if job_count > 0:
        job_score = _score_job_volume(job_count)
        job_value = f"{job_count} active postings found"
        job_method = "<5=1, 5-15=2, 15-30=3, 30-50=4, 50+=5"
    elif bls_employment > 0:
        # Fallback: use BLS total employment as a proxy for market size
        if bls_employment > 500000:
            job_score = 5
        elif bls_employment > 200000:
            job_score = 4
        elif bls_employment > 100000:
            job_score = 3
        elif bls_employment > 50000:
            job_score = 2
        else:
            job_score = 1
        job_value = f"{bls_employment:,} employed nationally (BLS)"
        job_method = "BLS employment fallback: <50K=1, 50-100K=2, 100-200K=3, 200-500K=4, 500K+=5"
    else:
        job_score = 0
        job_value = "No data available"
        job_method = "No job postings or BLS data"

    scores["jobs"] = job_score
    rows.append({
        "Section": "",
        "Metric": "Job Market Size",
        "Value": job_value,
        "Score": str(job_score) if job_score > 0 else "N/A",
        "Methodology": job_method,
    })

    # Salary Competitiveness
    avg_salary = 0.0
    salary_range = ""
    if jobs_result and jobs_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        salaries = []
        for sheet_name, df in jobs_result.data.items():
            for col in df.columns:
                if any(kw in col.lower() for kw in ["salary", "wage", "pay", "compensation"]):
                    for val in df[col].dropna():
                        parsed = _parse_salary(str(val))
                        if parsed and 15000 < parsed < 500000:
                            salaries.append(parsed)
            if "bls" in sheet_name.lower() or "labor" in sheet_name.lower():
                for col in df.columns:
                    if any(kw in col.lower() for kw in ["annual", "mean", "median"]) and any(kw in col.lower() for kw in ["salary", "wage"]):
                        for val in df[col].dropna():
                            parsed = _parse_salary(str(val))
                            if parsed and 15000 < parsed < 500000:
                                salaries.append(parsed)

        if salaries:
            avg_salary = sum(salaries) / len(salaries)
            salary_range = f"${min(salaries):,.0f}-${max(salaries):,.0f}"

    salary_score = _score_salary(avg_salary) if avg_salary > 0 else 0
    scores["salary"] = salary_score
    rows.append({
        "Section": "",
        "Metric": "Salary Competitiveness",
        "Value": f"${avg_salary:,.0f} avg ({salary_range} range)" if avg_salary > 0 else "No salary data",
        "Score": str(salary_score) if salary_score > 0 else "N/A",
        "Methodology": "vs national median ($60K): <0.8x=1, 0.8-1x=2, 1-1.3x=3, 1.3-1.7x=4, 1.7x+=5",
    })

    # Search Interest (SEO)
    avg_interest = 0.0
    trend_direction_from_summary = ""
    if trends_result and trends_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in trends_result.data.items():
            if "summary" in sheet_name.lower():
                for col in df.columns:
                    if "avg" in col.lower() or col.lower() == "avg_interest":
                        vals = pd.to_numeric(df[col], errors="coerce").dropna()
                        if not vals.empty:
                            avg_interest = float(vals.mean())
                            break
                # Also grab trend_direction from the summary
                for col in df.columns:
                    if "direction" in col.lower():
                        dirs = df[col].dropna().tolist()
                        if dirs:
                            trend_direction_from_summary = str(dirs[0])
                break

    seo_score = _score_search_interest(avg_interest) if avg_interest > 0 else 0
    scores["seo"] = seo_score
    rows.append({
        "Section": "",
        "Metric": "Search Interest (SEO)",
        "Value": f"{avg_interest:.1f}/100 avg" if avg_interest > 0 else "No trends data",
        "Score": str(seo_score) if seo_score > 0 else "N/A",
        "Methodology": "0-20=1, 20-40=2, 40-60=3, 60-80=4, 80-100=5",
    })

    # Trend Momentum — look for any sheet with an "interest" numeric column (the time series)
    trend_df = None
    if trends_result and trends_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in trends_result.data.items():
            # Skip the summary sheet — we want the time-series data
            if "summary" in sheet_name.lower() or "related" in sheet_name.lower():
                continue
            # Check if this sheet has a numeric column that looks like interest data
            if "interest" in [c.lower() for c in df.columns]:
                trend_df = df
                break
            # Fallback: any sheet with "trend" in the name that has numeric data
            if "trend" in sheet_name.lower():
                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                if numeric_cols:
                    trend_df = df
                    break

    trend_score, trend_desc = _score_trend_momentum(trend_df)
    # If we got a direction from the summary, use it to supplement
    if trend_direction_from_summary and trend_desc == "Insufficient data":
        direction = trend_direction_from_summary.lower()
        if "rising" in direction:
            trend_score = 4
            trend_desc = f"Rising (per Google Trends)"
        elif "falling" in direction or "declin" in direction:
            trend_score = 2
            trend_desc = f"Declining (per Google Trends)"
        else:
            trend_score = 3
            trend_desc = f"{trend_direction_from_summary} (per Google Trends)"

    scores["trend"] = trend_score
    rows.append({
        "Section": "",
        "Metric": "Trend Momentum",
        "Value": trend_desc,
        "Score": str(trend_score),
        "Methodology": "Decline>10%=1, Decline=2, Stable=3, Growth=4, Growth>20%=5",
    })

    # Skills Demand Breadth — combine jobs skills + lightcast
    skill_count = 0
    skills_list = []
    if jobs_result and jobs_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        skills_df = jobs_result.data.get("Skills Summary")
        if skills_df is not None and not skills_df.empty:
            skill_count = len(skills_df)
            if "skill" in skills_df.columns:
                skills_list = skills_df["skill"].tolist()[:10]

    # Lightcast related skills
    lightcast_skills = []
    if lightcast_result and lightcast_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in lightcast_result.data.items():
            if "related" in sheet_name.lower() or "skill" in sheet_name.lower():
                for col in df.columns:
                    if "skill_name" in col.lower() or "name" in col.lower():
                        lightcast_skills.extend(df[col].dropna().tolist())
                        break

    # Merge skill lists (deduplicated)
    if lightcast_skills:
        all_skills_set = set(s.lower() for s in skills_list)
        for s in lightcast_skills:
            if s.lower() not in all_skills_set:
                skills_list.append(s)
                all_skills_set.add(s.lower())
        skill_count = max(skill_count, len(skills_list))

    skills_score = _score_skills_breadth(skill_count)
    scores["skills"] = skills_score
    rows.append({
        "Section": "",
        "Metric": "Skills Demand Breadth",
        "Value": f"{skill_count} distinct skills identified",
        "Score": str(skills_score),
        "Methodology": "<5=1, 5-10=2, 10-15=3, 15-25=4, 25+=5",
    })

    # Course Competition
    course_count = 0
    if courses_result and courses_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in courses_result.data.items():
            if "course" in sheet_name.lower():
                course_count += len(df)

    course_score = _score_course_competition(course_count)
    scores["courses"] = course_score
    rows.append({
        "Section": "",
        "Metric": "Course Competition",
        "Value": f"{course_count} existing courses",
        "Score": str(course_score),
        "Methodology": "0-3=5(blue ocean), 3-8=4, 8-15=3, 15-25=2, 25+=1(saturated)",
    })

    # Overall Weighted Average
    weighted_sum = 0.0
    total_weight = 0.0
    score_to_weight = {
        "jobs": weights["jobs"],
        "salary": weights["salary"],
        "seo": weights["seo"],
        "trend": weights["trend"],
        "skills": weights["skills"],
        "courses": weights["courses"],
    }
    for key, score in scores.items():
        if score > 0:
            w = score_to_weight.get(key, 0)
            weighted_sum += score * w
            total_weight += w

    overall_score = weighted_sum / total_weight if total_weight > 0 else 0
    rows.append({
        "Section": "",
        "Metric": "OVERALL MARKET DEMAND",
        "Value": f"Weighted average of {len([s for s in scores.values() if s > 0])} indicators",
        "Score": f"{overall_score:.1f}",
        "Methodology": "Jobs 25%, Salary 20%, SEO 15%, Trend 15%, Skills 15%, Courses 10%",
    })

    rows.append({"Section": "", "Metric": "", "Value": "", "Score": "", "Methodology": ""})

    # --- Section 3: Key Findings ---
    rows.append({"Section": "KEY FINDINGS", "Metric": "", "Value": "", "Score": "", "Methodology": ""})

    # Top Hiring Companies
    top_companies = []
    if jobs_result and jobs_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in jobs_result.data.items():
            if "job" in sheet_name.lower() and "skill" not in sheet_name.lower() and "bls" not in sheet_name.lower():
                for col in df.columns:
                    if any(kw in col.lower() for kw in ["company", "employer", "organization"]):
                        top_companies = df[col].dropna().value_counts().head(5).index.tolist()
                        break
                if top_companies:
                    break

    if top_companies:
        rows.append({
            "Section": "",
            "Metric": "Top Hiring Companies",
            "Value": ", ".join(str(c) for c in top_companies[:5]),
            "Score": "",
            "Methodology": "From job postings",
        })

    # Dominant Skills — always show if we have any skills data
    if skills_list:
        rows.append({
            "Section": "",
            "Metric": "Dominant Skills",
            "Value": ", ".join(str(s) for s in skills_list[:8]),
            "Score": "",
            "Methodology": "From job postings and Lightcast",
        })

    # BLS Employment Data
    if jobs_result and jobs_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in jobs_result.data.items():
            if "bls" not in sheet_name.lower() and "labor" not in sheet_name.lower():
                continue

            # Employment
            for col in df.columns:
                if any(kw in col.lower() for kw in ["employment", "workers", "total"]):
                    val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if val:
                        rows.append({
                            "Section": "",
                            "Metric": "BLS Employment",
                            "Value": f"{val:,}" if isinstance(val, (int, float)) else str(val),
                            "Score": "",
                            "Methodology": "Bureau of Labor Statistics",
                        })
                        break

            # Salary
            for col in df.columns:
                if "mean" in col.lower() and ("salary" in col.lower() or "annual" in col.lower()):
                    val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if val:
                        rows.append({
                            "Section": "",
                            "Metric": "BLS Mean Salary",
                            "Value": str(val),
                            "Score": "",
                            "Methodology": "Bureau of Labor Statistics",
                        })
                        break

            # Top BLS occupations
            occ_col = None
            for col in df.columns:
                if "title" in col.lower() or "occupation" in col.lower():
                    occ_col = col
                    break
            if occ_col:
                occupations = df[occ_col].dropna().tolist()[:5]
                if occupations:
                    rows.append({
                        "Section": "",
                        "Metric": "Related BLS Occupations",
                        "Value": ", ".join(str(o) for o in occupations),
                        "Score": "",
                        "Methodology": "Bureau of Labor Statistics SOC codes",
                    })
            break

    # Trend Direction
    trend_display = trend_desc
    if trend_direction_from_summary and trend_direction_from_summary not in trend_desc:
        trend_display = f"{trend_direction_from_summary} — {trend_desc}"
    rows.append({
        "Section": "",
        "Metric": "Trend Direction",
        "Value": trend_display,
        "Score": "",
        "Methodology": "Google Trends 12-month data",
    })

    # Top Course Providers
    top_providers = []
    if courses_result and courses_result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
        for sheet_name, df in courses_result.data.items():
            if "course" in sheet_name.lower():
                for col in df.columns:
                    if any(kw in col.lower() for kw in ["provider", "institution", "organization"]):
                        top_providers = df[col].dropna().value_counts().head(5).index.tolist()
                        break
                if top_providers:
                    break

    if top_providers:
        rows.append({
            "Section": "",
            "Metric": "Top Course Providers",
            "Value": ", ".join(str(p) for p in top_providers[:5]),
            "Score": "",
            "Methodology": "From Coursera/EdX scraping",
        })

    return pd.DataFrame(rows)


class PipelineOrchestrator:
    """
    Orchestrates the execution of market intelligence modules.

    Usage:
        orchestrator = PipelineOrchestrator()
        run = await orchestrator.execute(
            user_email="user@example.com",
            topic="Cybersecurity",
            selected_modules=["jobs", "courses", "trends"],
            module_inputs={
                "jobs": {"query": "cybersecurity analyst", "location": "United States"},
                "courses": {"keywords": "cybersecurity"},
                "trends": {"auto_from_jobs": True},
            },
        )
    """

    def __init__(
        self,
        sheets_service: Optional[GoogleSheetsService] = None,
        progress_callback: Optional[Callable[[str, ModuleProgress], None]] = None,
    ):
        self._sheets_service = sheets_service
        self._progress_callback = progress_callback
        self._modules: dict[str, BaseModule] = {}

        for name, module_class in MODULES.items():
            self._modules[name] = module_class()

    def get_module(self, name: str) -> Optional[BaseModule]:
        """Get a module by name."""
        return self._modules.get(name)

    def get_available_modules(self) -> list[dict]:
        """Get list of available modules with their status."""
        modules = []
        for name in MODULE_ORDER:
            module = self._modules.get(name)
            if module:
                modules.append({
                    "name": module.name,
                    "display_name": module.display_name,
                    "description": module.description,
                    "available": module.is_available(),
                    "availability_message": module.get_availability_message(),
                })
        return modules

    async def execute(
        self,
        user_email: str,
        topic: str,
        selected_modules: list[str],
        module_inputs: dict[str, dict[str, Any]],
        sharing_mode: str = "restricted",
    ) -> PipelineRun:
        """Execute the pipeline with selected modules."""
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        run = PipelineRun(
            run_id=run_id,
            user_email=user_email,
            topic=topic,
            selected_modules=selected_modules,
            module_inputs=module_inputs,
            sharing_mode=sharing_mode,
            started_at=datetime.now(),
        )

        for module_name in selected_modules:
            module = self._modules.get(module_name)
            if module:
                run.progress[module_name] = ModuleProgress(
                    name=module_name,
                    display_name=module.display_name,
                )

        run.status = PipelineStatus.RUNNING
        logger.info(f"Starting pipeline run {run_id} for topic: {topic}")

        extracted_skills: list[str] = []
        trend_terms: list[str] = []
        all_results: dict[str, ModuleResult] = {}

        # Execute modules in order
        for module_name in MODULE_ORDER:
            if module_name not in selected_modules:
                continue

            module = self._modules.get(module_name)
            if not module:
                continue

            progress = run.progress[module_name]
            progress.status = ModuleStatus.RUNNING
            progress.started_at = datetime.now()
            progress.message = f"Running {module.display_name}..."
            self._notify_progress(run_id, progress)

            try:
                inputs = module_inputs.get(module_name, {})

                if module_name == "lightcast":
                    result = await module.execute(inputs, job_skills=extracted_skills, trend_terms=trend_terms)
                elif module_name == "trends" and extracted_skills:
                    result = await module.execute(inputs, job_skills=extracted_skills)
                else:
                    result = await module.execute(inputs)

                all_results[module_name] = result
                progress.result = result
                progress.status = result.status
                progress.completed_at = datetime.now()

                # Extract skills from jobs for dependent modules
                if module_name == "jobs" and result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
                    skills_df = result.data.get("Skills Summary")
                    if skills_df is not None and not skills_df.empty:
                        extracted_skills = skills_df["skill"].tolist()
                        logger.info(f"Extracted {len(extracted_skills)} skills from jobs")

                # Extract terms from trends for Lightcast module
                if module_name == "trends" and result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
                    summary_df = result.data.get("Trends Summary")
                    if summary_df is not None and not summary_df.empty and "term" in summary_df.columns:
                        trend_terms = summary_df["term"].unique().tolist()
                        logger.info(f"Extracted {len(trend_terms)} terms from trends for reuse")

                if result.status == ModuleStatus.COMPLETED:
                    progress.message = f"Completed successfully ({result.total_rows} rows)"
                elif result.status == ModuleStatus.PARTIAL:
                    progress.message = f"Completed with warnings ({result.total_rows} rows)"
                else:
                    progress.message = f"Failed: {', '.join(result.errors[:2])}"

                logger.info(f"Module {module_name} completed with status: {result.status}")

            except Exception as e:
                logger.error(f"Module {module_name} failed with exception: {e}")
                progress.status = ModuleStatus.FAILED
                progress.completed_at = datetime.now()
                progress.message = f"Error: {str(e)}"
                run.errors.append(f"{module.display_name}: {str(e)}")

            self._notify_progress(run_id, progress)

        # Build Executive Summary
        try:
            logger.info("Building Executive Summary...")
            exec_summary_df = build_executive_summary(
                topic=topic,
                results=all_results,
                selected_modules=selected_modules,
            )
            logger.info(f"Executive Summary built with {len(exec_summary_df)} rows")
        except Exception as e:
            logger.error(f"Failed to build Executive Summary: {e}")
            exec_summary_df = None

        # Aggregate results and create output
        try:
            output_info = await self._create_output(
                run=run,
                results=all_results,
                executive_summary=exec_summary_df,
            )
            run.output_url = output_info.get("spreadsheet_url")
            run.output_folder_url = output_info.get("folder_url")

        except Exception as e:
            logger.error(f"Failed to create output: {e}")
            run.errors.append(f"Output creation failed: {str(e)}")

        # Determine final status
        run.completed_at = datetime.now()

        statuses = [p.status for p in run.progress.values()]
        if all(s == ModuleStatus.COMPLETED for s in statuses):
            run.status = PipelineStatus.COMPLETED
        elif all(s == ModuleStatus.FAILED for s in statuses):
            run.status = PipelineStatus.FAILED
        elif any(s in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL] for s in statuses):
            run.status = PipelineStatus.PARTIAL
        else:
            run.status = PipelineStatus.FAILED

        logger.info(f"Pipeline run {run_id} completed with status: {run.status}")
        return run

    async def _create_output(
        self,
        run: PipelineRun,
        results: dict[str, ModuleResult],
        executive_summary: Optional[pd.DataFrame] = None,
    ) -> dict:
        """Create Google Sheets output from module results."""
        sheets_service = self._sheets_service or get_sheets_service()

        if not sheets_service.is_available():
            raise RuntimeError(
                "Google Sheets service not available. "
                "Please configure GOOGLE_CREDENTIALS_PATH and GOOGLE_DRIVE_FOLDER_ID."
            )

        # Build ordered data dict — Executive Summary first
        all_data: dict[str, pd.DataFrame] = {}

        if executive_summary is not None and not executive_summary.empty:
            all_data["Executive Summary"] = executive_summary

        # Module data
        module_prefixes = {
            "jobs": "Jobs",
            "courses": "Courses",
            "trends": "Trends",
            "lightcast": "Lightcast",
        }

        for module_name, result in results.items():
            if result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
                prefix = module_prefixes.get(module_name, module_name.title())
                for sheet_name, df in result.data.items():
                    if not df.empty:
                        full_name = f"{prefix} - {sheet_name}"
                        full_name = full_name[:95]
                        if full_name in all_data:
                            full_name = f"{full_name} (2)"
                        all_data[full_name] = df

        # If no successful data, create a summary sheet with error information
        if not all_data:
            logger.warning("No module data available, creating summary-only spreadsheet")
            summary_data = {
                "Run Summary": pd.DataFrame({
                    "Topic": [run.topic],
                    "Status": ["Failed - No data collected"],
                    "Modules Run": [", ".join(run.progress.keys())],
                    "Errors": ["; ".join(run.errors) if run.errors else "Unknown error"],
                    "Started": [run.started_at.strftime("%Y-%m-%d %H:%M:%S")],
                    "Completed": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                })
            }
            all_data = summary_data

        # Create the spreadsheet
        title = f"Market Intelligence - {run.topic}"
        try:
            output_info = sheets_service.create_output(
                title=title,
                data=all_data,
                share_with=run.user_email,
                sharing_mode=run.sharing_mode,
            )
            logger.info(f"Successfully created spreadsheet: {output_info.get('spreadsheet_url')}")
            return output_info
        except Exception as e:
            logger.error(f"Google Sheets failed: {e}, falling back to XLSX download")

            # FALLBACK: Save as local XLSX file for download
            try:
                return self._save_xlsx_fallback(title, all_data, run.run_id)
            except Exception as xlsx_err:
                logger.error(f"XLSX fallback also failed: {xlsx_err}", exc_info=True)
                return {
                    "spreadsheet_id": None,
                    "spreadsheet_url": None,
                    "folder_url": None,
                    "shared_with": [],
                    "error": str(e),
                }

    def _save_xlsx_fallback(
        self, title: str, all_data: dict[str, pd.DataFrame], run_id: str
    ) -> dict:
        """Save report as XLSX file for download when Google Sheets fails."""
        import os

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        filename = f"{title.replace(' ', '_')}_{timestamp}.xlsx"

        # Ensure downloads directory exists
        downloads_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "downloads")
        os.makedirs(downloads_dir, exist_ok=True)

        filepath = os.path.join(downloads_dir, filename)

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            for sheet_name, df in all_data.items():
                # Excel sheet names max 31 chars
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)

        download_url = f"/static/downloads/{filename}"
        logger.info(f"XLSX fallback saved: {filepath}")

        return {
            "spreadsheet_id": None,
            "spreadsheet_url": download_url,
            "folder_url": None,
            "shared_with": [],
            "xlsx_fallback": True,
        }

    def _notify_progress(self, run_id: str, progress: ModuleProgress) -> None:
        """Notify progress callback if configured."""
        if self._progress_callback:
            try:
                self._progress_callback(run_id, progress)
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")


# Singleton instance
_orchestrator: Optional[PipelineOrchestrator] = None


def get_orchestrator() -> PipelineOrchestrator:
    """Get the singleton orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = PipelineOrchestrator()
    return _orchestrator

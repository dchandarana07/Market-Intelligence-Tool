"""
Pipeline Orchestrator - Coordinates module execution and output generation.

Handles:
- Module execution in correct order (Jobs first for skill dependencies)
- Partial failure handling (continue if one module fails)
- Progress tracking
- Output aggregation to Google Sheets
- Email notification
"""

import asyncio
import logging
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
        """
        Initialize the orchestrator.

        Args:
            sheets_service: Google Sheets service instance (uses default if not provided)
            progress_callback: Optional callback for progress updates
        """
        self._sheets_service = sheets_service
        self._progress_callback = progress_callback
        self._modules: dict[str, BaseModule] = {}

        # Initialize module instances
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
        """
        Execute the pipeline with selected modules.

        Args:
            user_email: Email to share results with
            topic: Topic/title for the output
            selected_modules: List of module names to run
            module_inputs: Inputs for each module
            sharing_mode: "restricted" or "anyone"

        Returns:
            PipelineRun with results and status
        """
        # Generate run ID
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Initialize run tracking
        run = PipelineRun(
            run_id=run_id,
            user_email=user_email,
            topic=topic,
            selected_modules=selected_modules,
            module_inputs=module_inputs,
            sharing_mode=sharing_mode,
            started_at=datetime.now(),
        )

        # Initialize progress for each selected module
        for module_name in selected_modules:
            module = self._modules.get(module_name)
            if module:
                run.progress[module_name] = ModuleProgress(
                    name=module_name,
                    display_name=module.display_name,
                )

        run.status = PipelineStatus.RUNNING
        logger.info(f"Starting pipeline run {run_id} for topic: {topic}")

        # Track extracted skills for passing to dependent modules
        extracted_skills: list[str] = []
        trend_terms: list[str] = []  # Terms from Trends module for reuse in Lightcast
        all_results: dict[str, ModuleResult] = {}

        # Execute modules in order
        for module_name in MODULE_ORDER:
            if module_name not in selected_modules:
                continue

            module = self._modules.get(module_name)
            if not module:
                continue

            # Update progress
            progress = run.progress[module_name]
            progress.status = ModuleStatus.RUNNING
            progress.started_at = datetime.now()
            progress.message = f"Running {module.display_name}..."
            self._notify_progress(run_id, progress)

            try:
                # Get inputs for this module
                inputs = module_inputs.get(module_name, {})

                # Execute module with context from previous modules
                if module_name == "lightcast":
                    # Lightcast can use skills from jobs or terms from trends
                    result = await module.execute(inputs, job_skills=extracted_skills, trend_terms=trend_terms)
                elif module_name == "trends" and extracted_skills:
                    # Trends can use skills from jobs (but now requires manual input)
                    result = await module.execute(inputs, job_skills=extracted_skills)
                else:
                    result = await module.execute(inputs)

                # Store result
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
                    # Get the terms from the trend summary
                    summary_df = result.data.get("Trend Summary")
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

        # Aggregate results and create output
        try:
            output_info = await self._create_output(
                run=run,
                results=all_results,
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
    ) -> dict:
        """Create Google Sheets output from module results."""
        sheets_service = self._sheets_service or get_sheets_service()

        if not sheets_service.is_available():
            raise RuntimeError(
                "Google Sheets service not available. "
                "Please configure GOOGLE_CREDENTIALS_PATH and GOOGLE_DRIVE_FOLDER_ID."
            )

        # Aggregate all DataFrames from all modules
        all_data: dict[str, pd.DataFrame] = {}

        for module_name, result in results.items():
            if result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]:
                for sheet_name, df in result.data.items():
                    if not df.empty:
                        # Prefix sheet name with module if there might be conflicts
                        full_name = f"{self._modules[module_name].display_name} - {sheet_name}"
                        # Truncate to 100 chars (Sheets limit)
                        full_name = full_name[:100]
                        all_data[full_name] = df

        # If no successful data, create a summary sheet with error information
        if not all_data:
            logger.warning("No module data available, creating summary-only spreadsheet")
            # Create a summary dataframe with run information
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
            logger.error(f"Failed to create spreadsheet: {e}", exc_info=True)
            # Return a minimal output info so the run doesn't completely fail
            return {
                "spreadsheet_id": None,
                "spreadsheet_url": None,
                "folder_url": None,
                "shared_with": [],
                "error": str(e),
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

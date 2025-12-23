"""
Unit tests for Pipeline Orchestrator.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
from datetime import datetime

from app.services.orchestrator import PipelineOrchestrator, PipelineStatus
from app.modules.base import ModuleResult, ModuleStatus


class TestPipelineOrchestrator:
    """Test Pipeline Orchestrator functionality."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mocked sheets service."""
        with patch('app.services.orchestrator.get_sheets_service') as mock_sheets:
            mock_sheets_instance = MagicMock()
            mock_sheets_instance.is_available.return_value = True
            mock_sheets.return_value = mock_sheets_instance
            return PipelineOrchestrator(sheets_service=mock_sheets_instance)

    def test_get_available_modules(self, orchestrator):
        """Test getting list of available modules."""
        modules = orchestrator.get_available_modules()

        assert len(modules) > 0
        assert all("name" in m for m in modules)
        assert all("display_name" in m for m in modules)
        assert all("available" in m for m in modules)

    @pytest.mark.asyncio
    async def test_execute_single_module_success(self, orchestrator):
        """Test successful execution with single module."""
        # Mock module execution
        mock_result = ModuleResult.success(
            data={"Jobs": pd.DataFrame([{"title": "Software Engineer"}])},
            metadata={"jobs_found": 1}
        )

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = mock_result

            # Mock sheets service
            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_id": "test_id",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test_id",
                "folder_url": "https://drive.google.com/drive/folders/test_folder",
                "shared_with": ["user@example.com"]
            }

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Software Jobs",
                selected_modules=["jobs"],
                module_inputs={"jobs": {"query": "software engineer", "results_limit": 20}}
            )

            assert run.status == PipelineStatus.COMPLETED
            assert run.output_url is not None
            assert "jobs" in run.progress
            assert run.progress["jobs"].status == ModuleStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_execute_multiple_modules_success(self, orchestrator):
        """Test successful execution with multiple modules."""
        # Mock jobs module
        jobs_result = ModuleResult.success(
            data={
                "Jobs": pd.DataFrame([{"title": "Engineer"}]),
                "Skills Summary": pd.DataFrame([{"skill": "python", "frequency": 5}])
            }
        )

        # Mock trends module
        trends_result = ModuleResult.success(
            data={"Trend Summary": pd.DataFrame([{"term": "python", "interest": 100}])}
        )

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_jobs, \
             patch.object(orchestrator._modules["trends"], 'execute', new_callable=AsyncMock) as mock_trends:

            mock_jobs.return_value = jobs_result
            mock_trends.return_value = trends_result

            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_id": "test_id",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test_id"
            }

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Tech Analysis",
                selected_modules=["jobs", "trends"],
                module_inputs={
                    "jobs": {"query": "software", "results_limit": 20},
                    "trends": {"terms": "python, javascript"}
                }
            )

            assert run.status == PipelineStatus.COMPLETED
            assert len(run.progress) == 2

    @pytest.mark.asyncio
    async def test_execute_partial_failure(self, orchestrator):
        """Test execution when one module fails but others succeed."""
        # Mock successful module
        success_result = ModuleResult.success(
            data={"Jobs": pd.DataFrame([{"title": "Engineer"}])}
        )

        # Mock failed module
        failed_result = ModuleResult.failure(["API Error"])

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_jobs, \
             patch.object(orchestrator._modules["trends"], 'execute', new_callable=AsyncMock) as mock_trends:

            mock_jobs.return_value = success_result
            mock_trends.return_value = failed_result

            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_id": "test_id",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test_id"
            }

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Analysis",
                selected_modules=["jobs", "trends"],
                module_inputs={
                    "jobs": {"query": "software", "results_limit": 20},
                    "trends": {"terms": "python"}
                }
            )

            assert run.status == PipelineStatus.PARTIAL
            assert run.progress["jobs"].status == ModuleStatus.COMPLETED
            assert run.progress["trends"].status == ModuleStatus.FAILED

    @pytest.mark.asyncio
    async def test_execute_complete_failure(self, orchestrator):
        """Test execution when all modules fail."""
        failed_result = ModuleResult.failure(["API Error"])

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_jobs:
            mock_jobs.return_value = failed_result

            # Even on complete failure, spreadsheet should be created
            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_id": "test_id",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test_id"
            }

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Analysis",
                selected_modules=["jobs"],
                module_inputs={"jobs": {"query": "test", "results_limit": 20}}
            )

            assert run.status == PipelineStatus.FAILED
            assert run.output_url is not None  # Spreadsheet still created

    @pytest.mark.asyncio
    async def test_trends_to_lightcast_data_passing(self, orchestrator):
        """Test that terms from Trends are passed to Lightcast."""
        # Mock trends result with terms
        trends_result = ModuleResult.success(
            data={"Trend Summary": pd.DataFrame([
                {"term": "python", "interest": 100},
                {"term": "javascript", "interest": 85}
            ])}
        )

        # Mock lightcast module
        lightcast_result = ModuleResult.success(
            data={"Skills Normalized": pd.DataFrame([{"skill": "python"}])}
        )

        with patch.object(orchestrator._modules["trends"], 'execute', new_callable=AsyncMock) as mock_trends, \
             patch.object(orchestrator._modules["lightcast"], 'execute', new_callable=AsyncMock) as mock_lightcast:

            mock_trends.return_value = trends_result
            mock_lightcast.return_value = lightcast_result

            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_id": "test_id",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/test_id"
            }

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Analysis",
                selected_modules=["trends", "lightcast"],
                module_inputs={
                    "trends": {"terms": "python, javascript"},
                    "lightcast": {"reuse_from_trends": True, "max_skills": 30}
                }
            )

            # Verify lightcast was called with trend_terms
            assert mock_lightcast.called
            call_kwargs = mock_lightcast.call_args[1]
            assert "trend_terms" in call_kwargs
            assert "python" in call_kwargs["trend_terms"]
            assert "javascript" in call_kwargs["trend_terms"]

    @pytest.mark.asyncio
    async def test_spreadsheet_creation_failure_handling(self, orchestrator):
        """Test that run doesn't completely fail if spreadsheet creation fails."""
        success_result = ModuleResult.success(
            data={"Jobs": pd.DataFrame([{"title": "Engineer"}])}
        )

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_jobs:
            mock_jobs.return_value = success_result

            # Mock spreadsheet creation failure
            orchestrator._sheets_service.create_output.side_effect = Exception("Sheets API Error")

            run = await orchestrator.execute(
                user_email="user@example.com",
                topic="Analysis",
                selected_modules=["jobs"],
                module_inputs={"jobs": {"query": "software", "results_limit": 20}}
            )

            # Run should still show module completed
            assert run.progress["jobs"].status == ModuleStatus.COMPLETED
            # But overall might be partial due to sheet creation failure
            assert len(run.errors) > 0
            assert any("Output creation failed" in e for e in run.errors)

    @pytest.mark.asyncio
    async def test_module_execution_order(self, orchestrator):
        """Test that modules execute in correct order (jobs first)."""
        execution_order = []

        async def track_execution(module_name):
            execution_order.append(module_name)
            return ModuleResult.success(data={f"{module_name}": pd.DataFrame()})

        with patch.object(orchestrator._modules["jobs"], 'execute', new_callable=AsyncMock) as mock_jobs, \
             patch.object(orchestrator._modules["trends"], 'execute', new_callable=AsyncMock) as mock_trends, \
             patch.object(orchestrator._modules["lightcast"], 'execute', new_callable=AsyncMock) as mock_lightcast:

            mock_jobs.side_effect = lambda *args, **kwargs: track_execution("jobs")
            mock_trends.side_effect = lambda *args, **kwargs: track_execution("trends")
            mock_lightcast.side_effect = lambda *args, **kwargs: track_execution("lightcast")

            orchestrator._sheets_service.create_output.return_value = {
                "spreadsheet_url": "https://test.com"
            }

            await orchestrator.execute(
                user_email="user@example.com",
                topic="Analysis",
                selected_modules=["lightcast", "trends", "jobs"],  # Order doesn't matter in input
                module_inputs={
                    "jobs": {"query": "test", "results_limit": 20},
                    "trends": {"terms": "python"},
                    "lightcast": {"skills": "python", "max_skills": 30}
                }
            )

            # Jobs should execute first (defined in MODULE_ORDER)
            assert execution_order[0] == "jobs"

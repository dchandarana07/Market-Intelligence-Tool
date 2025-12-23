"""
Unit tests for Jobs Module.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import pandas as pd

from app.modules.jobs import JobsModule
from app.modules.base import ModuleStatus


class TestJobsModuleValidation:
    """Test validation logic for Jobs module."""

    @pytest.fixture
    def jobs_module(self):
        return JobsModule()

    def test_valid_inputs(self, jobs_module):
        """Test that valid inputs pass validation."""
        inputs = {
            "query": "software engineer",
            "location": "Phoenix, AZ",
            "results_limit": 20,
            "employment_type": "all",
            "date_posted": "month",
            "include_bls": True,
            "extract_skills": True
        }
        result = jobs_module.validate_inputs(inputs)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_query(self, jobs_module):
        """Test that missing query fails validation."""
        inputs = {"query": ""}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "query" in result.errors
        assert "required" in result.errors["query"][0].lower()

    def test_query_too_short(self, jobs_module):
        """Test that query less than 2 chars fails."""
        inputs = {"query": "a"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "query" in result.errors
        assert "2 characters" in result.errors["query"][0]

    def test_query_too_long(self, jobs_module):
        """Test that query over 200 chars fails."""
        inputs = {"query": "a" * 201}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "query" in result.errors
        assert "200 characters" in result.errors["query"][0]

    def test_location_too_short(self, jobs_module):
        """Test that location less than 2 chars fails if provided."""
        inputs = {"query": "developer", "location": "a"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "location" in result.errors

    def test_location_too_long(self, jobs_module):
        """Test that location over 100 chars fails."""
        inputs = {"query": "developer", "location": "a" * 101}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "location" in result.errors

    def test_results_limit_not_integer(self, jobs_module):
        """Test that non-integer results_limit fails."""
        inputs = {"query": "developer", "results_limit": "abc"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "results_limit" in result.errors

    def test_results_limit_out_of_range(self, jobs_module):
        """Test that results_limit outside 5-100 fails."""
        inputs = {"query": "developer", "results_limit": 200}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "results_limit" in result.errors

    def test_invalid_employment_type(self, jobs_module):
        """Test that invalid employment_type fails."""
        inputs = {"query": "developer", "employment_type": "INVALID"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "employment_type" in result.errors

    def test_invalid_date_posted(self, jobs_module):
        """Test that invalid date_posted fails."""
        inputs = {"query": "developer", "date_posted": "invalid"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "date_posted" in result.errors

    def test_invalid_boolean_fields(self, jobs_module):
        """Test that non-boolean values for boolean fields fail."""
        inputs = {"query": "developer", "include_bls": "yes"}
        result = jobs_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "include_bls" in result.errors


class TestJobsModuleExecution:
    """Test execution logic for Jobs module."""

    @pytest.fixture
    def jobs_module(self):
        return JobsModule()

    @pytest.mark.asyncio
    async def test_execute_success(self, jobs_module, mock_jobs_api_response, mock_bls_api_response):
        """Test successful job search execution."""
        with patch.object(jobs_module, '_fetch_google_jobs', new_callable=AsyncMock) as mock_fetch_jobs, \
             patch.object(jobs_module, '_fetch_bls_data', new_callable=AsyncMock) as mock_fetch_bls:

            # Mock returns
            jobs_df = pd.DataFrame([
                {"job_title": "Software Engineer", "company": "Tech Corp", "location": "SF"}
            ])
            skills = ["python", "javascript"]
            mock_fetch_jobs.return_value = (jobs_df, skills)
            mock_fetch_bls.return_value = pd.DataFrame([
                {"soc_code": "15-1252", "occupation_title": "Software Developers"}
            ])

            inputs = {
                "query": "software engineer",
                "location": "United States",
                "results_limit": 20,
                "include_bls": True,
                "extract_skills": True
            }

            result = await jobs_module.execute(inputs)

            assert result.status == ModuleStatus.COMPLETED
            assert "Jobs" in result.data
            assert "BLS Data" in result.data
            assert "Skills Summary" in result.data
            assert not result.data["Jobs"].empty

    @pytest.mark.asyncio
    async def test_execute_no_jobs_found(self, jobs_module):
        """Test execution when no jobs are found."""
        with patch.object(jobs_module, '_fetch_google_jobs', new_callable=AsyncMock) as mock_fetch_jobs:
            mock_fetch_jobs.return_value = (pd.DataFrame(), [])

            inputs = {
                "query": "nonexistent job xyz123",
                "location": "Mars",
                "results_limit": 20
            }

            result = await jobs_module.execute(inputs)

            assert result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]
            assert result.data["Jobs"].empty
            assert len(result.warnings) > 0

    @pytest.mark.asyncio
    async def test_execute_api_failure(self, jobs_module):
        """Test execution when API fails."""
        with patch.object(jobs_module, '_fetch_google_jobs', new_callable=AsyncMock) as mock_fetch_jobs:
            mock_fetch_jobs.side_effect = Exception("API Error")

            inputs = {
                "query": "software engineer",
                "results_limit": 20
            }

            result = await jobs_module.execute(inputs)

            assert result.status == ModuleStatus.FAILED
            assert len(result.errors) > 0

    def test_extract_skills(self, jobs_module):
        """Test skill extraction from job description."""
        description = "We need someone with Python, JavaScript, and AWS experience. Strong communication skills required."
        skills = jobs_module._extract_skills(description)

        assert "python" in skills
        assert "javascript" in skills
        assert "aws" in skills
        assert "communication" in skills

    def test_parse_salary(self, jobs_module):
        """Test salary parsing."""
        job_data = {
            "detected_extensions": {
                "salary": "$50,000 - $70,000"
            }
        }
        min_sal, max_sal = jobs_module._parse_salary(job_data)
        assert min_sal == "50000"
        assert max_sal == "70000"

    def test_parse_salary_with_k_notation(self, jobs_module):
        """Test salary parsing with K notation."""
        job_data = {
            "detected_extensions": {
                "salary": "$50K - $70K"
            }
        }
        min_sal, max_sal = jobs_module._parse_salary(job_data)
        assert float(min_sal) == 50000
        assert float(max_sal) == 70000

    def test_create_skills_summary(self, jobs_module):
        """Test skills summary creation."""
        skills = ["python", "python", "javascript", "python", "aws"]
        df = jobs_module._create_skills_summary(skills, 5)

        assert not df.empty
        assert df.iloc[0]["skill"] == "Python"  # Most frequent
        assert df.iloc[0]["frequency"] == 3
        assert df.iloc[0]["percentage"] == 60.0

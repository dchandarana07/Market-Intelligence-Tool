"""
Unit tests for Trends Module.
"""
import pytest
from unittest.mock import AsyncMock, patch
import pandas as pd

from app.modules.trends import TrendsModule
from app.modules.base import ModuleStatus


class TestTrendsModuleValidation:
    """Test validation logic for Trends module."""

    @pytest.fixture
    def trends_module(self):
        return TrendsModule()

    def test_valid_inputs(self, trends_module):
        """Test that valid inputs pass validation."""
        inputs = {
            "terms": "python, javascript, java",
            "max_terms": 5,
            "timeframe": "today 12-m",
            "region": "united_states"
        }
        result = trends_module.validate_inputs(inputs)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_terms(self, trends_module):
        """Test that missing terms fails validation."""
        inputs = {"terms": ""}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "terms" in result.errors
        assert "required" in result.errors["terms"][0].lower()

    def test_empty_terms_after_parsing(self, trends_module):
        """Test that empty string with only whitespace fails."""
        inputs = {"terms": "   ,  ,  "}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "terms" in result.errors

    def test_too_many_terms(self, trends_module):
        """Test that more than 5 terms fails."""
        inputs = {"terms": "python, java, javascript, ruby, go, rust"}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "terms" in result.errors
        assert "5 terms" in result.errors["terms"][0]

    def test_max_terms_not_integer(self, trends_module):
        """Test that non-integer max_terms fails."""
        inputs = {"terms": "python", "max_terms": "abc"}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "max_terms" in result.errors

    def test_max_terms_out_of_range(self, trends_module):
        """Test that max_terms outside 1-5 fails."""
        inputs = {"terms": "python", "max_terms": 10}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "max_terms" in result.errors

    def test_invalid_timeframe(self, trends_module):
        """Test that invalid timeframe fails."""
        inputs = {"terms": "python", "timeframe": "invalid"}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "timeframe" in result.errors

    def test_invalid_region(self, trends_module):
        """Test that invalid region fails."""
        inputs = {"terms": "python", "region": "mars"}
        result = trends_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "region" in result.errors

    def test_single_term(self, trends_module):
        """Test that single term is valid."""
        inputs = {"terms": "python"}
        result = trends_module.validate_inputs(inputs)
        assert result.is_valid


class TestTrendsModuleExecution:
    """Test execution logic for Trends module."""

    @pytest.fixture
    def trends_module(self):
        return TrendsModule()

    @pytest.mark.asyncio
    async def test_execute_success(self, trends_module, mock_serpapi_trends_response):
        """Test successful trends search."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_serpapi_trends_response
            mock_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "terms": "python, javascript",
                "timeframe": "today 12-m",
                "region": "united_states"
            }

            result = await trends_module.execute(inputs)

            assert result.status == ModuleStatus.COMPLETED
            assert "Trend Summary" in result.data
            assert "Interest Over Time" in result.data
            assert "Interest By Region" in result.data
            assert not result.data["Trend Summary"].empty

    @pytest.mark.asyncio
    async def test_execute_no_data(self, trends_module):
        """Test execution when no trend data is found."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"interest_over_time": {"timeline_data": []}}
            mock_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "terms": "xyz123nonexistent",
                "timeframe": "today 12-m"
            }

            result = await trends_module.execute(inputs)

            # Should still complete but with empty or warning data
            assert result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]

    @pytest.mark.asyncio
    async def test_execute_api_failure(self, trends_module):
        """Test execution when API fails."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.raise_for_status.side_effect = Exception("API Error")

            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "terms": "python",
                "timeframe": "today 12-m"
            }

            result = await trends_module.execute(inputs)

            assert result.status == ModuleStatus.FAILED
            assert len(result.errors) > 0

    @pytest.mark.asyncio
    async def test_execute_single_term(self, trends_module, mock_serpapi_trends_response):
        """Test execution with single term."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_serpapi_trends_response
            mock_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()
            mock_client_instance.get.return_value = mock_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "terms": "python",
                "timeframe": "today 12-m"
            }

            result = await trends_module.execute(inputs)

            assert result.status == ModuleStatus.COMPLETED
            assert not result.data["Trend Summary"].empty

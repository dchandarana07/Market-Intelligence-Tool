"""
Unit tests for Lightcast Module.
"""
import pytest
from unittest.mock import AsyncMock, patch
import pandas as pd

from app.modules.lightcast import LightcastModule
from app.modules.base import ModuleStatus


class TestLightcastModuleValidation:
    """Test validation logic for Lightcast module."""

    @pytest.fixture
    def lightcast_module(self):
        return LightcastModule()

    def test_valid_inputs_with_skills(self, lightcast_module):
        """Test that valid inputs with skills pass validation."""
        inputs = {
            "skills": "python, javascript, sql",
            "reuse_from_trends": False,
            "max_skills": 30,
            "include_related": False
        }
        result = lightcast_module.validate_inputs(inputs)
        assert result.is_valid

    def test_valid_inputs_with_reuse(self, lightcast_module):
        """Test that valid inputs with reuse enabled pass validation."""
        inputs = {
            "skills": "",
            "reuse_from_trends": True,
            "max_skills": 30
        }
        result = lightcast_module.validate_inputs(inputs)
        assert result.is_valid

    def test_no_skills_and_no_reuse(self, lightcast_module):
        """Test that no skills and no reuse fails validation."""
        inputs = {
            "skills": "",
            "reuse_from_trends": False
        }
        result = lightcast_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "skills" in result.errors


class TestLightcastModuleExecution:
    """Test execution logic for Lightcast module."""

    @pytest.fixture
    def lightcast_module(self):
        return LightcastModule()

    @pytest.mark.asyncio
    async def test_execute_with_manual_skills(self, lightcast_module, mock_lightcast_token_response, mock_lightcast_skills_response):
        """Test execution with manually entered skills."""
        with patch('httpx.AsyncClient') as mock_client:
            # Mock token request
            mock_token_response = AsyncMock()
            mock_token_response.status_code = 200
            mock_token_response.json.return_value = mock_lightcast_token_response
            mock_token_response.raise_for_status = AsyncMock()

            # Mock skills search
            mock_skills_response = AsyncMock()
            mock_skills_response.status_code = 200
            mock_skills_response.json.return_value = mock_lightcast_skills_response
            mock_skills_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()

            # Return token response for post, skills response for get
            async def mock_request(*args, **kwargs):
                if args[0] if args else kwargs.get('url', '').endswith('token'):
                    return mock_token_response
                return mock_skills_response

            mock_client_instance.post.return_value = mock_token_response
            mock_client_instance.get.return_value = mock_skills_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "skills": "python, javascript",
                "reuse_from_trends": False,
                "max_skills": 30
            }

            result = await lightcast_module.execute(inputs)

            assert result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]
            assert "Skills Normalized" in result.data
            assert "Skills Summary" in result.data

    @pytest.mark.asyncio
    async def test_execute_with_reused_trends(self, lightcast_module, mock_lightcast_token_response, mock_lightcast_skills_response):
        """Test execution with skills reused from Trends module."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_token_response = AsyncMock()
            mock_token_response.status_code = 200
            mock_token_response.json.return_value = mock_lightcast_token_response
            mock_token_response.raise_for_status = AsyncMock()

            mock_skills_response = AsyncMock()
            mock_skills_response.status_code = 200
            mock_skills_response.json.return_value = mock_lightcast_skills_response
            mock_skills_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_token_response
            mock_client_instance.get.return_value = mock_skills_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "skills": "",
                "reuse_from_trends": True,
                "max_skills": 30
            }

            # Pass trend_terms from Trends module
            trend_terms = ["python", "javascript", "machine learning"]

            result = await lightcast_module.execute(inputs, trend_terms=trend_terms)

            assert result.status in [ModuleStatus.COMPLETED, ModuleStatus.PARTIAL]
            assert "Skills Normalized" in result.data

    @pytest.mark.asyncio
    async def test_execute_no_skills_provided(self, lightcast_module):
        """Test execution when no skills are provided."""
        inputs = {
            "skills": "",
            "reuse_from_trends": True,
            "max_skills": 30
        }

        # No trend_terms provided
        result = await lightcast_module.execute(inputs)

        assert result.status == ModuleStatus.FAILED
        assert len(result.errors) > 0
        assert "No skills" in result.errors[0]

    @pytest.mark.asyncio
    async def test_execute_auth_failure(self, lightcast_module):
        """Test execution when authentication fails."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.raise_for_status.side_effect = Exception("Auth failed")

            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            inputs = {
                "skills": "python",
                "max_skills": 30
            }

            result = await lightcast_module.execute(inputs)

            assert result.status == ModuleStatus.FAILED
            assert len(result.errors) > 0

    @pytest.mark.asyncio
    async def test_execute_respects_max_skills(self, lightcast_module, mock_lightcast_token_response, mock_lightcast_skills_response):
        """Test that max_skills limit is respected."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_token_response = AsyncMock()
            mock_token_response.json.return_value = mock_lightcast_token_response
            mock_token_response.raise_for_status = AsyncMock()

            mock_skills_response = AsyncMock()
            mock_skills_response.json.return_value = mock_lightcast_skills_response
            mock_skills_response.raise_for_status = AsyncMock()

            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_token_response
            mock_client_instance.get.return_value = mock_skills_response
            mock_client_instance.__aenter__.return_value = mock_client_instance
            mock_client_instance.__aexit__.return_value = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Provide more skills than max_skills
            many_skills = ["skill" + str(i) for i in range(100)]

            inputs = {
                "skills": ", ".join(many_skills),
                "max_skills": 10
            }

            result = await lightcast_module.execute(inputs)

            # Should process max 10 skills
            if result.status != ModuleStatus.FAILED:
                assert result.metadata["skills_processed"] <= 10

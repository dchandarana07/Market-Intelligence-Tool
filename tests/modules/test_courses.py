"""
Unit tests for Courses Module.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from app.modules.courses import CoursesModule
from app.modules.base import ModuleStatus


class TestCoursesModuleValidation:
    """Test validation logic for Courses module."""

    @pytest.fixture
    def courses_module(self):
        return CoursesModule()

    def test_valid_inputs(self, courses_module):
        """Test that valid inputs pass validation."""
        inputs = {
            "keywords": "machine learning",
            "max_results": 15,
            "sources": ["coursera", "edx"],
            "level": "all",
            "include_certificates": False
        }
        result = courses_module.validate_inputs(inputs)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_keywords(self, courses_module):
        """Test that missing keywords fails validation."""
        inputs = {"keywords": ""}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "keywords" in result.errors

    def test_keywords_too_short(self, courses_module):
        """Test that keywords less than 2 chars fails."""
        inputs = {"keywords": "a"}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "keywords" in result.errors

    def test_keywords_too_long(self, courses_module):
        """Test that keywords over 200 chars fails."""
        inputs = {"keywords": "a" * 201}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "keywords" in result.errors

    def test_max_results_not_integer(self, courses_module):
        """Test that non-integer max_results fails."""
        inputs = {"keywords": "python", "max_results": "abc"}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "max_results" in result.errors

    def test_max_results_out_of_range(self, courses_module):
        """Test that max_results outside 5-50 fails."""
        inputs = {"keywords": "python", "max_results": 100}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "max_results" in result.errors

    def test_empty_sources(self, courses_module):
        """Test that empty sources fails."""
        inputs = {"keywords": "python", "sources": []}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "sources" in result.errors

    def test_invalid_sources(self, courses_module):
        """Test that invalid source values fail."""
        inputs = {"keywords": "python", "sources": ["coursera", "udemy"]}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "sources" in result.errors
        assert "udemy" in result.errors["sources"][0]

    def test_invalid_level(self, courses_module):
        """Test that invalid level fails."""
        inputs = {"keywords": "python", "level": "expert"}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "level" in result.errors

    def test_invalid_boolean_field(self, courses_module):
        """Test that non-boolean value for boolean field fails."""
        inputs = {"keywords": "python", "include_certificates": "yes"}
        result = courses_module.validate_inputs(inputs)
        assert not result.is_valid
        assert "include_certificates" in result.errors


class TestCoursesModuleExecution:
    """Test execution logic for Courses module."""

    @pytest.fixture
    def courses_module(self):
        return CoursesModule()

    @pytest.mark.asyncio
    async def test_execute_success(self, courses_module, mock_courses_data):
        """Test successful course scraping."""
        with patch.object(courses_module, '_scrape_coursera', return_value=mock_courses_data[:1]), \
             patch.object(courses_module, '_scrape_edx', return_value=mock_courses_data[1:]):

            inputs = {
                "keywords": "machine learning",
                "max_results": 15,
                "sources": ["coursera", "edx"],
                "level": "all"
            }

            result = await courses_module.execute(inputs)

            assert result.status == ModuleStatus.COMPLETED
            assert "Courses" in result.data
            assert not result.data["Courses"].empty
            assert len(result.data["Courses"]) == 2

    @pytest.mark.asyncio
    async def test_execute_no_courses_found(self, courses_module):
        """Test execution when no courses are found."""
        with patch.object(courses_module, '_scrape_coursera', return_value=[]), \
             patch.object(courses_module, '_scrape_edx', return_value=[]):

            inputs = {
                "keywords": "nonexistent topic xyz123",
                "max_results": 15,
                "sources": ["coursera", "edx"]
            }

            result = await courses_module.execute(inputs)

            assert result.data["Courses"].empty

    @pytest.mark.asyncio
    async def test_execute_partial_failure(self, courses_module, mock_courses_data):
        """Test execution when one source fails."""
        with patch.object(courses_module, '_scrape_coursera', side_effect=Exception("Scraping failed")), \
             patch.object(courses_module, '_scrape_edx', return_value=mock_courses_data[1:]):

            inputs = {
                "keywords": "machine learning",
                "max_results": 15,
                "sources": ["coursera", "edx"]
            }

            result = await courses_module.execute(inputs)

            assert result.status == ModuleStatus.PARTIAL
            assert len(result.errors) > 0
            assert not result.data["Courses"].empty

    @pytest.mark.asyncio
    async def test_execute_all_sources_fail(self, courses_module):
        """Test execution when all sources fail."""
        with patch.object(courses_module, '_scrape_coursera', side_effect=Exception("Failed")), \
             patch.object(courses_module, '_scrape_edx', side_effect=Exception("Failed")):

            inputs = {
                "keywords": "machine learning",
                "max_results": 15,
                "sources": ["coursera", "edx"]
            }

            result = await courses_module.execute(inputs)

            assert result.status == ModuleStatus.FAILED
            assert len(result.errors) == 2

    def test_parse_coursera_card(self, courses_module):
        """Test parsing Coursera course card."""
        # Create mock card element
        mock_card = MagicMock()

        # Mock title element
        mock_title_elem = MagicMock()
        mock_title_elem.text = "Machine Learning Specialization"
        mock_title_elem.get_attribute.return_value = "https://coursera.org/ml"

        # Mock provider element
        mock_provider_elem = MagicMock()
        mock_provider_elem.text = "Stanford University"

        # Setup find_element behavior
        def find_element_side_effect(by, selector):
            if "titleLink" in selector:
                return mock_title_elem
            elif "partnerNames" in selector:
                return mock_provider_elem
            raise Exception("Not found")

        mock_card.find_element.side_effect = find_element_side_effect
        mock_card.find_elements.return_value = []

        course = courses_module._parse_coursera_card(mock_card)

        assert course is not None
        assert course["title"] == "Machine Learning Specialization"
        assert course["provider"] == "Stanford University"
        assert course["source"] == "Coursera"

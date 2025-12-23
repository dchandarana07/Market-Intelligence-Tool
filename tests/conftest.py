"""
Pytest configuration and shared fixtures.
"""
import pytest
from datetime import datetime
import pandas as pd


@pytest.fixture
def mock_jobs_api_response():
    """Mock response from SerpAPI Google Jobs."""
    return {
        "jobs_results": [
            {
                "title": "Software Engineer",
                "company_name": "Tech Corp",
                "location": "San Francisco, CA",
                "via": "LinkedIn",
                "description": "We are looking for a software engineer with experience in Python, JavaScript, and AWS. Strong problem solving skills required.",
                "detected_extensions": {
                    "posted_at": "2 days ago",
                    "schedule_type": "Full-time",
                    "salary": "$120,000 - $180,000"
                },
                "share_link": "https://example.com/job/1"
            },
            {
                "title": "Data Scientist",
                "company_name": "Data Inc",
                "location": "Remote",
                "via": "Indeed",
                "description": "Seeking data scientist with machine learning and data analysis expertise. Python, SQL required.",
                "detected_extensions": {
                    "posted_at": "1 week ago",
                    "schedule_type": "Full-time",
                    "salary": "$100K-$150K"
                },
                "share_link": "https://example.com/job/2"
            }
        ]
    }


@pytest.fixture
def mock_bls_api_response():
    """Mock response from BLS API."""
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "OEUN000000000001525201",
                    "data": [{"value": "1234567", "year": "2024", "period": "A01"}]
                },
                {
                    "seriesID": "OEUN000000000001525204",
                    "data": [{"value": "145000", "year": "2024", "period": "A01"}]
                },
                {
                    "seriesID": "OEUN000000000001525213",
                    "data": [{"value": "69.71", "year": "2024", "period": "A01"}]
                }
            ]
        }
    }


@pytest.fixture
def mock_serpapi_trends_response():
    """Mock response from SerpAPI Google Trends."""
    return {
        "interest_over_time": {
            "timeline_data": [
                {
                    "date": "Jan 1 - 7, 2024",
                    "timestamp": "1704067200",
                    "values": [
                        {"query": "python", "value": "100", "extracted_value": 100},
                        {"query": "javascript", "value": "85", "extracted_value": 85}
                    ]
                },
                {
                    "date": "Jan 8 - 14, 2024",
                    "timestamp": "1704672000",
                    "values": [
                        {"query": "python", "value": "95", "extracted_value": 95},
                        {"query": "javascript", "value": "88", "extracted_value": 88}
                    ]
                }
            ]
        },
        "interest_by_region": [
            {"location": "United States", "python": "100", "javascript": "85"},
            {"location": "United Kingdom", "python": "75", "javascript": "90"}
        ]
    }


@pytest.fixture
def mock_lightcast_token_response():
    """Mock response from Lightcast auth endpoint."""
    return {
        "access_token": "mock_token_12345",
        "expires_in": 3600,
        "token_type": "Bearer"
    }


@pytest.fixture
def mock_lightcast_skills_response():
    """Mock response from Lightcast skills search."""
    return {
        "data": [
            {
                "id": "KS120076FGP5WGWYMP0F",
                "name": "Python (Programming Language)",
                "type": {"name": "Hard Skill"},
                "category": {"name": "Software and Programming"},
                "subcategory": {"name": "Programming Languages"}
            }
        ]
    }


@pytest.fixture
def mock_courses_data():
    """Mock course data for testing."""
    return [
        {
            "source": "Coursera",
            "title": "Machine Learning Specialization",
            "provider": "Stanford University",
            "url": "https://www.coursera.org/specializations/machine-learning",
            "price": "$49/month",
            "duration": "3 months",
            "level": "Intermediate",
            "rating": "4.9",
            "enrollments": "500K+",
            "description": "Learn machine learning fundamentals",
            "skills": "Machine Learning, Python, Deep Learning",
            "has_certificate": "Yes",
            "last_updated": "2024-01"
        },
        {
            "source": "EdX",
            "title": "Data Science Professional Certificate",
            "provider": "Harvard University",
            "url": "https://www.edx.org/professional-certificate/harvardx-data-science",
            "price": "$99/month",
            "duration": "9 months",
            "level": "Beginner",
            "rating": "4.7",
            "enrollments": "300K+",
            "description": "Comprehensive data science program",
            "skills": "R, Data Analysis, Statistics",
            "has_certificate": "Yes",
            "last_updated": "2023-12"
        }
    ]


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        "col1": ["value1", "value2"],
        "col2": [100, 200],
        "col3": [1.5, 2.5]
    })

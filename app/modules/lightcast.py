"""
Lightcast Skills Module - Skills Normalization and Enrichment

Uses the Lightcast Open Skills API to normalize and enrich skills
extracted from job postings.

Free tier limits:
- 50 skill extractions per month
- 50 title normalizations per month
- 5 requests per second
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Optional
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

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


class LightcastModule(BaseModule):
    """
    Lightcast module for skills normalization and enrichment.

    Uses the Lightcast Open Skills API to:
    - Normalize raw skill strings to canonical names
    - Get skill metadata (type, category)
    - Find related skills
    """

    def __init__(self):
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0

    @property
    def name(self) -> str:
        return "lightcast"

    @property
    def display_name(self) -> str:
        return "Skills Enrichment (Lightcast)"

    @property
    def description(self) -> str:
        return (
            "Find Related Skills using Lightcast's Skills API. "
            "Discovers other skills that are often found together with your input skills."
        )

    @property
    def input_fields(self) -> list[InputField]:
        return [
            InputField(
                name="skills",
                label="Skills to Analyze",
                field_type="text",
                required=True,
                placeholder="e.g., Data Science, Data Analysis, Machine Learning",
                help_text="Enter skills to find related skills that are often found together",
            ),
            InputField(
                name="max_related",
                label="Max Related Skills to Show",
                field_type="number",
                required=False,
                default=10,
                min_value=5,
                max_value=20,
                help_text="Number of related skills to return (5-20)",
            ),
        ]

    @property
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        return {
            "Input Skills": [
                OutputColumn("skill_name", "Skill entered", "string"),
                OutputColumn("lightcast_id", "Lightcast skill ID", "string"),
                OutputColumn("skill_type", "Skill type", "string"),
                OutputColumn("category", "Category", "string"),
            ],
            "Related Skills": [
                OutputColumn("skill_name", "Related skill name", "string"),
                OutputColumn("skill_type", "Skill type", "string"),
                OutputColumn("category", "Category", "string"),
                OutputColumn("description", "What this skill involves", "string"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        skills = inputs.get("skills", "").strip()

        if not skills:
            result.add_error("skills", "Skills are required")
        elif len(skills) < 2:
            result.add_error("skills", "Skills must be at least 2 characters")

        max_related = inputs.get("max_related", 10)
        if not isinstance(max_related, int) or max_related < 5 or max_related > 20:
            result.add_error("max_related", "Max related skills must be between 5 and 20")

        return result

    def is_available(self) -> bool:
        return settings.lightcast_available

    def get_availability_message(self) -> Optional[str]:
        if not settings.lightcast_available:
            return (
                "Lightcast API credentials not configured. "
                "Apply for free access at https://lightcast.io/open-skills/access "
                "and add LIGHTCAST_CLIENT_ID and LIGHTCAST_CLIENT_SECRET to .env"
            )
        return None

    async def execute(
        self,
        inputs: dict[str, Any],
        job_skills: Optional[list[str]] = None,
        trend_terms: Optional[list[str]] = None,
    ) -> ModuleResult:
        """Execute the Lightcast Related Skills module."""
        logger.info("[Lightcast] Starting Lightcast Related Skills module")
        started_at = datetime.now()
        errors = []
        warnings = []

        skills_input = inputs.get("skills", "").strip()
        max_related = inputs.get("max_related", 10)

        # Parse input skills
        skills = [s.strip() for s in skills_input.split(",") if s.strip()]

        # Supplement with job skills if available and user input is sparse
        if job_skills and len(skills) < 3:
            # Add top job skills that aren't already in the user's list
            user_skills_lower = {s.lower() for s in skills}
            for js in job_skills:
                if js.lower() not in user_skills_lower and len(skills) < 8:
                    skills.append(js)
                    user_skills_lower.add(js.lower())
            logger.info(f"[Lightcast] Supplemented with job skills, total: {len(skills)}")

        logger.info(f"[Lightcast] Processing {len(skills)} input skills: {skills}")

        # Get access token
        try:
            logger.debug("[Lightcast] Obtaining access token...")
            await self._ensure_access_token()
            logger.info("[Lightcast] Access token obtained successfully")
        except Exception as e:
            logger.error(f"[Lightcast] Failed to authenticate: {type(e).__name__}: {e}")
            errors.append(f"Lightcast authentication failed: {str(e)}")
            return ModuleResult.failure(errors)

        # Step 1: Normalize input skills to get Lightcast IDs
        input_skills_data = []
        skill_ids = []

        for skill in skills:
            try:
                logger.debug(f"[Lightcast] Normalizing skill: '{skill}'")
                normalized = await self._normalize_skill(skill)
                if normalized:
                    input_skills_data.append({
                        "skill_name": normalized.get("canonical_name", skill),
                        "lightcast_id": normalized.get("lightcast_id", ""),
                        "skill_type": normalized.get("skill_type", ""),
                        "category": normalized.get("category", ""),
                    })
                    skill_ids.append(normalized.get("lightcast_id"))
                    logger.info(f"[Lightcast] Normalized '{skill}' -> '{normalized.get('canonical_name')}' (ID: {normalized.get('lightcast_id')})")
                else:
                    warnings.append(f"Could not find skill in Lightcast database: {skill}")
                    logger.warning(f"[Lightcast] No match found for '{skill}'")

                await asyncio.sleep(0.25)  # Rate limit

            except Exception as e:
                logger.warning(f"[Lightcast] Error normalizing skill '{skill}': {type(e).__name__}: {e}")
                warnings.append(f"Error processing: {skill}")

        if not skill_ids:
            errors.append("No valid skills found in Lightcast database. Try using more specific or standard skill names (e.g., 'Machine Learning' instead of 'Applied AI').")
            return ModuleResult.failure(errors)

        # Step 2: Get related skills
        logger.info(f"[Lightcast] Finding related skills for {len(skill_ids)} normalized skills")
        related_skills_data = []

        try:
            related_skills = await self._get_related_skills(skill_ids, max_related)
            logger.info(f"[Lightcast] Found {len(related_skills)} related skills")

            for skill_info in related_skills:
                # Handle both nested and flat category/type structures
                skill_type = skill_info.get("type", {})
                if isinstance(skill_type, dict):
                    skill_type_name = skill_type.get("name", "")
                else:
                    skill_type_name = str(skill_type) if skill_type else ""

                category = skill_info.get("category", {})
                if isinstance(category, dict):
                    category_name = category.get("name", "")
                else:
                    category_name = str(category) if category else ""

                description = skill_info.get("description", "")
                if description:
                    description = description[:200]

                related_skills_data.append({
                    "skill_name": skill_info.get("name", ""),
                    "skill_type": skill_type_name,
                    "category": category_name,
                    "description": description,
                })

        except Exception as e:
            logger.error(f"[Lightcast] Error fetching related skills: {type(e).__name__}: {e}")
            warnings.append(f"Could not fetch related skills: {str(e)}")

        # Create DataFrames
        input_df = pd.DataFrame(input_skills_data)
        related_df = pd.DataFrame(related_skills_data)

        data = {
            "Input Skills": input_df,
            "Related Skills": related_df,
        }

        completed_at = datetime.now()

        if errors:
            return ModuleResult.failure(errors)
        elif warnings:
            return ModuleResult(
                status=ModuleStatus.PARTIAL if not related_skills_data else ModuleStatus.COMPLETED,
                data=data,
                warnings=warnings,
                metadata={
                    "input_skills": len(input_skills_data),
                    "related_skills": len(related_skills_data),
                },
                started_at=started_at,
                completed_at=completed_at,
            )
        else:
            return ModuleResult.success(
                data=data,
                metadata={
                    "input_skills": len(input_skills_data),
                    "related_skills": len(related_skills_data),
                },
            )

    async def _ensure_access_token(self) -> str:
        """Ensure we have a valid access token."""
        current_time = time.time()

        # Check if we have a valid token
        if self._access_token and current_time < self._token_expires_at - 60:
            return self._access_token

        # Get new token
        logger.info("Obtaining Lightcast access token")

        auth_url = "https://auth.emsicloud.com/connect/token"
        payload = {
            "client_id": settings.lightcast_client_id,
            "client_secret": settings.lightcast_client_secret,
            "grant_type": "client_credentials",
            "scope": "emsi_open",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(auth_url, data=payload)
            response.raise_for_status()
            data = response.json()

        self._access_token = data["access_token"]
        self._token_expires_at = current_time + data.get("expires_in", 3600)

        logger.info("Lightcast access token obtained successfully")
        return self._access_token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=1, max=5),
    )
    async def _normalize_skill(self, skill: str) -> Optional[dict]:
        """Normalize a single skill using Lightcast API.

        If the first search returns no results, tries alternative queries:
        - Broader search with more results to find closest match
        - Individual words from compound terms
        """
        token = await self._ensure_access_token()

        search_url = "https://emsiservices.com/skills/versions/latest/skills"
        headers = {"Authorization": f"Bearer {token}"}

        # Try exact search first
        result = await self._search_skill(search_url, headers, skill, limit=1)
        if result:
            return result

        # Try with more results to find a close match
        result = await self._search_skill(search_url, headers, skill, limit=5)
        if result:
            return result

        # For compound terms like "Applied AI", try the most meaningful word
        words = skill.split()
        if len(words) > 1:
            # Try each word individually, prioritizing longer/more specific words
            for word in sorted(words, key=len, reverse=True):
                if len(word) >= 3:  # Skip very short words
                    result = await self._search_skill(search_url, headers, word, limit=1)
                    if result:
                        logger.info(f"[Lightcast] Matched '{skill}' via sub-term '{word}' -> '{result.get('canonical_name')}'")
                        return result

        return None

    async def _search_skill(self, url: str, headers: dict, query: str, limit: int) -> Optional[dict]:
        """Search for a skill in the Lightcast API."""
        params = {"q": query, "limit": limit}

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

        skills_data = data.get("data", [])
        if not skills_data:
            return None

        # Pick the best match
        skill_info = skills_data[0]

        # Get skill type
        skill_type_obj = skill_info.get("type", {})
        if isinstance(skill_type_obj, dict):
            skill_type = skill_type_obj.get("name", "Unknown")
        else:
            skill_type = str(skill_type_obj) if skill_type_obj else "Unknown"

        # Get category info
        category = ""
        category_info = skill_info.get("category", {})
        if isinstance(category_info, dict):
            category = category_info.get("name", "")
        elif category_info:
            category = str(category_info)

        return {
            "raw_skill": query,
            "lightcast_id": skill_info.get("id", ""),
            "canonical_name": skill_info.get("name", query),
            "skill_type": skill_type,
            "category": category,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=1, max=5),
    )
    async def _get_related_skills(self, skill_ids: list[str], limit: int = 10) -> list[dict]:
        """
        Get related skills using Lightcast API.

        Uses the skills/related endpoint to find skills that are often found together.
        """
        token = await self._ensure_access_token()

        # Use the related skills endpoint
        related_url = "https://emsiservices.com/skills/versions/latest/related"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Build request payload - send skill IDs to find related skills
        payload = {
            "ids": skill_ids,
            "limit": limit,
        }

        logger.debug(f"[Lightcast] Requesting related skills for IDs: {skill_ids}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(related_url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        related_skills = data.get("data", [])
        logger.info(f"[Lightcast] Retrieved {len(related_skills)} related skills")

        return related_skills

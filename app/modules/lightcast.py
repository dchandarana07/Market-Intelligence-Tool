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
            "Normalize and enrich skills using Lightcast's Skills API. "
            "Maps raw skill mentions to standardized taxonomy."
        )

    @property
    def input_fields(self) -> list[InputField]:
        return [
            InputField(
                name="skills",
                label="Skills to Normalize",
                field_type="text",
                required=False,  # Can be auto-populated from jobs
                placeholder="e.g., python, machine learning, project mgmt",
                help_text="Comma-separated skills. Leave empty to use skills from Jobs module.",
            ),
            InputField(
                name="auto_from_jobs",
                label="Auto-populate from Jobs",
                field_type="checkbox",
                required=False,
                default=True,
                help_text="Use skills extracted from job postings",
            ),
            InputField(
                name="max_skills",
                label="Max Skills to Process",
                field_type="number",
                required=False,
                default=30,
                min_value=5,
                max_value=50,
                help_text="Maximum number of skills to normalize (5-50). Free tier: 50/month total.",
            ),
            InputField(
                name="include_related",
                label="Include Related Skills",
                field_type="checkbox",
                required=False,
                default=False,
                is_advanced=True,
                help_text="Fetch related skills for each normalized skill (uses more API calls)",
            ),
        ]

    @property
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        return {
            "Skills Normalized": [
                OutputColumn("raw_skill", "Original skill text", "string"),
                OutputColumn("lightcast_id", "Lightcast skill ID", "string"),
                OutputColumn("canonical_name", "Standardized skill name", "string"),
                OutputColumn("skill_type", "Skill type (Hard/Soft/Certification)", "string"),
                OutputColumn("category", "Skill category", "string"),
                OutputColumn("subcategory", "Skill subcategory", "string"),
                OutputColumn("match_confidence", "Confidence of match", "string"),
            ],
            "Skills Summary": [
                OutputColumn("category", "Skill category", "string"),
                OutputColumn("count", "Number of skills in category", "number"),
                OutputColumn("skills_list", "Skills in this category", "string"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        skills = inputs.get("skills", "").strip()
        auto_from_jobs = inputs.get("auto_from_jobs", True)

        if not skills and not auto_from_jobs:
            result.add_error(
                "skills",
                "Either provide skills or enable 'Auto-populate from Jobs'"
            )

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
    ) -> ModuleResult:
        """Execute the Lightcast module."""
        logger.info("[Lightcast] Starting Lightcast module execution")
        started_at = datetime.now()
        errors = []
        warnings = []

        skills_input = inputs.get("skills", "").strip()
        auto_from_jobs = inputs.get("auto_from_jobs", True)
        max_skills = inputs.get("max_skills", 30)
        include_related = inputs.get("include_related", False)

        logger.debug(f"[Lightcast] Inputs: skills_input='{skills_input[:50] if skills_input else ''}', "
                    f"auto_from_jobs={auto_from_jobs}, max_skills={max_skills}")

        # Determine skills to process
        skills = []
        if skills_input:
            skills = [s.strip() for s in skills_input.split(",") if s.strip()]
            logger.info(f"[Lightcast] Using {len(skills)} skills from manual input")
        elif auto_from_jobs and job_skills:
            # Use unique skills from jobs
            skills = list(dict.fromkeys(job_skills))  # Preserve order, remove duplicates
            logger.info(f"[Lightcast] Using {len(skills)} skills from jobs module")

        if not skills:
            logger.error("[Lightcast] No skills to process")
            errors.append(
                "No skills provided and no job skills available. "
                "Either enter skills manually or run the Jobs module first."
            )
            return ModuleResult.failure(errors)

        # Limit to max_skills
        original_count = len(skills)
        skills = skills[:max_skills]
        if original_count > max_skills:
            logger.info(f"[Lightcast] Limited skills from {original_count} to {max_skills}")

        logger.info(f"[Lightcast] Processing {len(skills)} skills: {skills[:5]}{'...' if len(skills) > 5 else ''}")

        # Get access token
        try:
            logger.debug("[Lightcast] Obtaining access token...")
            await self._ensure_access_token()
            logger.info("[Lightcast] Access token obtained successfully")
        except Exception as e:
            logger.error(f"[Lightcast] Failed to authenticate: {type(e).__name__}: {e}")
            errors.append(f"Lightcast authentication failed: {str(e)}")
            return ModuleResult.failure(errors)

        # Normalize skills
        normalized_skills = []
        success_count = 0
        for i, skill in enumerate(skills):
            try:
                logger.debug(f"[Lightcast] Normalizing skill {i+1}/{len(skills)}: '{skill}'")
                result = await self._normalize_skill(skill)
                if result:
                    normalized_skills.append(result)
                    success_count += 1
                    logger.debug(f"[Lightcast] Normalized '{skill}' -> '{result.get('canonical_name')}'")
                else:
                    normalized_skills.append({
                        "raw_skill": skill,
                        "lightcast_id": "",
                        "canonical_name": skill,  # Use original if not found
                        "skill_type": "Unknown",
                        "category": "",
                        "subcategory": "",
                        "match_confidence": "Not Found",
                    })
                    logger.debug(f"[Lightcast] No match found for '{skill}'")
                # Rate limit: 5 requests/second
                await asyncio.sleep(0.25)

            except Exception as e:
                logger.warning(f"[Lightcast] Error normalizing skill '{skill}': {type(e).__name__}: {e}")
                warnings.append(f"Could not normalize: {skill}")
                normalized_skills.append({
                    "raw_skill": skill,
                    "lightcast_id": "",
                    "canonical_name": skill,
                    "skill_type": "Error",
                    "category": "",
                    "subcategory": "",
                    "match_confidence": "Error",
                })

        logger.info(f"[Lightcast] Normalized {success_count}/{len(skills)} skills successfully")

        # Create DataFrames
        skills_df = pd.DataFrame(normalized_skills)

        # Create category summary
        summary_rows = []
        if not skills_df.empty and "category" in skills_df.columns:
            category_groups = skills_df.groupby("category")
            for category, group in category_groups:
                if category:  # Skip empty categories
                    summary_rows.append({
                        "category": category,
                        "count": len(group),
                        "skills_list": ", ".join(group["canonical_name"].tolist()[:10]),
                    })

        summary_df = pd.DataFrame(summary_rows)

        data = {
            "Skills Normalized": skills_df,
            "Skills Summary": summary_df,
        }

        completed_at = datetime.now()

        if warnings:
            return ModuleResult(
                status=ModuleStatus.PARTIAL if len(warnings) > len(skills) / 2 else ModuleStatus.COMPLETED,
                data=data,
                warnings=warnings,
                metadata={
                    "skills_processed": len(skills),
                    "skills_normalized": len([s for s in normalized_skills if s.get("lightcast_id")]),
                },
                started_at=started_at,
                completed_at=completed_at,
            )

        return ModuleResult.success(
            data=data,
            metadata={
                "skills_processed": len(skills),
                "skills_normalized": len([s for s in normalized_skills if s.get("lightcast_id")]),
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
        """Normalize a single skill using Lightcast API."""
        token = await self._ensure_access_token()

        # Use the skills extraction endpoint for single skill lookup
        # Or the autocomplete endpoint for matching
        search_url = "https://emsiservices.com/skills/versions/latest/skills"

        params = {
            "q": skill,
            "limit": 1,
        }

        headers = {
            "Authorization": f"Bearer {token}",
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(search_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

        skills_data = data.get("data", [])

        if not skills_data:
            return None

        skill_info = skills_data[0]

        # Get skill type
        skill_type = skill_info.get("type", {}).get("name", "Unknown")

        # Get category info
        category = ""
        subcategory = ""
        category_info = skill_info.get("category", {})
        if category_info:
            category = category_info.get("name", "")
            subcategory_info = skill_info.get("subcategory", {})
            if subcategory_info:
                subcategory = subcategory_info.get("name", "")

        return {
            "raw_skill": skill,
            "lightcast_id": skill_info.get("id", ""),
            "canonical_name": skill_info.get("name", skill),
            "skill_type": skill_type,
            "category": category,
            "subcategory": subcategory,
            "match_confidence": "High" if skill.lower() == skill_info.get("name", "").lower() else "Partial",
        }

    async def extract_skills_from_text(self, text: str) -> list[dict]:
        """
        Extract skills from a text block using Lightcast extraction API.

        Note: This uses the skill extraction quota (50/month free).
        """
        token = await self._ensure_access_token()

        extract_url = "https://emsiservices.com/skills/versions/latest/extract"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "text": text,
            "confidenceThreshold": 0.6,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(extract_url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        extracted = []
        for skill in data.get("data", []):
            extracted.append({
                "lightcast_id": skill.get("skill", {}).get("id", ""),
                "canonical_name": skill.get("skill", {}).get("name", ""),
                "skill_type": skill.get("skill", {}).get("type", {}).get("name", ""),
                "confidence": skill.get("confidence", 0),
            })

        return extracted

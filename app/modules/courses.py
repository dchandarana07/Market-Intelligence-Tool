"""
Courses Module - Coursera and EdX Course Scraping

Scrapes course information from online learning platforms.
Uses requests + BeautifulSoup for all platforms (no Selenium/Chrome required).
"""

import asyncio
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Optional
import pandas as pd
import requests as sync_requests
from bs4 import BeautifulSoup

from app.modules.base import (
    BaseModule,
    InputField,
    OutputColumn,
    ValidationResult,
    ModuleResult,
    ModuleStatus,
)

logger = logging.getLogger(__name__)

# Thread pool for running synchronous scraping code
executor = ThreadPoolExecutor(max_workers=2)

# Shared browser user agent
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


class CoursesModule(BaseModule):
    """
    Courses module for scraping Coursera and EdX.

    Uses requests + BeautifulSoup for both platforms.
    EdX scraping is best-effort since EdX is JS-heavy.
    """

    @property
    def name(self) -> str:
        return "courses"

    @property
    def display_name(self) -> str:
        return "Online Courses"

    @property
    def description(self) -> str:
        return (
            "Search Coursera and EdX for relevant online courses, "
            "including pricing, ratings, and skill tags."
        )

    @property
    def input_fields(self) -> list[InputField]:
        return [
            InputField(
                name="keywords",
                label="Search Keywords",
                field_type="text",
                required=True,
                placeholder="e.g., machine learning, project management",
                help_text="Keywords to search for courses",
            ),
            InputField(
                name="max_results",
                label="Maximum Results Per Source",
                field_type="number",
                required=False,
                default=15,
                min_value=5,
                max_value=50,
                help_text="Number of courses to retrieve from each platform (5-50)",
            ),
            InputField(
                name="sources",
                label="Course Platforms",
                field_type="multiselect",
                required=False,
                default=["coursera", "edx"],
                options=[
                    {"value": "coursera", "label": "Coursera"},
                    {"value": "edx", "label": "EdX"},
                ],
                help_text="Select which platforms to search",
            ),
            InputField(
                name="level",
                label="Course Level",
                field_type="select",
                required=False,
                default="all",
                options=[
                    {"value": "all", "label": "All Levels"},
                    {"value": "beginner", "label": "Beginner"},
                    {"value": "intermediate", "label": "Intermediate"},
                    {"value": "advanced", "label": "Advanced"},
                ],
                is_advanced=True,
                help_text="Filter by difficulty level",
            ),
            InputField(
                name="include_certificates",
                label="Certificate Programs Only",
                field_type="checkbox",
                required=False,
                default=False,
                is_advanced=True,
                help_text="Only show courses that offer certificates",
            ),
        ]

    @property
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        return {
            "Courses": [
                OutputColumn("source", "Platform (Coursera/EdX)", "string"),
                OutputColumn("title", "Course title", "string"),
                OutputColumn("provider", "University or organization", "string"),
                OutputColumn("url", "Course URL", "url"),
                OutputColumn("type", "Course, Specialization, Certificate, etc.", "string"),
                OutputColumn("level", "Difficulty level", "string"),
                OutputColumn("duration", "Course duration", "string"),
                OutputColumn("rating", "Average rating", "string"),
                OutputColumn("reviews", "Number of reviews", "string"),
                OutputColumn("skills", "Skills/topics covered", "string"),
                OutputColumn("has_certificate", "Offers certificate", "string"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        keywords = inputs.get("keywords", "").strip()
        if not keywords:
            result.add_error("keywords", "Search keywords are required")
        elif len(keywords) < 2:
            result.add_error("keywords", "Search keywords must be at least 2 characters")
        elif len(keywords) > 200:
            result.add_error("keywords", "Search keywords must be less than 200 characters")

        max_results = inputs.get("max_results", 15)
        if not isinstance(max_results, int):
            result.add_error("max_results", "Maximum results must be a number")
        elif max_results < 5 or max_results > 50:
            result.add_error("max_results", "Maximum results must be between 5 and 50")

        sources = inputs.get("sources", [])
        if not sources:
            result.add_error("sources", "At least one course platform must be selected")
        elif not isinstance(sources, list):
            result.add_error("sources", "Course platforms must be a list")
        else:
            valid_sources = ["coursera", "edx"]
            invalid_sources = [s for s in sources if s not in valid_sources]
            if invalid_sources:
                result.add_error("sources", f"Invalid course platforms: {', '.join(invalid_sources)}. Must be 'coursera' or 'edx'")

        level = inputs.get("level", "all")
        valid_levels = ["all", "beginner", "intermediate", "advanced"]
        if level not in valid_levels:
            result.add_error("level", f"Course level must be one of: {', '.join(valid_levels)}")

        include_certificates = inputs.get("include_certificates", False)
        if not isinstance(include_certificates, bool):
            result.add_error("include_certificates", "Include certificates must be a checkbox value (true/false)")

        return result

    def is_available(self) -> bool:
        return True

    async def execute(self, inputs: dict[str, Any]) -> ModuleResult:
        """Execute the courses module."""
        started_at = datetime.now()
        errors = []
        warnings = []

        keywords = inputs.get("keywords", "")
        max_results = inputs.get("max_results", 15)
        sources = inputs.get("sources", ["coursera", "edx"])
        level = inputs.get("level", "all")

        all_courses = []

        for source in sources:
            try:
                if source == "coursera":
                    courses = await self._scrape_coursera(keywords, max_results, level)
                    all_courses.extend(courses)
                    logger.info(f"Retrieved {len(courses)} courses from Coursera")

                elif source == "edx":
                    courses = await self._scrape_edx(keywords, max_results, level)
                    all_courses.extend(courses)
                    logger.info(f"Retrieved {len(courses)} courses from EdX")

            except Exception as e:
                logger.error(f"Error scraping {source}: {e}")
                errors.append(f"Failed to scrape {source}: {str(e)}")

        if all_courses:
            df = pd.DataFrame(all_courses)
        else:
            df = pd.DataFrame()

        data = {"Courses": df}

        completed_at = datetime.now()

        if errors and df.empty:
            return ModuleResult(
                status=ModuleStatus.FAILED,
                errors=errors,
                started_at=started_at,
                completed_at=completed_at,
            )
        elif errors:
            return ModuleResult(
                status=ModuleStatus.PARTIAL,
                data=data,
                errors=errors,
                warnings=warnings,
                metadata={
                    "keywords": keywords,
                    "sources": sources,
                    "courses_found": len(df),
                },
                started_at=started_at,
                completed_at=completed_at,
            )
        else:
            return ModuleResult.success(
                data=data,
                metadata={
                    "keywords": keywords,
                    "sources": sources,
                    "courses_found": len(df),
                },
            )

    async def _scrape_coursera(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Scrape courses from Coursera using requests + BeautifulSoup."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            executor,
            self._scrape_coursera_sync,
            keywords,
            max_results,
            level,
        )

    def _scrape_coursera_sync(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Synchronous Coursera scraping using requests + BeautifulSoup."""
        courses = []

        try:
            logger.info(f"[Coursera] Scraping for: '{keywords}', max_results={max_results}, level={level}")

            search_url = f"https://www.coursera.org/search?query={keywords.replace(' ', '%20')}"
            if level != "all":
                search_url += f"&productDifficultyLevel={level.capitalize()}"

            logger.debug(f"[Coursera] Fetching: {search_url}")
            headers = {"User-Agent": USER_AGENT}
            response = sync_requests.get(search_url, headers=headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            cards = soup.select("li.cds-9")
            if not cards:
                cards = soup.select("[data-testid='product-card-cds']")
            if not cards:
                cards = soup.select(".cds-ProductCard-base")

            logger.info(f"[Coursera] Found {len(cards)} course cards")

            if not cards:
                logger.warning("[Coursera] No course cards found in page")
                return courses

            cards = cards[:max_results]

            for i, card in enumerate(cards):
                try:
                    course = self._parse_coursera_card_bs4(card)
                    if course:
                        courses.append(course)
                        logger.debug(f"[Coursera] Parsed course {i+1}: {course.get('title', 'Unknown')[:50]}")
                except Exception as e:
                    logger.warning(f"[Coursera] Error parsing card {i+1}: {e}")
                    continue

            logger.info(f"[Coursera] Successfully scraped {len(courses)} courses")

        except Exception as e:
            logger.error(f"[Coursera] Error scraping: {type(e).__name__}: {e}")
            raise

        return courses

    def _parse_coursera_card_bs4(self, card) -> Optional[dict]:
        """Parse a single Coursera course card using BeautifulSoup."""
        try:
            # Title and URL
            title_elem = card.select_one("a.cds-CommonCard-titleLink")
            if not title_elem:
                title_elem = card.select_one("a[data-click-key*='search_card']")
            if not title_elem:
                return None

            title = title_elem.get_text(strip=True)
            href = title_elem.get("href", "")
            url = f"https://www.coursera.org{href}" if href and not href.startswith("http") else href

            if not title:
                return None

            # Provider/Institution
            provider = ""
            provider_elem = card.select_one("p.cds-ProductCard-partnerNames")
            if provider_elem:
                provider = provider_elem.get_text(strip=True)

            # Rating
            rating = ""
            rating_elem = card.select_one("[aria-valuenow]")
            if rating_elem:
                rating = rating_elem.get("aria-valuenow", "")

            # Reviews count
            reviews = ""
            review_texts = card.find_all(string=re.compile(r'\d+[KMkm]?\s*review', re.IGNORECASE))
            if review_texts:
                match = re.search(r'([\d,.]+[KMkm]?)\s*review', review_texts[0], re.IGNORECASE)
                if match:
                    reviews = match.group(1)
            else:
                all_text = card.get_text()
                review_match = re.search(r'(\d[\d,.]*[KMkm]?)\s*(?:reviews?|ratings?)', all_text, re.IGNORECASE)
                if review_match:
                    reviews = review_match.group(1)

            # Metadata: level, type, duration
            course_level = ""
            course_type = ""
            duration = ""
            meta_elem = card.select_one("div.cds-CommonCard-metadata p")
            if meta_elem:
                meta_text = meta_elem.get_text(strip=True)
                # Clean encoding artifacts: replace middle dots, non-breaking spaces, etc.
                meta_text = re.sub(r'[\u00b7\u2022\u2024\u00a0\u200b]', '·', meta_text)
                meta_text = re.sub(r'[^\x20-\x7E·]', '', meta_text)  # Remove non-ASCII except middle dot
                parts = [p.strip() for p in meta_text.split("·") if p.strip()]
                for part in parts:
                    part_lower = part.lower()
                    if any(lvl in part_lower for lvl in ["beginner", "intermediate", "advanced", "mixed"]):
                        course_level = part.strip()
                    elif any(t in part_lower for t in ["course", "specialization", "certificate", "guided project"]):
                        course_type = part.strip()
                    elif any(t in part_lower for t in ["week", "month", "hour"]):
                        duration = part.strip()

            # Skills
            skills = ""
            skills_elem = card.select_one("p.cds-ProductCard-skills")
            if skills_elem:
                skills = skills_elem.get_text(strip=True)
            else:
                skills_elem = card.select_one("div.cds-ProductCard-body p")
                if skills_elem:
                    text = skills_elem.get_text(strip=True)
                    if "skills" in text.lower() or ":" in text:
                        skills = text

            return {
                "source": "Coursera",
                "title": title,
                "provider": provider,
                "url": url,
                "type": course_type,
                "level": course_level,
                "duration": duration,
                "rating": rating,
                "reviews": reviews,
                "skills": skills,
                "has_certificate": "Yes" if course_type.lower() in ["professional certificate", "specialization", "certificate"] else "Yes",
            }

        except Exception as e:
            logger.warning(f"Failed to parse Coursera card: {e}")
            return None

    async def _scrape_edx(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Scrape courses from EdX using requests + BeautifulSoup.

        EdX is JavaScript-heavy so BS4 scraping is best-effort.
        Falls back to EdX search API if available.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            executor,
            self._scrape_edx_sync,
            keywords,
            max_results,
            level,
        )

    def _scrape_edx_sync(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Synchronous EdX scraping using requests + BeautifulSoup.

        Tries the EdX search/discovery API first, falls back to HTML scraping.
        """
        courses = []

        try:
            logger.info(f"[EdX] Scraping for: '{keywords}', max_results={max_results}, level={level}")

            # Try EdX search API (public, no auth needed)
            courses = self._scrape_edx_api(keywords, max_results, level)
            if courses:
                logger.info(f"[EdX] Got {len(courses)} courses from API")
                return courses

            # Fallback: try HTML scraping
            logger.info("[EdX] API returned no results, trying HTML scraping")
            courses = self._scrape_edx_html(keywords, max_results, level)

            logger.info(f"[EdX] Successfully scraped {len(courses)} courses")

        except Exception as e:
            logger.error(f"[EdX] Error scraping: {type(e).__name__}: {e}")
            raise

        return courses

    def _scrape_edx_api(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Try EdX's public search/discovery API."""
        courses = []

        try:
            # EdX has a public search endpoint
            api_url = "https://www.edx.org/api/v1/catalog/search"
            params = {
                "q": keywords,
                "page_size": min(max_results, 40),
            }
            if level != "all":
                level_map = {
                    "beginner": "Introductory",
                    "intermediate": "Intermediate",
                    "advanced": "Advanced",
                }
                if level in level_map:
                    params["level"] = level_map[level]

            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            }

            response = sync_requests.get(api_url, params=params, headers=headers, timeout=15)

            if response.status_code != 200:
                logger.debug(f"[EdX] API returned status {response.status_code}")
                return courses

            data = response.json()

            results = data.get("objects", {}).get("results", [])
            if not results:
                results = data.get("results", [])

            for item in results[:max_results]:
                title = item.get("title", "")
                if not title:
                    continue

                org = ""
                orgs = item.get("org", "") or item.get("organizations", [])
                if isinstance(orgs, list) and orgs:
                    org = orgs[0] if isinstance(orgs[0], str) else orgs[0].get("name", "")
                elif isinstance(orgs, str):
                    org = orgs

                url = item.get("marketing_url", "") or item.get("url", "")
                if url and not url.startswith("http"):
                    url = f"https://www.edx.org{url}"

                course_type = item.get("type", "") or item.get("content_type", "")
                course_level = item.get("level", "") or item.get("level_type", "")

                courses.append({
                    "source": "EdX",
                    "title": title,
                    "provider": org,
                    "url": url,
                    "type": course_type,
                    "level": course_level,
                    "duration": item.get("weeks_to_complete", "") or item.get("length", ""),
                    "rating": "",
                    "reviews": "",
                    "skills": ", ".join(item.get("skill_names", [])[:5]) if item.get("skill_names") else "",
                    "has_certificate": "Yes",
                })

        except Exception as e:
            logger.debug(f"[EdX] API scraping failed: {e}")

        return courses

    def _scrape_edx_html(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Fallback HTML scraping for EdX (best-effort, JS-heavy site)."""
        courses = []

        try:
            search_url = f"https://www.edx.org/search?q={keywords.replace(' ', '%20')}"
            if level != "all":
                level_map = {
                    "beginner": "Introductory",
                    "intermediate": "Intermediate",
                    "advanced": "Advanced",
                }
                if level in level_map:
                    search_url += f"&level={level_map[level]}"

            headers = {"User-Agent": USER_AGENT}
            response = sync_requests.get(search_url, headers=headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Try multiple selectors for course cards
            cards = []
            selectors = [
                "[data-testid='product-card']",
                ".discovery-card",
                ".pgn__card",
                "div[class*='card']",
            ]

            for selector in selectors:
                cards = soup.select(selector)
                if cards:
                    logger.debug(f"[EdX] Found {len(cards)} cards with selector: {selector}")
                    break

            if not cards:
                logger.warning("[EdX] No course cards found in HTML (site is JS-rendered)")
                return courses

            cards = cards[:max_results]

            for i, card in enumerate(cards):
                try:
                    course = self._parse_edx_card_bs4(card)
                    if course:
                        courses.append(course)
                except Exception as e:
                    logger.warning(f"[EdX] Error parsing card {i+1}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"[EdX] HTML scraping failed: {e}")

        return courses

    def _parse_edx_card_bs4(self, card) -> Optional[dict]:
        """Parse a single EdX course card using BeautifulSoup."""
        try:
            # Title and URL
            title = ""
            url = ""

            title_selectors = ["h3 a", "a.course-card-title", "a[data-testid]"]
            for selector in title_selectors:
                elem = card.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    url = elem.get("href", "")
                    if title:
                        break

            if not title:
                h3 = card.select_one("h3")
                if h3:
                    title = h3.get_text(strip=True)

            if not title:
                return None

            if url and not url.startswith("http"):
                url = f"https://www.edx.org{url}"

            # Provider
            provider = ""
            provider_selectors = [".partner-image-cap", "[data-testid='partner-name']"]
            for selector in provider_selectors:
                elem = card.select_one(selector)
                if elem:
                    provider = elem.get_text(strip=True) or elem.get("alt", "")
                    if provider:
                        break

            return {
                "source": "EdX",
                "title": title,
                "provider": provider,
                "url": url,
                "type": "",
                "level": "",
                "duration": "",
                "rating": "",
                "reviews": "",
                "skills": "",
                "has_certificate": "Yes",
            }

        except Exception as e:
            logger.warning(f"Failed to parse EdX card: {e}")
            return None

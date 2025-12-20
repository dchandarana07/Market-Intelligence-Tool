"""
Courses Module - Coursera and EdX Course Scraping

Scrapes course information from online learning platforms.
Uses Selenium for dynamic content rendering.

Note: This module runs Selenium synchronously in a thread pool
since Selenium is not natively async.
"""

import asyncio
import logging
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

from app.modules.base import (
    BaseModule,
    InputField,
    OutputColumn,
    ValidationResult,
    ModuleResult,
    ModuleStatus,
)

logger = logging.getLogger(__name__)

# Thread pool for running Selenium (synchronous) code
executor = ThreadPoolExecutor(max_workers=2)


class CoursesModule(BaseModule):
    """
    Courses module for scraping Coursera and EdX.

    Uses Selenium to handle dynamic JavaScript content.
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
                OutputColumn("price", "Price or Free", "string"),
                OutputColumn("duration", "Course duration", "string"),
                OutputColumn("level", "Difficulty level", "string"),
                OutputColumn("rating", "Average rating", "string"),
                OutputColumn("enrollments", "Number of enrollments", "string"),
                OutputColumn("description", "Course description snippet", "string"),
                OutputColumn("skills", "Skills/topics covered", "string"),
                OutputColumn("has_certificate", "Offers certificate", "string"),
                OutputColumn("last_updated", "When course was updated", "string"),
            ],
        }

    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        result = ValidationResult.success()

        keywords = inputs.get("keywords", "").strip()
        if not keywords:
            result.add_error("keywords", "Search keywords are required")
        elif len(keywords) < 2:
            result.add_error("keywords", "Search keywords must be at least 2 characters")

        max_results = inputs.get("max_results", 15)
        if not isinstance(max_results, int) or max_results < 5 or max_results > 50:
            result.add_error("max_results", "Maximum results must be between 5 and 50")

        sources = inputs.get("sources", [])
        if not sources:
            result.add_error("sources", "At least one course platform must be selected")

        return result

    def is_available(self) -> bool:
        # Courses module is always available (uses Selenium)
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

        # Scrape each selected source
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

        # Create DataFrame
        if all_courses:
            df = pd.DataFrame(all_courses)
        else:
            df = pd.DataFrame()

        data = {"Courses": df}

        # Determine status
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

    def _create_driver(self) -> webdriver.Chrome:
        """Create a Chrome WebDriver with appropriate options."""
        logger.debug("[Courses] Setting up Chrome options...")
        options = Options()
        options.add_argument("--headless=new")  # Modern headless mode
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Try to create driver with retries
        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                logger.debug(f"[Courses] ChromeDriver install attempt {attempt + 1}/{max_retries}")

                # Install ChromeDriver
                driver_path = ChromeDriverManager().install()
                logger.debug(f"[Courses] ChromeDriverManager returned path: {driver_path}")

                # Validate the driver path - sometimes it returns wrong file
                driver_path_obj = Path(driver_path)
                if not driver_path_obj.exists():
                    raise FileNotFoundError(f"ChromeDriver not found at: {driver_path}")

                # Check if it's the actual binary or a notices file
                if "THIRD_PARTY_NOTICES" in driver_path or "LICENSE" in driver_path:
                    logger.warning(f"[Courses] ChromeDriverManager returned non-binary file: {driver_path}")
                    # Try to find the actual binary in the same directory
                    parent_dir = driver_path_obj.parent
                    possible_names = ["chromedriver", "chromedriver.exe", "chromedriver-mac-arm64", "chromedriver-mac-x64"]

                    actual_driver = None
                    for name in possible_names:
                        candidate = parent_dir / name
                        if candidate.exists() and candidate.is_file():
                            # Check file size (actual binary should be > 1MB)
                            if candidate.stat().st_size > 1_000_000:
                                # Make it executable if not already
                                if not os.access(str(candidate), os.X_OK):
                                    logger.info(f"[Courses] Making {candidate} executable")
                                    os.chmod(str(candidate), 0o755)
                                actual_driver = str(candidate)
                                logger.info(f"[Courses] Found actual ChromeDriver binary: {actual_driver}")
                                break

                    if actual_driver:
                        driver_path = actual_driver
                    else:
                        # Clear the cache and retry
                        logger.warning("[Courses] Could not find ChromeDriver binary, clearing cache...")
                        cache_dir = Path.home() / ".wdm"
                        if cache_dir.exists():
                            logger.debug(f"[Courses] Clearing WebDriverManager cache at: {cache_dir}")
                            try:
                                shutil.rmtree(cache_dir)
                            except Exception as e:
                                logger.warning(f"[Courses] Failed to clear cache: {e}")
                        continue  # Retry

                logger.debug(f"[Courses] Using ChromeDriver path: {driver_path}")
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=options)
                logger.info("[Courses] Chrome WebDriver created successfully")
                return driver

            except WebDriverException as e:
                last_error = e
                logger.warning(f"[Courses] WebDriverException on attempt {attempt + 1}: {e}")
                # Clear cache and retry
                cache_dir = Path.home() / ".wdm"
                if cache_dir.exists():
                    try:
                        shutil.rmtree(cache_dir)
                        logger.debug("[Courses] Cleared WebDriverManager cache for retry")
                    except Exception:
                        pass

            except Exception as e:
                last_error = e
                logger.warning(f"[Courses] Error on attempt {attempt + 1}: {type(e).__name__}: {e}")

        # All retries failed
        error_msg = f"Failed to create Chrome WebDriver after {max_retries} attempts: {last_error}"
        logger.error(f"[Courses] {error_msg}")
        raise RuntimeError(error_msg)

    async def _scrape_coursera(
        self,
        keywords: str,
        max_results: int,
        level: str,
    ) -> list[dict]:
        """Scrape courses from Coursera."""
        # Run Selenium in thread pool (it's synchronous)
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
        """Synchronous Coursera scraping (runs in thread pool)."""
        driver = None
        courses = []

        try:
            logger.info(f"[Coursera] Creating Chrome WebDriver...")
            driver = self._create_driver()
            logger.info(f"[Coursera] WebDriver created successfully")
            logger.info(f"[Coursera] Scraping for: '{keywords}', max_results={max_results}, level={level}")

            # Navigate to Coursera search
            search_url = f"https://www.coursera.org/search?query={keywords.replace(' ', '%20')}"
            if level != "all":
                search_url += f"&productDifficultyLevel={level.capitalize()}"

            logger.debug(f"[Coursera] Navigating to: {search_url}")
            driver.get(search_url)

            # Wait for results to load
            logger.debug("[Coursera] Waiting for search results to load...")
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='search-results']"))
                )
                logger.debug("[Coursera] Search results container found")
            except TimeoutException:
                logger.warning("[Coursera] Primary selector not found, trying alternative...")
                # Try alternative selector
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul li"))
                )

            # Scroll to load more results
            scroll_count = min(max_results // 10 + 1, 5)
            logger.debug(f"[Coursera] Scrolling {scroll_count} times to load more results")
            for i in range(scroll_count):
                driver.execute_script("window.scrollBy(0, 1000)")
                time.sleep(1)

            # Find course cards - try multiple selectors
            course_cards = []
            selectors = ["li.cds-9", "[data-testid='search-card']", ".rc-CardList li"]
            for selector in selectors:
                course_cards = driver.find_elements(By.CSS_SELECTOR, selector)
                if course_cards:
                    logger.debug(f"[Coursera] Found {len(course_cards)} cards with selector: {selector}")
                    break

            if not course_cards:
                logger.warning("[Coursera] No course cards found with any selector")
                # Log page source for debugging
                logger.debug(f"[Coursera] Page title: {driver.title}")
                return courses

            course_cards = course_cards[:max_results]
            logger.info(f"[Coursera] Processing {len(course_cards)} course cards")

            for i, card in enumerate(course_cards):
                try:
                    course = self._parse_coursera_card(card)
                    if course:
                        courses.append(course)
                        logger.debug(f"[Coursera] Parsed course {i+1}: {course.get('title', 'Unknown')[:50]}")
                except Exception as e:
                    logger.warning(f"[Coursera] Error parsing card {i+1}: {e}")
                    continue

            logger.info(f"[Coursera] Successfully scraped {len(courses)} courses")

        except TimeoutException:
            logger.error("[Coursera] Timeout waiting for search results - page may have changed structure")
        except Exception as e:
            logger.error(f"[Coursera] Error scraping: {type(e).__name__}: {e}")
            import traceback
            logger.debug(f"[Coursera] Traceback: {traceback.format_exc()}")
            raise
        finally:
            if driver:
                logger.debug("[Coursera] Closing WebDriver")
                driver.quit()

        return courses

    def _parse_coursera_card(self, card) -> Optional[dict]:
        """Parse a single Coursera course card."""
        try:
            # Title and URL
            title_elem = card.find_element(By.CSS_SELECTOR, "a.cds-CommonCard-titleLink")
            title = title_elem.text.strip()
            url = title_elem.get_attribute("href")

            if not title or not url:
                return None

            # Provider/Institution
            provider = "Unknown"
            try:
                provider_elem = card.find_element(By.CSS_SELECTOR, "p.cds-ProductCard-partnerNames")
                provider = provider_elem.text.strip()
            except NoSuchElementException:
                pass

            # Rating
            rating = ""
            try:
                rating_elem = card.find_element(By.CSS_SELECTOR, "span.css-6ecy9b")
                rating = rating_elem.text.strip()
            except NoSuchElementException:
                pass

            # Level and duration from metadata
            level = ""
            duration = ""
            try:
                meta_items = card.find_elements(By.CSS_SELECTOR, "div.cds-CommonCard-metadata span")
                for item in meta_items:
                    text = item.text.strip().lower()
                    if any(l in text for l in ["beginner", "intermediate", "advanced", "mixed"]):
                        level = text.capitalize()
                    elif "hour" in text or "week" in text or "month" in text:
                        duration = item.text.strip()
            except NoSuchElementException:
                pass

            # Skills
            skills = ""
            try:
                skills_elem = card.find_element(By.CSS_SELECTOR, "p.cds-ProductCard-skills")
                skills = skills_elem.text.strip()
            except NoSuchElementException:
                pass

            return {
                "source": "Coursera",
                "title": title,
                "provider": provider,
                "url": url,
                "price": "",  # Not easily available on search page
                "duration": duration,
                "level": level,
                "rating": rating,
                "enrollments": "",  # Not on search page
                "description": "",  # Need to visit course page
                "skills": skills,
                "has_certificate": "Yes",  # Coursera generally offers certificates
                "last_updated": "",
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
        """Scrape courses from EdX."""
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
        """Synchronous EdX scraping (runs in thread pool)."""
        driver = None
        courses = []

        try:
            logger.info(f"[EdX] Creating Chrome WebDriver...")
            driver = self._create_driver()
            logger.info(f"[EdX] WebDriver created successfully")
            logger.info(f"[EdX] Scraping for: '{keywords}', max_results={max_results}, level={level}")

            # Navigate to EdX search
            search_url = f"https://www.edx.org/search?q={keywords.replace(' ', '%20')}"
            if level != "all":
                level_map = {
                    "beginner": "Introductory",
                    "intermediate": "Intermediate",
                    "advanced": "Advanced",
                }
                if level in level_map:
                    search_url += f"&level={level_map[level]}"

            logger.debug(f"[EdX] Navigating to: {search_url}")
            driver.get(search_url)

            # Wait for results to load
            logger.debug("[EdX] Waiting for page to load (JavaScript-heavy)...")
            time.sleep(3)  # EdX is JavaScript-heavy

            # Try to wait for course cards
            card_found = False
            selectors_to_try = [
                "[data-testid='product-card']",
                ".discovery-card",
                ".pgn__card",
                "[class*='card']",
            ]

            for selector in selectors_to_try:
                try:
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.debug(f"[EdX] Found elements with selector: {selector}")
                    card_found = True
                    break
                except TimeoutException:
                    continue

            if not card_found:
                logger.warning("[EdX] Timeout waiting for search results - no cards found")
                logger.debug(f"[EdX] Page title: {driver.title}")
                return courses

            # Scroll to load more results
            scroll_count = min(max_results // 10 + 1, 5)
            logger.debug(f"[EdX] Scrolling {scroll_count} times to load more results")
            for _ in range(scroll_count):
                driver.execute_script("window.scrollBy(0, 1000)")
                time.sleep(1.5)

            # Try multiple selectors for course cards
            course_cards = []
            for selector in selectors_to_try:
                course_cards = driver.find_elements(By.CSS_SELECTOR, selector)
                if course_cards:
                    logger.debug(f"[EdX] Found {len(course_cards)} cards with selector: {selector}")
                    break

            if not course_cards:
                logger.warning("[EdX] No course cards found")
                return courses

            course_cards = course_cards[:max_results]
            logger.info(f"[EdX] Processing {len(course_cards)} course cards")

            for i, card in enumerate(course_cards):
                try:
                    course = self._parse_edx_card(card)
                    if course:
                        courses.append(course)
                        logger.debug(f"[EdX] Parsed course {i+1}: {course.get('title', 'Unknown')[:50]}")
                except Exception as e:
                    logger.warning(f"[EdX] Error parsing card {i+1}: {e}")
                    continue

            logger.info(f"[EdX] Successfully scraped {len(courses)} courses")

        except Exception as e:
            logger.error(f"[EdX] Error scraping: {type(e).__name__}: {e}")
            import traceback
            logger.debug(f"[EdX] Traceback: {traceback.format_exc()}")
            raise
        finally:
            if driver:
                logger.debug("[EdX] Closing WebDriver")
                driver.quit()

        return courses

    def _parse_edx_card(self, card) -> Optional[dict]:
        """Parse a single EdX course card."""
        try:
            # Title and URL
            title = ""
            url = ""

            # Try different selectors for title
            title_selectors = [
                "h3 a",
                ".discovery-card-title a",
                "[data-testid='product-card-title'] a",
                "a.course-card-title",
            ]

            for selector in title_selectors:
                try:
                    elem = card.find_element(By.CSS_SELECTOR, selector)
                    title = elem.text.strip()
                    url = elem.get_attribute("href")
                    if title:
                        break
                except NoSuchElementException:
                    continue

            if not title:
                # Try just getting any title-like element
                try:
                    elem = card.find_element(By.CSS_SELECTOR, "h3")
                    title = elem.text.strip()
                except NoSuchElementException:
                    return None

            if not url:
                try:
                    elem = card.find_element(By.CSS_SELECTOR, "a")
                    url = elem.get_attribute("href")
                except NoSuchElementException:
                    pass

            # Provider
            provider = ""
            provider_selectors = [
                ".partner-image-cap",
                "[data-testid='partner-name']",
                ".discovery-card-partner",
            ]
            for selector in provider_selectors:
                try:
                    elem = card.find_element(By.CSS_SELECTOR, selector)
                    provider = elem.text.strip() or elem.get_attribute("alt") or ""
                    if provider:
                        break
                except NoSuchElementException:
                    continue

            # Price
            price = ""
            try:
                price_elem = card.find_element(By.CSS_SELECTOR, "[data-testid='product-card-price']")
                price = price_elem.text.strip()
            except NoSuchElementException:
                try:
                    price_elem = card.find_element(By.XPATH, ".//*[contains(text(), '$') or contains(text(), 'Free')]")
                    price = price_elem.text.strip()
                except NoSuchElementException:
                    pass

            return {
                "source": "EdX",
                "title": title,
                "provider": provider,
                "url": url if url and url.startswith("http") else f"https://www.edx.org{url}" if url else "",
                "price": price,
                "duration": "",
                "level": "",
                "rating": "",
                "enrollments": "",
                "description": "",
                "skills": "",
                "has_certificate": "Yes",  # EdX generally offers verified certificates
                "last_updated": "",
            }

        except Exception as e:
            logger.warning(f"Failed to parse EdX card: {e}")
            return None

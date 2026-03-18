"""
Market Intelligence Tool - FastAPI Application

Simplified 3-page flow:
  1. Login (Google OAuth)
  2. Configure & Run (single page with topic + module selection)
  3. Progress + Results (live execution with inline results)
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import secrets

from config.settings import settings
from app.services.orchestrator import get_orchestrator, PipelineStatus
from app.services.email import get_email_service
from app.services.google_sheets import get_sheets_service
from app.services.auth import get_auth_service
from app.middleware.auth import AuthMiddleware

# Configure logging
LOG_LEVEL = logging.DEBUG if settings.debug else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.getLogger("app").setLevel(logging.DEBUG if settings.debug else logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("multipart").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Application paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Market Intelligence Tool")
    # Validate Google credentials at startup
    try:
        sheets = get_sheets_service()
        if sheets.is_available():
            email = sheets.get_service_account_email()
            logger.info(f"Google Sheets service OK. Service account: {email}")
        else:
            logger.warning("Google Sheets service not configured")
    except Exception as e:
        logger.error(f"Google Sheets startup check FAILED: {e}")
    yield
    logger.info("Shutting down Market Intelligence Tool")


# Create FastAPI app
app = FastAPI(
    title="Market Intelligence Tool",
    description="ASU Learning Enterprise Market Intelligence Tool",
    version="2.0.0",
    lifespan=lifespan,
)

# Mount static files FIRST (before middleware)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Setup templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Add authentication middleware (must be added BEFORE SessionMiddleware)
app.add_middleware(AuthMiddleware)

# Add session middleware
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    max_age=3600,
    https_only=settings.is_production,
)

# In production, ensure OAuth callbacks use HTTPS scheme behind reverse proxy
if settings.is_production:
    @app.middleware("http")
    async def force_https_scheme(request: Request, call_next):
        """Fix scheme for requests behind HTTPS reverse proxy (Render, etc.)."""
        if request.headers.get("x-forwarded-proto") == "https":
            request.scope["scheme"] = "https"
        return await call_next(request)

# In-memory storage for active runs
active_runs: dict[str, dict] = {}


# ============================================================================
# Authentication Routes
# ============================================================================

@app.get("/auth/login", response_class=HTMLResponse)
async def auth_login(request: Request, error: str = None):
    """Google OAuth login page."""
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": error},
    )


@app.get("/auth/callback-init")
async def auth_callback_init(request: Request):
    """Initialize OAuth flow and redirect to Google."""
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    auth_service = get_auth_service()
    redirect_uri = str(request.url_for("auth_callback"))

    authorization_url, code_verifier = auth_service.get_authorization_url(
        redirect_uri=redirect_uri,
        state=state,
    )

    # Store PKCE code_verifier in session for the callback
    request.session["oauth_code_verifier"] = code_verifier

    return RedirectResponse(url=authorization_url)


@app.get("/auth/callback")
async def auth_callback(request: Request):
    """Handle OAuth callback from Google."""
    try:
        state = request.query_params.get("state")
        session_state = request.session.get("oauth_state")

        if not state or state != session_state:
            return RedirectResponse(
                url="/auth/login?error=Invalid+state+token",
                status_code=303,
            )

        auth_service = get_auth_service()
        redirect_uri = str(request.url_for("auth_callback"))

        # Retrieve PKCE code_verifier from session
        code_verifier = request.session.get("oauth_code_verifier")

        user_info = auth_service.fetch_token(
            redirect_uri=redirect_uri,
            authorization_response=str(request.url),
            code_verifier=code_verifier,
        )

        request.session["user"] = {
            "email": user_info["email"],
            "name": user_info["name"],
            "picture": user_info.get("picture"),
        }

        request.session.pop("oauth_state", None)
        request.session.pop("oauth_code_verifier", None)
        logger.info(f"User logged in: {user_info['email']}")

        return RedirectResponse(url="/", status_code=303)

    except Exception as e:
        logger.error(f"OAuth callback error: {e}")
        return RedirectResponse(
            url=f"/auth/login?error={str(e)}",
            status_code=303,
        )


@app.get("/auth/logout")
async def auth_logout(request: Request):
    """Logout user."""
    user = request.session.get("user", {})
    logger.info(f"User logged out: {user.get('email', 'unknown')}")
    request.session.clear()
    return RedirectResponse(url="/auth/login", status_code=303)


# ============================================================================
# Main Routes — Simplified 3-Page Flow
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Page 2: Configure & Run — single page with topic + modules."""
    user = request.session.get("user", {})

    orchestrator = get_orchestrator()
    modules = orchestrator.get_available_modules()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "modules": modules,
            "topic": "",
        },
    )


@app.post("/run-report")
async def run_report(request: Request):
    """Handle the single-form submission: create run and redirect to status."""
    user = request.session.get("user", {})
    email = user.get("email")

    if not email:
        return RedirectResponse(url="/auth/login", status_code=303)

    form = await request.form()
    topic = form.get("topic", "").strip()

    # Validate topic
    if not topic or len(topic) < 2:
        orchestrator = get_orchestrator()
        modules = orchestrator.get_available_modules()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "user": user,
                "modules": modules,
                "topic": topic,
                "error": "Please enter a topic (at least 2 characters).",
            },
        )

    # Get selected modules
    selected_modules = form.getlist("modules")
    if not selected_modules:
        selected_modules = ["jobs", "courses", "trends", "lightcast"]
        # Filter to only available ones
        orchestrator = get_orchestrator()
        available = {m["name"] for m in orchestrator.get_available_modules() if m["available"]}
        selected_modules = [m for m in selected_modules if m in available]

    sharing_mode = form.get("sharing_mode", "restricted")
    max_results = 15
    try:
        max_results = int(form.get("max_results", 15))
        max_results = max(5, min(50, max_results))
    except (ValueError, TypeError):
        pass

    # Auto-generate module inputs from topic
    module_inputs = _build_module_inputs(topic, selected_modules, max_results)

    # Create run ID
    run_id = str(uuid.uuid4())[:8]

    # Store run info
    session_data = {
        "email": email,
        "topic": topic,
        "selected_modules": selected_modules,
        "module_inputs": module_inputs,
    }

    active_runs[run_id] = {
        "status": "starting",
        "session": session_data,
        "sharing_mode": sharing_mode,
    }

    return RedirectResponse(url=f"/status/{run_id}", status_code=303)


def _build_module_inputs(topic: str, selected_modules: list[str], max_results: int = 15) -> dict:
    """Auto-generate module inputs from the topic string."""
    inputs = {}

    if "jobs" in selected_modules:
        inputs["jobs"] = {
            "query": topic,
            "location": "United States",
            "results_limit": max_results,
            "employment_type": "all",
            "date_posted": "month",
            "include_bls": True,
            "extract_skills": True,
        }

    if "courses" in selected_modules:
        inputs["courses"] = {
            "keywords": topic,
            "max_results": max_results,
            "sources": ["coursera", "edx"],
            "level": "all",
            "include_certificates": False,
        }

    if "trends" in selected_modules:
        inputs["trends"] = {
            "terms": topic,
            "timeframe": "today 12-m",
            "geo": "US",
        }

    if "lightcast" in selected_modules:
        inputs["lightcast"] = {
            "skills": topic,
        }

    return inputs


@app.get("/status/{run_id}", response_class=HTMLResponse)
async def status_page(request: Request, run_id: str):
    """Page 3: Progress + Results — live execution with inline results."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return RedirectResponse(url="/", status_code=303)

    user = request.session.get("user", {})

    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "run_id": run_id,
            "run_info": run_info,
            "user": user,
        },
    )


@app.post("/execute/{run_id}")
async def execute_pipeline(request: Request, run_id: str):
    """Execute the pipeline (called via AJAX from status page)."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return JSONResponse({"error": "Run not found"})

    session = run_info["session"]
    sharing_mode = run_info["sharing_mode"]

    run_info["status"] = "running"

    try:
        orchestrator = get_orchestrator()
        result = await orchestrator.execute(
            user_email=session["email"],
            topic=session["topic"],
            selected_modules=session["selected_modules"],
            module_inputs=session.get("module_inputs", {}),
            sharing_mode=sharing_mode,
        )

        run_info["status"] = result.status.value
        run_info["result"] = result
        run_info["output_url"] = result.output_url
        run_info["output_folder_url"] = result.output_folder_url
        run_info["errors"] = result.errors

        module_summary = {}
        for module_name, progress in result.progress.items():
            module_summary[module_name] = {
                "display_name": progress.display_name,
                "status": progress.status.value,
                "rows": progress.result.total_rows if progress.result else 0,
                "message": progress.message,
            }

        # Send email notification (only for Google Sheets links, not local downloads)
        if result.output_url and not result.output_url.startswith("/static/"):
            try:
                email_service = get_email_service()
                email_service.send_results_email(
                    to_email=session["email"],
                    topic=session["topic"],
                    spreadsheet_url=result.output_url,
                    folder_url=result.output_folder_url or "",
                    run_summary={"modules": module_summary},
                )
            except Exception as e:
                logger.warning(f"Email notification failed: {e}")

        return JSONResponse({
            "status": result.status.value,
            "output_url": result.output_url,
            "output_folder_url": result.output_folder_url,
            "errors": result.errors,
            "modules": module_summary,
        })

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        run_info["status"] = "failed"
        run_info["errors"] = [str(e)]
        return JSONResponse({
            "status": "failed",
            "errors": [str(e)],
        })


@app.get("/results/{run_id}", response_class=HTMLResponse)
async def results_page(request: Request, run_id: str):
    """Results page (alternate view)."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return RedirectResponse(url="/", status_code=303)

    user = request.session.get("user", {})

    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "run_id": run_id,
            "run_info": run_info,
            "user": user,
        },
    )


# ============================================================================
# Legacy Routes (redirect to new flow)
# ============================================================================

@app.post("/start")
async def start_wizard_legacy(request: Request):
    """Legacy route — redirect to new flow."""
    return RedirectResponse(url="/", status_code=303)


@app.get("/select-modules", response_class=HTMLResponse)
async def select_modules_legacy(request: Request):
    """Legacy route — redirect to new flow."""
    return RedirectResponse(url="/", status_code=303)


@app.get("/review", response_class=HTMLResponse)
async def review_legacy(request: Request):
    """Legacy route — redirect to new flow."""
    return RedirectResponse(url="/", status_code=303)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/api/status/{run_id}")
async def get_run_status(run_id: str):
    """Get current status of a run."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return JSONResponse({"error": "Run not found"})

    result = run_info.get("result")

    response = {
        "status": run_info.get("status", "unknown"),
        "output_url": run_info.get("output_url"),
        "errors": run_info.get("errors", []),
    }

    if result:
        response["modules"] = {}
        for module_name, progress in result.progress.items():
            response["modules"][module_name] = {
                "display_name": progress.display_name,
                "status": progress.status.value,
                "message": progress.message,
            }

    return JSONResponse(response)


@app.get("/api/config-status")
async def get_config_status():
    """Get configuration status for all services."""
    return JSONResponse({
        "google_sheets": get_sheets_service().is_available(),
        "email": get_email_service().is_available(),
        "serpapi": settings.serpapi_available,
        "lightcast": settings.lightcast_available,
        "bls": settings.bls_available,
    })


# ============================================================================
# Health Check
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return JSONResponse({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
    })

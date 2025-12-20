"""
Market Intelligence Tool - FastAPI Application

Main entry point for the web application.
Provides a wizard-style interface for running market intelligence queries.
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
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

# Configure logging - Set to DEBUG for detailed module logging
LOG_LEVEL = logging.DEBUG if settings.debug else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Set specific loggers to appropriate levels
# Keep app modules at DEBUG level for detailed logging
logging.getLogger("app").setLevel(logging.DEBUG)
# Keep third-party libraries at INFO or WARNING to reduce noise
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Application paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Market Intelligence Tool")
    yield
    logger.info("Shutting down Market Intelligence Tool")


# Create FastAPI app
app = FastAPI(
    title="Market Intelligence Tool",
    description="ASU Learning Enterprise Market Intelligence Tool",
    version="1.0.0",
    lifespan=lifespan,
)

# Add session middleware
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    max_age=3600,  # 1 hour session
)

# Add authentication middleware
app.add_middleware(AuthMiddleware)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Setup templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# In-memory storage for active runs (in production, use Redis or database)
active_runs: dict[str, dict] = {}


# ============================================================================
# Template Helpers
# ============================================================================

def get_session_data(request: Request) -> dict:
    """Get session data from request."""
    return request.session.get("wizard", {})


def set_session_data(request: Request, data: dict) -> None:
    """Set session data."""
    request.session["wizard"] = data


def clear_session_data(request: Request) -> None:
    """Clear session data."""
    request.session.pop("wizard", None)


# ============================================================================
# Authentication Routes
# ============================================================================

@app.get("/auth/login", response_class=HTMLResponse)
async def auth_login(request: Request, error: str = None):
    """Google OAuth login page."""
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error,
        },
    )


@app.get("/auth/callback-init")
async def auth_callback_init(request: Request):
    """Initialize OAuth flow and redirect to Google."""
    # Generate state token for CSRF protection
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    # Get authorization URL
    auth_service = get_auth_service()
    redirect_uri = str(request.url_for("auth_callback"))

    authorization_url = auth_service.get_authorization_url(
        redirect_uri=redirect_uri,
        state=state,
    )

    return RedirectResponse(url=authorization_url)


@app.get("/auth/callback")
async def auth_callback(request: Request):
    """Handle OAuth callback from Google."""
    try:
        # Verify state token
        state = request.query_params.get("state")
        session_state = request.session.get("oauth_state")

        if not state or state != session_state:
            return RedirectResponse(
                url="/auth/login?error=Invalid+state+token",
                status_code=303,
            )

        # Exchange code for token and get user info
        auth_service = get_auth_service()
        redirect_uri = str(request.url_for("auth_callback"))

        user_info = auth_service.fetch_token(
            redirect_uri=redirect_uri,
            authorization_response=str(request.url),
        )

        # Store user info in session
        request.session["user"] = {
            "email": user_info["email"],
            "name": user_info["name"],
            "picture": user_info.get("picture"),
        }

        # Clear OAuth state
        request.session.pop("oauth_state", None)

        logger.info(f"User logged in: {user_info['email']}")

        # Redirect to home page
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
# Routes - Wizard Flow
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Home page - Step 1: Topic entry (email from OAuth)."""
    # Clear any previous session
    clear_session_data(request)

    # Get authenticated user
    user = request.session.get("user", {})

    # Get configuration status
    config_status = {
        "google_sheets": get_sheets_service().is_available(),
        "email": get_email_service().is_available(),
        "serpapi": settings.serpapi_available,
        "lightcast": settings.lightcast_available,
    }

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "config_status": config_status,
        },
    )


@app.post("/start")
async def start_wizard(
    request: Request,
    topic: str = Form(...),
):
    """Start the wizard - validate topic, proceed to module selection."""
    # Get authenticated user email
    user = request.session.get("user", {})
    email = user.get("email")

    if not email:
        return RedirectResponse(url="/auth/login", status_code=303)

    # Validate topic
    if not topic or len(topic) < 2:
        config_status = {
            "google_sheets": get_sheets_service().is_available(),
            "email": get_email_service().is_available(),
            "serpapi": settings.serpapi_available,
            "lightcast": settings.lightcast_available,
        }
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "user": user,
                "error": "Please enter a topic (at least 2 characters).",
                "topic": topic,
                "config_status": config_status,
            },
        )

    # Store in session
    session_data = {
        "email": email,
        "topic": topic,
        "run_id": str(uuid.uuid4())[:8],
        "created_at": datetime.now().isoformat(),
    }
    set_session_data(request, session_data)

    return RedirectResponse(url="/select-modules", status_code=303)


@app.get("/select-modules", response_class=HTMLResponse)
async def select_modules_page(request: Request):
    """Step 2: Module selection page."""
    session = get_session_data(request)
    if not session.get("email"):
        return RedirectResponse(url="/", status_code=303)

    # Get available modules
    orchestrator = get_orchestrator()
    modules = orchestrator.get_available_modules()

    return templates.TemplateResponse(
        "select_modules.html",
        {
            "request": request,
            "session": session,
            "modules": modules,
        },
    )


@app.post("/select-modules")
async def select_modules_submit(request: Request):
    """Process module selection."""
    session = get_session_data(request)
    if not session.get("email"):
        return RedirectResponse(url="/", status_code=303)

    form = await request.form()
    selected_modules = form.getlist("modules")

    if not selected_modules:
        orchestrator = get_orchestrator()
        modules = orchestrator.get_available_modules()
        return templates.TemplateResponse(
            "select_modules.html",
            {
                "request": request,
                "session": session,
                "modules": modules,
                "error": "Please select at least one module.",
            },
        )

    # Store selection
    session["selected_modules"] = list(selected_modules)
    session["current_module_index"] = 0
    session["module_inputs"] = {}
    set_session_data(request, session)

    # Redirect to first module's input page
    first_module = selected_modules[0]
    return RedirectResponse(url=f"/module/{first_module}", status_code=303)


@app.get("/module/{module_name}", response_class=HTMLResponse)
async def module_input_page(request: Request, module_name: str):
    """Step 3: Module-specific input page."""
    session = get_session_data(request)
    if not session.get("email") or not session.get("selected_modules"):
        return RedirectResponse(url="/", status_code=303)

    if module_name not in session["selected_modules"]:
        return RedirectResponse(url="/select-modules", status_code=303)

    # Get module info
    orchestrator = get_orchestrator()
    module = orchestrator.get_module(module_name)

    if not module:
        raise HTTPException(status_code=404, detail="Module not found")

    # Determine navigation
    modules = session["selected_modules"]
    current_index = modules.index(module_name)
    prev_module = modules[current_index - 1] if current_index > 0 else None
    next_module = modules[current_index + 1] if current_index < len(modules) - 1 else None
    is_last = current_index == len(modules) - 1

    # Get existing inputs for this module
    existing_inputs = session.get("module_inputs", {}).get(module_name, {})

    return templates.TemplateResponse(
        "module_input.html",
        {
            "request": request,
            "session": session,
            "module": module,
            "module_name": module_name,
            "basic_fields": module.get_basic_fields(),
            "advanced_fields": module.get_advanced_fields(),
            "existing_inputs": existing_inputs,
            "prev_module": prev_module,
            "next_module": next_module,
            "is_last": is_last,
            "topic": session.get("topic", ""),
        },
    )


@app.post("/module/{module_name}")
async def module_input_submit(request: Request, module_name: str):
    """Process module input submission."""
    session = get_session_data(request)
    if not session.get("email") or not session.get("selected_modules"):
        return RedirectResponse(url="/", status_code=303)

    form = await request.form()

    # Get module
    orchestrator = get_orchestrator()
    module = orchestrator.get_module(module_name)

    if not module:
        raise HTTPException(status_code=404, detail="Module not found")

    # Parse form data into inputs
    inputs = {}
    for field in module.input_fields:
        value = form.get(field.name)

        if field.field_type == "checkbox":
            inputs[field.name] = value == "on" or value == "true"
        elif field.field_type == "number":
            try:
                inputs[field.name] = int(value) if value else field.default
            except (ValueError, TypeError):
                inputs[field.name] = field.default
        elif field.field_type == "multiselect":
            inputs[field.name] = form.getlist(field.name)
        else:
            inputs[field.name] = value if value else field.default

    # Validate inputs
    validation = module.validate_inputs(inputs)

    if not validation.is_valid:
        modules_list = session["selected_modules"]
        current_index = modules_list.index(module_name)
        prev_module = modules_list[current_index - 1] if current_index > 0 else None
        next_module = modules_list[current_index + 1] if current_index < len(modules_list) - 1 else None
        is_last = current_index == len(modules_list) - 1

        return templates.TemplateResponse(
            "module_input.html",
            {
                "request": request,
                "session": session,
                "module": module,
                "module_name": module_name,
                "basic_fields": module.get_basic_fields(),
                "advanced_fields": module.get_advanced_fields(),
                "existing_inputs": inputs,
                "prev_module": prev_module,
                "next_module": next_module,
                "is_last": is_last,
                "topic": session.get("topic", ""),
                "errors": {e.field: e.message for e in validation.errors},
            },
        )

    # Store inputs
    if "module_inputs" not in session:
        session["module_inputs"] = {}
    session["module_inputs"][module_name] = inputs
    set_session_data(request, session)

    # Determine next step
    modules = session["selected_modules"]
    current_index = modules.index(module_name)

    if current_index < len(modules) - 1:
        # Go to next module
        next_module = modules[current_index + 1]
        return RedirectResponse(url=f"/module/{next_module}", status_code=303)
    else:
        # All modules configured, go to review
        return RedirectResponse(url="/review", status_code=303)


@app.get("/review", response_class=HTMLResponse)
async def review_page(request: Request):
    """Step 4: Review all inputs before running."""
    session = get_session_data(request)
    if not session.get("email") or not session.get("selected_modules"):
        return RedirectResponse(url="/", status_code=303)

    orchestrator = get_orchestrator()

    # Get module info for display
    modules_info = []
    for module_name in session["selected_modules"]:
        module = orchestrator.get_module(module_name)
        if module:
            modules_info.append({
                "name": module_name,
                "display_name": module.display_name,
                "inputs": session.get("module_inputs", {}).get(module_name, {}),
            })

    return templates.TemplateResponse(
        "review.html",
        {
            "request": request,
            "session": session,
            "modules_info": modules_info,
        },
    )


@app.post("/run")
async def run_pipeline(request: Request):
    """Execute the pipeline."""
    session = get_session_data(request)
    if not session.get("email") or not session.get("selected_modules"):
        return RedirectResponse(url="/", status_code=303)

    form = await request.form()
    sharing_mode = form.get("sharing_mode", "restricted")

    # Create run ID
    run_id = session.get("run_id", str(uuid.uuid4())[:8])

    # Store run info for status page
    active_runs[run_id] = {
        "status": "starting",
        "session": session,
        "sharing_mode": sharing_mode,
    }

    return RedirectResponse(url=f"/status/{run_id}", status_code=303)


@app.get("/status/{run_id}", response_class=HTMLResponse)
async def status_page(request: Request, run_id: str):
    """Step 5: Pipeline execution status page."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "run_id": run_id,
            "run_info": run_info,
        },
    )


@app.post("/execute/{run_id}")
async def execute_pipeline(request: Request, run_id: str):
    """Actually execute the pipeline (called via AJAX from status page)."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return {"error": "Run not found"}

    session = run_info["session"]
    sharing_mode = run_info["sharing_mode"]

    # Update status
    run_info["status"] = "running"

    try:
        # Execute pipeline
        orchestrator = get_orchestrator()
        result = await orchestrator.execute(
            user_email=session["email"],
            topic=session["topic"],
            selected_modules=session["selected_modules"],
            module_inputs=session.get("module_inputs", {}),
            sharing_mode=sharing_mode,
        )

        # Update run info with results
        run_info["status"] = result.status.value
        run_info["result"] = result
        run_info["output_url"] = result.output_url
        run_info["output_folder_url"] = result.output_folder_url
        run_info["errors"] = result.errors

        # Build module summary for email
        module_summary = {}
        for module_name, progress in result.progress.items():
            module_summary[module_name] = {
                "display_name": progress.display_name,
                "status": progress.status.value,
                "rows": progress.result.total_rows if progress.result else 0,
            }

        # Send email notification
        if result.output_url:
            email_service = get_email_service()
            email_service.send_results_email(
                to_email=session["email"],
                topic=session["topic"],
                spreadsheet_url=result.output_url,
                folder_url=result.output_folder_url or "",
                run_summary={"modules": module_summary},
            )

        return {
            "status": result.status.value,
            "output_url": result.output_url,
            "output_folder_url": result.output_folder_url,
            "errors": result.errors,
            "modules": module_summary,
        }

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        run_info["status"] = "failed"
        run_info["errors"] = [str(e)]
        return {
            "status": "failed",
            "errors": [str(e)],
        }


@app.get("/results/{run_id}", response_class=HTMLResponse)
async def results_page(request: Request, run_id: str):
    """Step 6: Final results page."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "run_id": run_id,
            "run_info": run_info,
        },
    )


# ============================================================================
# API Endpoints (for AJAX calls)
# ============================================================================

@app.get("/api/status/{run_id}")
async def get_run_status(run_id: str):
    """Get current status of a run."""
    run_info = active_runs.get(run_id)

    if not run_info:
        return {"error": "Run not found"}

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

    return response


@app.get("/api/config-status")
async def get_config_status():
    """Get configuration status for all services."""
    return {
        "google_sheets": get_sheets_service().is_available(),
        "email": get_email_service().is_available(),
        "serpapi": settings.serpapi_available,
        "lightcast": settings.lightcast_available,
        "bls": settings.bls_available,
    }


# ============================================================================
# Health Check
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
    }

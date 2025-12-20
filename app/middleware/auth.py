"""
Authentication middleware to protect routes.
"""

from fastapi import Request
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware to require Google OAuth authentication for all routes."""

    # Routes that don't require authentication
    EXCLUDED_PATHS = [
        "/auth/login",
        "/auth/callback",
        "/static",
        "/health",
    ]

    async def dispatch(self, request: Request, call_next):
        # Skip auth for excluded paths
        path = request.url.path
        if any(path.startswith(excluded) for excluded in self.EXCLUDED_PATHS):
            return await call_next(request)

        # Check if user is authenticated
        user = request.session.get("user")

        if not user:
            # Redirect to login
            return RedirectResponse(url="/auth/login", status_code=303)

        # User is authenticated, proceed
        response = await call_next(request)
        return response

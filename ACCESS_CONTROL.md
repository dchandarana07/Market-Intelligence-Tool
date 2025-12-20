# Access Control Implementation Guide

This guide shows you how to restrict access to only your manager.

## Option A: Simple Password Protection (RECOMMENDED)

### Step 1: Add Password to Settings

Edit `config/settings.py` and add:

```python
class Settings(BaseSettings):
    # ... existing settings ...

    # Access Control
    access_password: str = ""  # Set via ACCESS_PASSWORD env variable
```

### Step 2: Add to Environment Variables

Edit `.env`:

```bash
# Add this line
ACCESS_PASSWORD=your_secure_password_here
```

For production (Render/Railway), set `ACCESS_PASSWORD` as environment variable.

### Step 3: Add Login Middleware

Create `app/middleware/auth.py`:

```python
"""Simple password-based authentication middleware."""

from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from config.settings import settings


class SimpleAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to require password for all routes except login."""

    EXCLUDED_PATHS = ["/login", "/static", "/health"]

    async def dispatch(self, request: Request, call_next):
        # Skip auth for excluded paths
        if any(request.url.path.startswith(path) for path in self.EXCLUDED_PATHS):
            return await call_next(request)

        # Check if user is authenticated
        authenticated = request.session.get("authenticated", False)

        if not authenticated:
            # Redirect to login
            return RedirectResponse(url="/login", status_code=303)

        # User is authenticated, proceed
        return await call_next(request)
```

### Step 4: Add Login Route

Add to `app/main.py` (before the existing routes):

```python
from fastapi import FastAPI, Request, Form, HTTPException
from app.middleware.auth import SimpleAuthMiddleware

# ... existing imports ...

# Add middleware (after SessionMiddleware)
if settings.access_password:  # Only enable if password is set
    app.add_middleware(SimpleAuthMiddleware)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    """Login page."""
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error,
        },
    )


@app.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    """Process login."""
    if password == settings.access_password:
        request.session["authenticated"] = True
        return RedirectResponse(url="/", status_code=303)
    else:
        return RedirectResponse(url="/login?error=Invalid+password", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    """Logout user."""
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
```

### Step 5: Create Login Template

Create `app/templates/login.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Market Intelligence Tool</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="/static/css/custom.css">
</head>
<body class="bg-light">
    <div class="container">
        <div class="row justify-content-center mt-5">
            <div class="col-md-6 col-lg-4">
                <div class="card shadow">
                    <div class="card-body p-5">
                        <h2 class="text-center mb-4">🔒 Access Required</h2>
                        <h5 class="text-center text-muted mb-4">Market Intelligence Tool</h5>

                        {% if error %}
                        <div class="alert alert-danger" role="alert">
                            {{ error }}
                        </div>
                        {% endif %}

                        <form method="POST" action="/login">
                            <div class="mb-3">
                                <label for="password" class="form-label">Password</label>
                                <input
                                    type="password"
                                    class="form-control form-control-lg"
                                    id="password"
                                    name="password"
                                    required
                                    autofocus
                                    placeholder="Enter access password"
                                >
                            </div>

                            <button type="submit" class="btn btn-primary btn-lg w-100">
                                Login
                            </button>
                        </form>

                        <hr class="my-4">

                        <p class="text-center text-muted small mb-0">
                            Contact your administrator for access
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
```

### Step 6: Test Locally

1. Set password in `.env`:
   ```bash
   ACCESS_PASSWORD=MySecurePassword123
   ```

2. Restart app:
   ```bash
   python run.py
   ```

3. Visit `http://localhost:8000` → Should redirect to login

4. Enter password → Should access app

---

## Option B: Email-Based Access (More Restrictive)

Only allow specific email address(es).

### Add to `app/main.py`:

```python
# After login, also check email
ALLOWED_EMAILS = [
    "manager@company.com",
    "you@company.com",
]

@app.post("/start")
async def start_wizard(request: Request, email: str = Form(...), topic: str = Form(...)):
    """Start the wizard - validate email and topic."""

    # Check if email is allowed
    if email not in ALLOWED_EMAILS:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": f"Access denied. Email '{email}' is not authorized. Please contact the administrator.",
                "email": email,
                "topic": topic,
                "config_status": {...},
            },
        )

    # ... rest of existing code ...
```

### Configure allowed emails via environment:

```python
# config/settings.py
allowed_emails: str = ""  # Comma-separated emails

# In code
ALLOWED_EMAILS = [e.strip() for e in settings.allowed_emails.split(",") if e.strip()]
```

```bash
# .env
ALLOWED_EMAILS=manager@company.com,you@company.com
```

---

## Option C: Google OAuth (Most Secure)

Require users to sign in with Google, restrict to specific email domain.

More complex, but most secure. See `DEPLOYMENT.md` for OAuth setup.

---

## Deployment Setup

### For Render/Railway:

1. **Set environment variable:**
   ```
   ACCESS_PASSWORD=YourSecurePassword123
   ```

2. **Share with manager:**
   - URL: `https://your-app.onrender.com`
   - Password: `YourSecurePassword123` (via secure channel)

3. **Manager workflow:**
   - Visit URL
   - Enter password
   - Use app normally
   - Results go to their email + Google Sheets

---

## Security Best Practices

### Generate Strong Password:

```bash
# On Mac/Linux
openssl rand -base64 32
```

Example output: `K7gH9mN3pQ8rT2vX5yZ1aB4cD6eF8hJ0`

### Share Password Securely:

- ✅ Use encrypted messaging (Signal, WhatsApp)
- ✅ Tell them in person
- ✅ Use password manager link sharing
- ❌ Don't email plain text
- ❌ Don't put in Slack/Teams

### Session Timeout:

Already configured: 1 hour (see `SessionMiddleware` in main.py)

Users will need to re-login after 1 hour of inactivity.

---

## Quick Implementation Checklist

1. ✅ Add `ACCESS_PASSWORD` to `.env`
2. ✅ Add password to `config/settings.py`
3. ✅ Create `app/middleware/auth.py`
4. ✅ Add login routes to `app/main.py`
5. ✅ Create `app/templates/login.html`
6. ✅ Test locally
7. ✅ Deploy to Render/Railway
8. ✅ Set `ACCESS_PASSWORD` in production env vars
9. ✅ Share URL + password with manager

---

## Which Option to Choose?

| Option | Security | Ease | Use Case |
|--------|----------|------|----------|
| **Password** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **Recommended** - Simple, effective |
| **Email List** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Multiple specific users |
| **Google OAuth** | ⭐⭐⭐⭐⭐ | ⭐⭐ | Enterprise, many users |

**My recommendation:** Start with **Option A (Password)** - simplest and sufficient for 1-2 users.

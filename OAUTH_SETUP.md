# Google OAuth Setup Guide

This guide will help you set up Google Sign-In authentication for the Market Intelligence Tool.

## Overview

The app now uses **Google OAuth** for authentication:
- Users sign in with their Google account
- No need to enter email - it's automatically captured from Google
- More secure than password protection
- Results are automatically shared with the signed-in user's email

---

## Step 1: Configure Google Cloud Console (5 minutes)

### 1.1 Go to Google Cloud Console

Visit: https://console.cloud.google.com/

### 1.2 Select Your Project

Use the same project where you configured Google Sheets API, or create a new one.

### 1.3 Configure OAuth Consent Screen

1. Navigate to: **APIs & Services** → **OAuth consent screen**

2. Choose User Type:
   - **Internal**: If you have Google Workspace (only your organization can access)
   - **External**: For personal Google accounts or allowing external users

3. Fill in required information:
   - **App name**: Market Intelligence Tool
   - **User support email**: Your email
   - **Developer contact**: Your email

4. **Scopes**: Click "Add or Remove Scopes"
   - Add: `openid`
   - Add: `email`
   - Add: `profile`
   - These are already selected by default, just verify

5. **Test users** (if External):
   - Add your email
   - Add your manager's email
   - Add any other authorized users

6. Click **Save and Continue** through remaining steps

### 1.4 Create OAuth 2.0 Credentials

1. Navigate to: **APIs & Services** → **Credentials**

2. Click **Create Credentials** → **OAuth 2.0 Client ID**

3. Configure:
   - **Application type**: Web application
   - **Name**: Market Intelligence Tool - Web Client

4. **Authorized redirect URIs** - Add BOTH:
   ```
   http://localhost:8000/auth/callback
   https://your-production-domain.com/auth/callback
   ```

   **Important Notes:**
   - For local development, use: `http://localhost:8000/auth/callback`
   - For production (Railway/Render), use your deployed URL
   - You can add multiple redirect URIs
   - URLs must match EXACTLY (including http vs https)

5. Click **Create**

6. **Download the JSON**:
   - Click the download icon for your newly created OAuth client
   - Save as: `config/oauth_client.json`

7. **Copy credentials**:
   - Note the Client ID and Client Secret
   - You'll add these to `.env`

---

## Step 2: Configure Environment Variables

### 2.1 Update `.env` file

Add these lines to your `.env` file:

```bash
# Google OAuth for Sign-In
GOOGLE_OAUTH_CLIENT_ID=your_client_id_here.apps.googleusercontent.com
GOOGLE_OAUTH_CLIENT_SECRET=GOCSPX-your_client_secret_here
GOOGLE_OAUTH_CLIENT_CONFIG=./config/oauth_client.json
```

Replace with your actual values from Google Cloud Console.

### 2.2 Place the OAuth Client JSON

Put the downloaded `oauth_client.json` file in:
```
config/oauth_client.json
```

This file should look like:
```json
{
  "web": {
    "client_id": "123456789.apps.googleusercontent.com",
    "client_secret": "GOCSPX-abc123...",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "redirect_uris": ["http://localhost:8000/auth/callback"]
  }
}
```

---

## Step 3: Test Locally

### 3.1 Restart the App

```bash
# Stop the current app (Ctrl+C if running)

# Restart
python run.py
```

### 3.2 Test Sign-In

1. Visit: http://localhost:8000

2. You should be redirected to `/auth/login`

3. Click **"Sign in with Google"**

4. Choose your Google account

5. Grant permissions (first time only)

6. You should be redirected back to the app home page

7. Your name, email, and picture should appear at the top

8. Click **"Logout"** to test logout functionality

---

## Step 4: Production Deployment

### 4.1 Update Redirect URIs

1. Go back to Google Cloud Console → Credentials

2. Edit your OAuth 2.0 Client ID

3. Add your production URL to **Authorized redirect URIs**:
   ```
   https://your-app.railway.app/auth/callback
   ```
   or
   ```
   https://your-app.onrender.com/auth/callback
   ```

4. Save

### 4.2 Set Environment Variables in Production

In Railway/Render dashboard, add:
```
GOOGLE_OAUTH_CLIENT_ID=your_client_id.apps.googleusercontent.com
GOOGLE_OAUTH_CLIENT_SECRET=GOCSPX-your_secret
```

**Note**: You don't need to upload `oauth_client.json` to production if you set the environment variables.

---

## How It Works Now

### User Flow

1. **Visit App** → Redirected to login page
2. **Click "Sign in with Google"** → Google OAuth consent screen
3. **Authorize** → Redirected back to app
4. **Authenticated** → Can use all features
5. **Results** → Automatically shared with their Google email

### For Your Manager

Your manager will:
1. Visit the app URL
2. Sign in with their Google account
3. Enter a research topic
4. Select modules and configure
5. Run analysis
6. Results automatically shared with their email

**No setup required on their end!**

---

## Security Features

✅ **OAuth 2.0**: Industry-standard authentication
✅ **State tokens**: CSRF protection
✅ **Session management**: 1-hour timeout
✅ **No password storage**: Google handles authentication
✅ **Automatic email capture**: From verified Google account

---

## Restricting Access

### Option 1: Internal App (Google Workspace Only)

If you have Google Workspace:
1. Set OAuth consent to **Internal**
2. Only users in your organization can access

### Option 2: Test Users List

For External apps:
1. Keep app in "Testing" mode
2. Only users in the test users list can access
3. Add users in: OAuth consent screen → Test users

### Option 3: Email Verification (Code Change)

Add email validation in `app/main.py`:

```python
# After OAuth callback, check email domain
ALLOWED_DOMAINS = ["asu.edu"]  # Only ASU emails

user_email = user_info["email"]
email_domain = user_email.split("@")[1]

if email_domain not in ALLOWED_DOMAINS:
    logger.warning(f"Unauthorized access attempt: {user_email}")
    return RedirectResponse(
        url="/auth/login?error=Unauthorized+domain",
        status_code=303,
    )
```

---

## Troubleshooting

### "Redirect URI mismatch" error

**Problem**: The redirect URI doesn't match what's configured in Google Cloud Console.

**Solution**:
1. Check the exact error message for the URI it received
2. Add that EXACT URI to your OAuth client's authorized redirect URIs
3. Common issues:
   - `http` vs `https`
   - `localhost` vs `127.0.0.1`
   - Trailing slash: `/auth/callback/` vs `/auth/callback`

### "Access blocked: This app's request is invalid"

**Problem**: OAuth consent screen not configured properly.

**Solution**:
1. Complete all required fields in OAuth consent screen
2. Add your email to test users (if External)
3. Ensure scopes include: openid, email, profile

### "Invalid client" error

**Problem**: Client ID or Secret is incorrect.

**Solution**:
1. Double-check values in `.env`
2. Ensure no extra spaces or quotes
3. Regenerate client secret if needed

### User sees blank page after sign-in

**Problem**: Session not being saved.

**Solution**:
1. Check SECRET_KEY is set in `.env`
2. Restart the app after changing environment variables

---

## FAQs

**Q: Do I need separate OAuth credentials for development and production?**

A: No, you can use the same client but add multiple redirect URIs.

**Q: Can I revoke someone's access?**

A: Yes, remove them from test users list (if External) or they can revoke access at https://myaccount.google.com/permissions

**Q: Does this replace the Google Sheets OAuth?**

A: No, these are separate:
- **Sign-In OAuth**: User authentication (who can access the app)
- **Sheets OAuth**: Service access (app accessing Google Sheets API)

Both are needed.

**Q: What if the user doesn't have a Google account?**

A: They'll need to create one (free) at https://accounts.google.com/signup

---

## Next Steps

After setup is complete:
1. ✅ Test locally
2. ✅ Deploy to production
3. ✅ Share app URL with your manager
4. ✅ They sign in with Google
5. ✅ Start generating market intelligence! 🎉

---

**Need Help?**

Check the logs for detailed error messages:
```bash
# View logs
tail -f logs/app.log

# Or check Railway/Render deployment logs
```

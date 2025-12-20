# Deployment Guide

## Quick Deploy Options

### Option 1: ngrok (Fastest - 5 minutes)

Perfect for immediate access and demos.

1. **Install ngrok:**
   ```bash
   brew install ngrok/ngrok/ngrok
   # Or download from https://ngrok.com/download
   ```

2. **Sign up** at https://ngrok.com (free)

3. **Authenticate:**
   ```bash
   ngrok config add-authtoken YOUR_AUTH_TOKEN
   ```

4. **Start your app** (if not already running):
   ```bash
   source venv/bin/activate
   python run.py
   ```

5. **Expose with ngrok:**
   ```bash
   ngrok http 8000
   ```

6. **Share the URL** (e.g., `https://abc123.ngrok.io`) with your manager

**Note:** Keep your terminal and app running. URL changes when you restart ngrok.

---

### Option 2: Railway (Best for Production - 30 minutes)

Permanent deployment with custom domain support.

#### Step 1: Prepare Files

Already done! The repo includes:
- `Procfile` - Deployment command
- `railway.json` - Railway configuration
- `runtime.txt` - Python version
- `requirements.txt` - Dependencies

#### Step 2: Deploy to Railway

1. **Sign up** at https://railway.app (free $5/month credit)

2. **Create New Project:**
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Authenticate with GitHub
   - Select `dchandarana07/Market-Intelligence-Tool`

3. **Configure Environment Variables:**

   In Railway dashboard → Variables, add:

   ```bash
   # Required
   GOOGLE_CREDENTIALS_PATH=./config/google-credentials.json
   GOOGLE_DRIVE_FOLDER_ID=your_folder_id_here
   SERPAPI_KEY=your_serpapi_key_here

   # Optional
   LIGHTCAST_CLIENT_ID=your_client_id_here
   LIGHTCAST_CLIENT_SECRET=your_client_secret_here
   BLS_API_KEY=your_bls_key_here

   # App settings
   ENVIRONMENT=production
   DEBUG=false
   SECRET_KEY=generate_random_string_here
   ```

4. **Upload Google Credentials:**

   Railway doesn't support file uploads directly, so we need to encode the credentials:

   ```bash
   # On your local machine
   cat config/google-credentials.json | base64
   ```

   Copy the output and add to Railway variables:
   ```
   GOOGLE_CREDENTIALS_BASE64=paste_base64_here
   ```

5. **Update code to handle base64 credentials:**

   The app needs a small update to decode base64 credentials on startup.
   (See below for code changes)

6. **Deploy:**
   - Railway will automatically deploy
   - Get your public URL: `https://your-app.railway.app`

#### Step 3: Configure OAuth for Production

Important! Update your Google OAuth credentials:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project
3. Go to **APIs & Services** → **Credentials**
4. Edit your OAuth 2.0 Client ID
5. Add authorized redirect URIs:
   ```
   https://your-app.railway.app/oauth/callback
   ```
6. Re-run the OAuth flow in production

---

### Option 3: Render (Alternative to Railway)

1. **Sign up** at https://render.com
2. **New Web Service** → Connect GitHub repo
3. **Configure:**
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Add environment variables (same as Railway)
5. Deploy

---

### Option 4: Fly.io (Serverless)

1. **Install flyctl:**
   ```bash
   brew install flyctl
   ```

2. **Login:**
   ```bash
   fly auth login
   ```

3. **Launch:**
   ```bash
   fly launch
   ```

4. **Set secrets:**
   ```bash
   fly secrets set SERPAPI_KEY=your_key
   fly secrets set GOOGLE_DRIVE_FOLDER_ID=your_id
   # ... etc
   ```

5. **Deploy:**
   ```bash
   fly deploy
   ```

---

## Required Code Changes for Cloud Deployment

### Handle Base64 Encoded Credentials

Add to `config/settings.py`:

```python
import os
import base64
from pathlib import Path

class Settings(BaseSettings):
    # ... existing code ...

    def _load_google_credentials(self):
        """Load Google credentials from file or base64 env var."""
        # Try base64 encoded credentials first (for cloud deployment)
        base64_creds = os.getenv("GOOGLE_CREDENTIALS_BASE64")
        if base64_creds:
            import json
            import tempfile

            # Decode and write to temp file
            creds_json = base64.b64decode(base64_creds)
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
            temp_file.write(creds_json.decode('utf-8'))
            temp_file.close()

            return Path(temp_file.name)

        # Otherwise use file path
        return self.google_credentials_path

    @property
    def google_credentials_available(self) -> bool:
        creds_path = self._load_google_credentials()
        return creds_path.exists() and self.google_drive_folder_id != ""
```

### Update Google Sheets Service

Modify `app/services/google_sheets_oauth.py` to use the new credential loading:

```python
from config.settings import settings

def get_credentials():
    creds_path = settings._load_google_credentials()
    # ... rest of existing code ...
```

---

## Security Considerations

1. **Never commit:**
   - `.env` file
   - `config/google-credentials.json`
   - `config/oauth_token.pickle`

2. **Environment variables:**
   - Set all secrets as environment variables in Railway/Render
   - Use strong SECRET_KEY in production

3. **OAuth Token:**
   - Store OAuth token in Railway persistent storage
   - Or regenerate on each deployment

4. **Rate Limits:**
   - SerpAPI: 100 searches/month (free)
   - Monitor usage in production

---

## How Your Manager Will Use It

1. **Access the URL:** `https://your-app.railway.app`
2. **Enter their email** and research topic
3. **Select modules** they want to run
4. **Configure parameters** (or use defaults)
5. **Run the analysis**
6. **Receive results:**
   - Google Sheets link will be shown on screen
   - Optional: Email notification with results

**Your manager does NOT need:**
- API keys (uses yours)
- Google OAuth setup (uses yours)
- Any technical setup

**They only need:**
- The app URL
- Their email address to receive results

---

## Monitoring & Maintenance

- **Railway Dashboard:** View logs, metrics, deployments
- **Cost:** Free tier covers ~500 hours/month
- **Updates:** Push to GitHub main branch → auto-deploys
- **Logs:** Check Railway dashboard for errors

---

## Recommended Approach

**For immediate access:** Use **ngrok** (5 minutes)

**For production use:** Deploy to **Railway** (30 minutes)
- Persistent URL
- Auto-deploys from GitHub
- Professional setup
- Custom domain support (optional)

---

## Support

Issues? Check:
1. Railway deployment logs
2. Environment variables are set correctly
3. Google OAuth redirect URIs include production URL
4. API keys are valid and have sufficient quota

# Render Docker Deployment Setup

## Critical: Enable Docker Deployment

Your Render service is currently **not using Docker**, which is why Chrome is not installed and the Courses module is failing.

## Steps to Fix

### Option 1: Update Existing Service (Recommended)

1. Go to your Render Dashboard: https://dashboard.render.com
2. Select your `market-intelligence-tool` service
3. Go to **Settings** tab
4. Scroll to **Build & Deploy** section
5. Change **Environment** from `Native` to `Docker`
6. Set **Dockerfile Path** to: `./Dockerfile`
7. Set **Docker Context** to: `.`
8. Click **Save Changes**
9. Go to **Manual Deploy** and click **Deploy latest commit**

### Option 2: Delete and Recreate Service

If Option 1 doesn't work:

1. Delete the existing service from Render dashboard
2. Click **New** → **Blueprint**
3. Connect your GitHub repository
4. Render will automatically detect `render.yaml` and configure Docker deployment
5. Click **Apply** to create the service

## Verifying Docker is Enabled

After deployment, check the logs for:
```
Building with Docker...
```

You should NOT see this error anymore:
```
/bin/sh: 1: google-chrome: not found
```

## Other Critical Fixes in This Deploy

1. **Google Sheets JSON Error**:
   - You need to **minify your Google credentials JSON** to a single line
   - Use: https://codebeautify.org/jsonminifier
   - Or use Render's **Secret File** feature to upload the JSON file directly

2. **Trends Module Crash**: Fixed the list/split error that was causing crashes

## After Deployment

Once Docker is enabled and deployed:
1. Test with a simple query like "Applied AI" or "Data Analysis"
2. All modules should work without errors
3. Check that Courses module returns results (requires Chrome/Docker)

## Need Help?

If you still see errors after these steps, please share:
1. Full deployment logs from Render
2. The error message you're seeing
3. Which modules are failing

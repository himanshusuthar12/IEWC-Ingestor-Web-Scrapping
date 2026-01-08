# Azure Function CORS Configuration Guide

## Issue: "Failed to fetch" Error in Azure Portal

If you're seeing "Failed to fetch" errors when testing your function in the Azure Portal, you need to configure CORS at the **Azure Function App level** in addition to the code-level CORS headers.

## Solution: Configure CORS in Azure Portal

### Step 1: Navigate to CORS Settings

1. Go to your **Azure Function App** in the Azure Portal
2. In the left sidebar, find **API** section
3. Click on **CORS** (or search for "CORS" in the search bar)

### Step 2: Add Allowed Origins

1. In the CORS settings, you'll see a list of allowed origins
2. Click **"Add"** or the **"+"** button
3. Add the following origins (one at a time):
   - `https://portal.azure.com`
   - `https://ms.portal.azure.com`
   - `*` (optional - allows all origins, use only for development)

4. Click **"Save"** at the top

### Step 3: Verify Configuration

After saving, wait a few seconds for the changes to propagate, then try testing your function again in the Azure Portal.

## Alternative: Configure via Azure CLI

If you prefer using Azure CLI:

```bash
az functionapp cors add --name <your-function-app-name> --resource-group <your-resource-group> --allowed-origins https://portal.azure.com https://ms.portal.azure.com
```

## Additional Network Requirements

If you still see network errors after configuring CORS:

1. **Check Network Restrictions:**
   - Go to **Networking** in your Function App
   - Ensure there are no access restrictions blocking the portal
   - If using VNet integration, ensure proper routing

2. **Check Service Tags:**
   - If using Network Security Groups, ensure `AzureCloud` service tag is allowed for outbound traffic

3. **Verify Function App Status:**
   - Ensure your Function App is running and not stopped
   - Check the **Overview** page for any warnings or errors

## Testing

After configuring CORS:

1. Go to your function in Azure Portal
2. Click **"Test/Run"**
3. The function should now respond without "Failed to fetch" errors

## Code-Level CORS (Already Implemented)

The code in `function_app.py` already includes CORS headers that will work for direct HTTP requests. However, Azure Portal requires CORS to be configured at the Function App level as well.


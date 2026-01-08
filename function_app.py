import logging
import os
import datetime
import azure.functions as func
from iewc_qdrant_crawler.crawler import run

# Create the FunctionApp instance
app = func.FunctionApp()

@app.function_name(name="iewc_qdrant_crawler")
@app.route(route="crawl", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "POST", "OPTIONS"])
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger for IEWC Qdrant crawler with CORS support"""
    logging.info("🚀 IEWC → Qdrant crawler triggered via HTTP")
    
    # Quick health check - respond immediately
    try:
        req_body_check = req.get_json()
    except:
        req_body_check = None
    
    if req.method == "GET" and not req.params.get("collection") and not req_body_check:
        origin = req.headers.get("Origin", "*")
        allow_origin = origin if origin.startswith("https://portal.azure.com") else "*"
        return func.HttpResponse(
            "IEWC Qdrant Crawler is running. Use POST with collection parameter to start crawling.",
            status_code=200,
            mimetype="text/plain",
            headers={
                "Access-Control-Allow-Origin": allow_origin,
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With"
            }
        )

    # Get the origin from the request header
    origin = req.headers.get("Origin", "*")
    # Allow portal.azure.com specifically, or use the request origin
    allowed_origins = ["https://portal.azure.com", "https://ms.portal.azure.com"]
    if origin in allowed_origins or origin.startswith("https://portal.azure.com"):
        allow_origin = origin
    else:
        # For other origins, allow all (or restrict as needed)
        allow_origin = "*"
    
    # Handle CORS preflight requests
    if req.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": allow_origin,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
            "Access-Control-Max-Age": "3600",
            "Access-Control-Allow-Credentials": "true"
        }
        return func.HttpResponse("", status_code=200, headers=headers)

    # CORS headers for all responses
    cors_headers = {
        "Access-Control-Allow-Origin": allow_origin,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
        "Access-Control-Allow-Credentials": "true"
    }

    try:
        # Try to get parameters from JSON body first, then query string, then environment variables
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        
        # Get parameters from JSON body, query string, or environment variables as defaults
        base_url = (
            req_body.get("base_url") or 
            req.params.get("base_url") or 
            os.environ.get("BASE_URL", "https://www.iewc.com/resources")
        )
        collection = (
            req_body.get("collection") or 
            req.params.get("collection") or 
            os.environ.get("QDRANT_COLLECTION")
        )
        max_depth = int(
            req_body.get("max_depth") or 
            req.params.get("max_depth") or 
            os.environ.get("MAX_DEPTH", "2")
        )
        embedding_model = (
            req_body.get("embedding_model") or 
            req.params.get("embedding_model") or 
            os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
        )

        if not collection:
            usage_msg = (
                "Missing required parameter: 'collection'\n\n"
                "Usage:\n"
                "  Query string: ?collection=your_collection_name\n"
                "  JSON body: {\"collection\": \"your_collection_name\"}\n"
                "  Environment variable: QDRANT_COLLECTION\n\n"
                "Optional parameters:\n"
                "  - base_url: URL to crawl (default: from BASE_URL env var)\n"
                "  - max_depth: Crawl depth (default: 2)\n"
                "  - embedding_model: Model name (default: from EMBEDDING_MODEL env var)"
            )
            return func.HttpResponse(
                usage_msg,
                status_code=400,
                mimetype="text/plain",
                headers=cors_headers
            )

        # Validate max_depth
        if max_depth < 0 or max_depth > 10:
            return func.HttpResponse(
                "max_depth must be between 0 and 10",
                status_code=400,
                mimetype="text/plain",
                headers=cors_headers
            )

        logging.info(f"Starting crawl: base_url={base_url}, collection={collection}, max_depth={max_depth}, model={embedding_model}")
        
        # Run crawler synchronously (Azure Functions will handle timeout)
        # For long-running operations, consider using Durable Functions or Queue triggers
        try:
            run(base_url, collection, max_depth, embedding_model)
            msg = f"✅ Completed ingestion for {base_url} into collection {collection}"
            logging.info(msg)
            return func.HttpResponse(
                msg,
                status_code=200,
                mimetype="text/plain",
                headers=cors_headers
            )
        except Exception as run_error:
            logging.exception("❌ Error during crawler execution")
            error_msg = f"Error during crawler execution: {str(run_error)}"
            return func.HttpResponse(
                error_msg,
                status_code=500,
                mimetype="text/plain",
                headers=cors_headers
            )

    except ValueError as e:
        logging.error(f"Validation error: {str(e)}")
        return func.HttpResponse(
            f"Validation Error: {str(e)}",
            status_code=400,
            mimetype="text/plain",
            headers=cors_headers
        )
    except Exception as e:
        logging.exception("❌ Error running crawler")
        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500,
            mimetype="text/plain",
            headers=cors_headers
        )
    

    

@app.function_name(name="timer_trigger_crawler")
@app.schedule(schedule="0 0 4 * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def timer_trigger(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger for IEWC Qdrant crawler - runs daily at 4 AM UTC (0 0 4 * * *)                    
    schedule = ["0 0 4 */15 * *"] Run every 15 days, schedule = ["0 0 4 * * 0"] Run every Sunday
    
    IMPORTANT: For timer triggers to work in Azure (Windows or Linux), ensure AzureWebJobsStorage is configured
    in your Function App's Application Settings with a valid storage account connection string.
    Format: DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
    Note: The "core.windows.net" endpoint is the Azure Storage endpoint and works for both Windows and Linux Function Apps.
    """
    utc_timestamp = datetime.datetime.utcnow().isoformat()
    logging.info("⏰ Timer trigger function started at %s", utc_timestamp)
    
    try:
        # Read configuration from environment variables
        base_url = os.environ.get("BASE_URL")
        if not base_url:
            raise ValueError("Required environment variable BASE_URL is not set")
        
        collection = os.environ.get("QDRANT_COLLECTION")
        if not collection:
            raise ValueError("Required environment variable QDRANT_COLLECTION is not set")
        
        max_depth = int(os.environ.get("MAX_DEPTH", "2"))
        embedding_model = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
        
        logging.info("Starting web crawler with configuration:")
        logging.info("- Base URL: %s", base_url)
        logging.info("- Collection: %s", collection)
        logging.info("- Max Depth: %d", max_depth)
        logging.info("- Embedding Model: %s", embedding_model)
        
        # Execute the crawler
        run(
            base_url=base_url,
            collection=collection,
            max_depth=max_depth,
            embedding_model=embedding_model
        )
        
        logging.info("✅ Crawler completed successfully at %s", datetime.datetime.utcnow().isoformat())
        
    except Exception as e:
        logging.error("❌ Error in timer trigger: %s", str(e), exc_info=True)
        # Don't re-raise to prevent function from being marked as failed repeatedly
        # The error is logged and can be monitored


# Implementation Plan - WARP Integration & Deployment

I will containerize the application using Docker and integrate Cloudflare WARP as a proxy service to safeguard the IP and prepare for web deployment.

## User Review Required

> [!IMPORTANT]
> This change introduces Docker. You will need Docker Desktop installed to run the `docker-compose` commands.
> The application will now use environment variables for proxy configuration instead of hardcoded Tor settings when the "Use Proxy" toggle is enabled.

## Proposed Changes

### codebase
#### [MODIFY] `gdelt_fetcher.py` & `article_scraper.py`
-   Abstract `TorManager` into a generic `ProxyManager`.
-   Read `PROXY_URL` from environment variables (e.g., `socks5://warp:1080` or `socks5://127.0.0.1:9150`).
-   If `PROXY_URL` is set, use it. If not, fallback to Tor defaults or direct connection.

### docker
#### [NEW] `Dockerfile`
-   Python 3.10-slim base image.
-   Install dependencies (`requirements.txt`).
-   Expose port 8501.

#### [NEW] `docker-compose.yml`
-   **Service `app`**: The Streamlit application.
-   **Service `warp`**: A WARP client container (using `monosense/warp-socks` or `jcardillo/warp-socks` which provides a SOCKS5 proxy on port 1080).
-   **Network**: A private bridge network connecting `app` and `warp`.

## Verification Plan

### Automated Tests
-   Build the docker containers: `docker-compose build`.
-   Run the stack: `docker-compose up`.

### Manual Verification
-   Open `http://localhost:8501`.
-   Enable "Use Tor Proxy" (renamed to "Use Secure Proxy").
-   Perform a search.
-   Verify logs show traffic routing through the proxy (WARP).

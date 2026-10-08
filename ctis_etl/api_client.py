"""HTTP Client for EU CTIS Public API with built-in retries, backoff, and pagination."""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Dict, Generator, Optional
import urllib.error
import urllib.request

from ctis_etl import config

logger = logging.getLogger(__name__)


class CTISAPIError(Exception):
    """Custom exception raised for CTIS API communication failures."""
    pass


class CTISNotFoundError(CTISAPIError):
    """Raised when a trial does not exist in CTIS (returns empty object)."""
    pass


class CTISClient:
    """Robust client interacting with CTIS Public Search & Retrieve APIs.

    Uses persistent httpx connection pooling when available, with resilient urllib fallback.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        max_retries: int = config.MAX_RETRIES,
        connect_timeout: float = config.CONNECT_TIMEOUT,
        read_timeout: float = config.READ_TIMEOUT,
        request_delay: float = config.REQUEST_DELAY_SECONDS,
    ) -> None:
        self.base_url = (base_url or config.CTIS_API_BASE_URL).rstrip("/")
        self.search_url = f"{self.base_url}/search"
        self.retrieve_url = f"{self.base_url}/retrieve"
        self.max_retries = max_retries
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.request_delay = request_delay

        # Try initializing httpx Client with connection pooling
        self._httpx_client = None
        try:
            import httpx
            self._httpx_client = httpx.Client(
                timeout=httpx.Timeout(timeout=read_timeout, connect=connect_timeout),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=30.0),
                headers=config.DEFAULT_HEADERS,
                follow_redirects=True,
            )
            logger.debug("CTISClient initialized with persistent httpx connection pool.")
        except ImportError:
            logger.debug("httpx not available; falling back to urllib.request.")

    def close(self) -> None:
        """Closes underlying HTTP client connection pool if present."""
        if self._httpx_client is not None:
            try:
                self._httpx_client.close()
            except Exception:
                pass
            self._httpx_client = None

    def __enter__(self) -> CTISClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _execute_request_with_retry(
        self,
        url: str,
        method: str = "GET",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Executes an HTTP request with connection pooling, exponential backoff, and jitter."""
        if self.request_delay > 0:
            time.sleep(self.request_delay)

        data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None

        for attempt in range(1, self.max_retries + 1):
            try:
                # Path A: httpx with connection pooling
                if self._httpx_client is not None:
                    import httpx
                    try:
                        if method == "POST":
                            resp = self._httpx_client.post(url, json=payload)
                        else:
                            resp = self._httpx_client.get(url)

                        if resp.status_code in (429, 500, 502, 503, 504):
                            raise httpx.HTTPStatusError(
                                f"Server returned {resp.status_code}",
                                request=resp.request,
                                response=resp
                            )
                        resp.raise_for_status()

                        ctype = resp.headers.get("content-type", "")
                        if "application/json" not in ctype and not ctype.startswith("application/"):
                            raise CTISAPIError(f"Unexpected non-JSON response from {url}: {resp.text[:200]}")

                        return resp.json()

                    except (httpx.HTTPStatusError, httpx.RequestError) as http_e:
                        status = getattr(getattr(http_e, "response", None), "status_code", None)
                        if attempt == self.max_retries:
                            raise CTISAPIError(f"Request failed after {attempt} attempts for {url}: {http_e}") from http_e

                        sleep_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                        logger.warning(f"HTTP {status or 'net'} error for {url}. Retrying in {sleep_time:.2f}s (Attempt {attempt}/{self.max_retries})...")
                        time.sleep(sleep_time)
                        continue

                # Path B: Standard urllib fallback
                req = urllib.request.Request(
                    url,
                    data=data_bytes,
                    headers=config.DEFAULT_HEADERS,
                    method=method,
                )
                with urllib.request.urlopen(req, timeout=self.read_timeout) as response:
                    content_type = response.headers.get("Content-Type", "")
                    if "application/json" not in content_type and not content_type.startswith("application/"):
                        body = response.read().decode("utf-8", errors="replace")
                        raise CTISAPIError(f"Unexpected non-JSON response from {url}: {body[:200]}")

                    raw_bytes = response.read()
                    return json.loads(raw_bytes.decode("utf-8"))

            except urllib.error.HTTPError as http_err:
                status = http_err.code
                if status in (429, 500, 502, 503, 504):
                    if attempt == self.max_retries:
                        raise CTISAPIError(f"HTTP {status} after {attempt} attempts for {url}") from http_err
                    sleep_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                    logger.warning(f"HTTP {status} for {url}. Retrying in {sleep_time:.2f}s (Attempt {attempt}/{self.max_retries})...")
                    time.sleep(sleep_time)
                else:
                    raise CTISAPIError(f"HTTP {status} client error for {url}") from http_err

            except (urllib.error.URLError, TimeoutError, OSError) as net_err:
                if attempt == self.max_retries:
                    raise CTISAPIError(f"Network error after {attempt} attempts for {url}: {net_err}") from net_err
                sleep_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                logger.warning(f"Network error for {url}: {net_err}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)

        raise CTISAPIError(f"Failed request to {url} after {self.max_retries} attempts.")

    def search_page(
        self,
        page: int = 1,
        size: int = 100,
        sort_property: str = "decisionDate",
        sort_direction: str = "DESC",
    ) -> Dict[str, Any]:
        """Calls POST /search for a specific 1-indexed page."""
        payload = {
            "pagination": {"page": page, "size": size},
            "sort": {"property": sort_property, "direction": sort_direction},
            "searchCriteria": {},
        }
        return self._execute_request_with_retry(self.search_url, method="POST", payload=payload)

    def iterate_search_trials(
        self,
        page_size: int = 100,
        sort_property: str = "decisionDate",
        max_pages: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Yields each trial summary item from the Search API sequentially across pages."""
        current_page = 1
        while True:
            resp = self.search_page(page=current_page, size=page_size, sort_property=sort_property)
            data_items = resp.get("data", [])
            pagination = resp.get("pagination", {})
            total_pages = pagination.get("totalPages", current_page)

            for item in data_items:
                yield item

            if not data_items or current_page >= total_pages:
                break

            if max_pages and current_page >= max_pages:
                break

            current_page += 1

    def retrieve_trial(self, ct_number: str) -> Dict[str, Any]:
        """Calls GET /retrieve/{ctNumber} and returns full raw trial dossier."""
        url = f"{self.retrieve_url}/{ct_number}"
        data = self._execute_request_with_retry(url, method="GET")

        # CTIS returns empty {} on 200 OK when trial doesn't exist
        if not data or not data.get("ctNumber"):
            raise CTISNotFoundError(f"Trial {ct_number} was not found or returned empty dossier.")

        return data

# EU CTIS Public REST API - Specification & Live Quirks

**Document ID:** API-001  
**Version:** 1.0  
**Target:** EMA CTIS Public Portal REST API (`https://euclinicaltrials.eu/ctis-public-api`)

---

## 1. Overview & Required Request Headers

The CTIS API is exposed by the European Medicines Agency (EMA). It does not require API keys or OAuth for basic public searches, but **strictly requires standard browser-like headers**:

```http
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
Content-Type: application/json
Accept: application/json
```

---

## 2. Search Endpoint: `POST /search`

Used to query, page through, and filter the global catalog of clinical trials.

* **URL:** `https://euclinicaltrials.eu/ctis-public-api/search`
* **Method:** `POST` *(Note: `GET` returns HTTP 405 Method Not Allowed)*

### Critical Live API Quirks:
1. **1-Indexed Pagination:** The API expects `"page": 1` for the first page. Supplying `"page": 0` returns `totalRecords: 0` and an empty list.
2. **Mandatory `"searchCriteria"`:** The POST body **must** contain `"searchCriteria": {}`. If omitted, the API returns zero records.
3. **Envelope Wrapping:** The endpoint does not return a raw array. Items are wrapped inside `response["data"]`.
4. **Sorting:** Sorting by `"decisionDate"` or `"lastPublicationUpdate"` with `"direction": "DESC"` returns the most recently approved or amended trials first.

### Request Body:
```json
{
  "pagination": {
    "page": 1,
    "size": 100
  },
  "sort": {
    "property": "decisionDate",
    "direction": "DESC"
  },
  "searchCriteria": {}
}
```

### Response Structure:
```json
{
  "showWarning": true,
  "pagination": {
    "totalRecords": 12573,
    "currentPage": 1,
    "totalPages": 126,
    "nextPage": true,
    "prevPage": false
  },
  "data": [
    {
      "ctNumber": "2026-527084-15-00",
      "ctStatus": 2,
      "ctTitle": "Evaluation of Thrombin Generation...",
      "decisionDateOverall": "07/10/2026",
      "decisionDate": "HU: 07/10/2026",
      "lastUpdated": "07/10/2026",
      "lastPublicationUpdate": "08/10/2026",
      "sponsor": "University Of Debrecen",
      "trialPhase": "Therapeutic use (Phase IV)"
    }
  ]
}
```

### Date Parsing Gotcha in Search API:
* Dates in `data[]` are formatted as `DD/MM/YYYY` strings (e.g. `"07/10/2026"`).
* The field `decisionDate` may contain country prefixes (e.g., `"HU: 07/10/2026"`). The pipeline uses regex `re.sub(r"^[A-Za-z]{2,3}:\s*", "", date_str)` before parsing.

---

## 3. Retrieve Endpoint: `GET /retrieve/{ctNumber}`

Fetches the complete, deeply nested dossier for a given trial identifier.

* **URL:** `https://euclinicaltrials.eu/ctis-public-api/retrieve/{ctNumber}`
* **Method:** `GET`
* **Example:** `https://euclinicaltrials.eu/ctis-public-api/retrieve/2026-527084-15-00`

### Critical Live API Quirks:
1. **Missing Trials Return HTTP 200 with `{}`:** If a trial identifier does not exist, the API returns `200 OK` with an empty JSON object `{}` (not HTTP 404). Always verify `data.get("ctNumber") == ctNumber`.
2. **HTML Response on Gateway Errors:** Malformed URLs or internal errors can produce HTML pages with status `200 OK`. The client verifies that `Content-Type` contains `application/json` and that JSON decoding succeeds.
3. **Payload Latency:** Dossiers range from 50KB to 2MB+ with extensive sponsor, site, and document arrays. The client configures a connect timeout of 10s and a read timeout of 30s.
4. **Timestamps:** Unlike the search endpoint, the retrieve endpoint uses ISO-8601 timestamps (e.g. `"2026-10-07T15:43:07.693"` and `"2026-10-08T03:32:46.650903775"`).

---

## 4. Regulatory Documents & PDF Downloads

* In `GET /retrieve/{ctNumber}`, the `documents` list contains metadata for attached public submissions (e.g. Protocol, Subject Information Sheet).
* **Document Attributes:** `title`, `uuid`, `documentType`, `documentTypeLabel`, `languageCode`, `fileType`, `manualVersion`.
* **Direct PDF Downloads:** The direct download URLs (e.g. `/ctis-public-api/documents/{uuid}`) return `403 Forbidden` unless accessed via an active session established through the public web portal. The pipeline stores the complete document metadata array inside `trial_documents.json`.

---

## 5. Rate Limiting & Backoff Guidelines

* CTIS gateway applies rate limits if flooded by too many requests per second.
* **Worker Recommendation:** Capped at `5` concurrent worker threads (`MAX_WORKERS=5`).
* **Backoff Strategy:** Jittered exponential backoff is triggered on HTTP 429, 500, 502, 503, 504:
  $$\text{wait\_time} = 2^{\text{attempt}} + \text{uniform}(0.1, 1.0)$$
* Trials failing after 3 attempts are quarantined and logged to prevent blocking the worker pool.

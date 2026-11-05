"""API Client module for ANZCTR (Australian New Zealand Clinical Trials Registry) Webservices.

Exposes operations to query and retrieve raw XML responses from:
1. Search (AnzctrTrialSearch)
2. result (ActSearchResults)
3. detail (AnzctrTrialDetails)
4. detailv2 (AnzctrTrialDetailsV2)
5. detailresult (ActMoreInfo)
"""

from __future__ import annotations

import html
import logging
import time
from typing import Any, Dict, Optional

import requests

from anz_etl import config

logger = logging.getLogger(__name__)

SOAP_NS = "http://anzctr.org.au/WebServices/AnzctrWebServices"


class AnzctrClient:
    """Client for querying ANZCTR ASMX SOAP / Web Service endpoints."""

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        self.base_url = (base_url or config.ANZCTR_WS_URL).rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ANZCTR-Raw-ETL-Pipeline/1.0 (+https://clinicaltrialsaustralia.io/methodology)",
        })

    def check_health(self) -> Dict[str, Any]:
        """Verifies endpoint accessibility by fetching the WSDL contract."""
        start_time = time.time()
        try:
            resp = self.session.get(f"{self.base_url}?WSDL", timeout=self.timeout)
            duration = round(time.time() - start_time, 2)
            is_ok = resp.status_code in (200, 302, 403)
            return {
                "status": "UP" if is_ok else "DEGRADED",
                "status_code": resp.status_code,
                "latency_seconds": duration,
                "endpoint": self.base_url,
            }
        except Exception as e:
            logger.warning(f"Health check connection failed: {e}")
            return {
                "status": "UNREACHABLE",
                "error": str(e),
                "endpoint": self.base_url,
            }

    def _call_soap(self, action_name: str, body_xml: str) -> str:
        """Constructs and executes a SOAP 1.1 request returning the exact raw XML response."""
        soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{action_name} xmlns="{SOAP_NS}">
{body_xml}
    </{action_name}>
  </soap:Body>
</soap:Envelope>"""

        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f'"{SOAP_NS}/{action_name}"',
        }

        resp = self.session.post(
            self.base_url,
            data=soap_envelope.encode("utf-8"),
            headers=headers,
            timeout=self.timeout,
        )
        return resp.text

    def fetch_raw_search(
        self,
        search_text: str = "",
        page_number: int = 1,
        paging: int = 20,
        purpose_of_study: str = "",
        recruitment_status: str = "",
        phase: str = "",
        study_type: str = "",
        get_actrn_only: bool = False,
    ) -> str:
        """Operation 1: Search - AnzctrTrialSearch (raw XML)."""
        body = f"""      <searchText>{html.escape(search_text)}</searchText>
      <purposeOfStudy>{html.escape(purpose_of_study)}</purposeOfStudy>
      <recruitmentStatus>{html.escape(recruitment_status)}</recruitmentStatus>
      <phase>{html.escape(phase)}</phase>
      <ethicsApproval></ethicsApproval>
      <gender></gender>
      <healthyVolunteers></healthyVolunteers>
      <healthConditions></healthConditions>
      <recruitmentSites></recruitmentSites>
      <ageGroup></ageGroup>
      <recruitmentCountries></recruitmentCountries>
      <studyType>{html.escape(study_type)}</studyType>
      <conditionCategory></conditionCategory>
      <conditionCode></conditionCode>
      <interventionCode></interventionCode>
      <paging>{paging}</paging>
      <pageNumber>{page_number}</pageNumber>
      <getActrnOnly>{str(get_actrn_only).lower()}</getActrnOnly>"""
        return self._call_soap("AnzctrTrialSearch", body)

    def fetch_raw_result(
        self,
        keyword: str = "",
        from_date: str = "",
        to_date: str = "",
        page_number: int = 1,
        paging: int = 20,
    ) -> str:
        """Operation 2: result - ActSearchResults (raw XML)."""
        body = f"""      <keyword>{html.escape(keyword)}</keyword>
      <cancerType></cancerType>
      <cancerStage></cancerStage>
      <purpose></purpose>
      <phase></phase>
      <intervention></intervention>
      <fromDate>{html.escape(from_date)}</fromDate>
      <toDate>{html.escape(to_date)}</toDate>
      <sponsor></sponsor>
      <fundingSource></fundingSource>
      <ageGroup></ageGroup>
      <gender></gender>
      <location></location>
      <ethicsApproval></ethicsApproval>
      <recruitmentStatus></recruitmentStatus>
      <paging>{paging}</paging>
      <pageNumber>{page_number}</pageNumber>"""
        return self._call_soap("ActSearchResults", body)

    def fetch_raw_detail(self, ids: str) -> str:
        """Operation 3: detail - AnzctrTrialDetails (raw XML)."""
        body = f"""      <ids>{html.escape(ids)}</ids>"""
        return self._call_soap("AnzctrTrialDetails", body)

    def fetch_raw_detailv2(self, ids: str, version: int = 1) -> str:
        """Operation 4: detailv2 - AnzctrTrialDetailsV2 (raw XML)."""
        body = f"""      <ids>{html.escape(ids)}</ids>
      <version>{version}</version>"""
        return self._call_soap("AnzctrTrialDetailsV2", body)

    def fetch_raw_detailresult(self, trial_id: str) -> str:
        """Operation 5: detailresult - ActMoreInfo (raw XML)."""
        body = f"""      <id>{html.escape(trial_id)}</id>"""
        return self._call_soap("ActMoreInfo", body)

    def fetch_all_raw(
        self,
        actrn: str,
        search_keyword: str = "",
    ) -> Dict[str, str]:
        """Collects raw XML payloads from all 5 initial testing endpoints."""
        logger.info(f"Collecting raw payloads for target ACTRN='{actrn}' and keyword='{search_keyword}'")
        raw_collection: Dict[str, str] = {}

        # 1. Search
        try:
            logger.info("Calling endpoint [1/5]: Search (AnzctrTrialSearch)...")
            raw_collection["Search"] = self.fetch_raw_search(search_text=search_keyword or actrn)
        except Exception as e:
            logger.error(f"Search API call failed: {e}")
            raw_collection["Search"] = f"<!-- Error: {e} -->"

        # 2. result
        try:
            logger.info("Calling endpoint [2/5]: result (ActSearchResults)...")
            raw_collection["result"] = self.fetch_raw_result(keyword=search_keyword or actrn)
        except Exception as e:
            logger.error(f"result API call failed: {e}")
            raw_collection["result"] = f"<!-- Error: {e} -->"

        # 3. detail
        try:
            logger.info("Calling endpoint [3/5]: detail (AnzctrTrialDetails)...")
            raw_collection["detail"] = self.fetch_raw_detail(ids=actrn)
        except Exception as e:
            logger.error(f"detail API call failed: {e}")
            raw_collection["detail"] = f"<!-- Error: {e} -->"

        # 4. detailv2
        try:
            logger.info("Calling endpoint [4/5]: detailv2 (AnzctrTrialDetailsV2)...")
            raw_collection["detailv2"] = self.fetch_raw_detailv2(ids=actrn, version=1)
        except Exception as e:
            logger.error(f"detailv2 API call failed: {e}")
            raw_collection["detailv2"] = f"<!-- Error: {e} -->"

        # 5. detailresult
        try:
            logger.info("Calling endpoint [5/5]: detailresult (ActMoreInfo)...")
            raw_collection["detailresult"] = self.fetch_raw_detailresult(trial_id=actrn)
        except Exception as e:
            logger.error(f"detailresult API call failed: {e}")
            raw_collection["detailresult"] = f"<!-- Error: {e} -->"

        return raw_collection

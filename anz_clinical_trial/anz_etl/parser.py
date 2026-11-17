"""Data normalization and parser module for ANZCTR trial records.

Splits incoming ANZCTR XML or dictionary records into 6 standardized domain JSON payloads:
1. meta_data.json
2. summary.json
3. full_trial_information.json
4. trial_documents.json
5. trial_results.json
6. locations_and_contact_points.json
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from anz_etl.storage import _xml_to_dict

logger = logging.getLogger(__name__)


def _extract_trial_node(obj: Any) -> Dict[str, Any]:
    """Recursively traverses a parsed XML dictionary tree to find the trial element or detect SOAP Faults."""
    if not isinstance(obj, dict):
        return {}

    # Check for SOAP Faults at the current level
    if "Fault" in obj:
        fault = obj["Fault"]
        fstr = fault.get("faultstring") if isinstance(fault, dict) else str(fault)
        raise ValueError(f"SOAP Fault returned by upstream ANZCTR API: {fstr}")

    # Check case-insensitive key mappings
    keys_lower = {k.lower(): k for k in obj.keys()}
    if "fault" in keys_lower:
        fault_val = obj[keys_lower["fault"]]
        fstr = fault_val.get("faultstring") if isinstance(fault_val, dict) else str(fault_val)
        raise ValueError(f"SOAP Fault returned by upstream ANZCTR API: {fstr}")

    # If this dictionary has trial markers, return it
    if "publictitle" in keys_lower or "scientifictitle" in keys_lower or ("actrn" in keys_lower and len(obj) > 3):
        return obj

    # Search nested children
    for k, v in obj.items():
        if isinstance(v, dict):
            res = _extract_trial_node(v)
            if res:
                return res
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    res = _extract_trial_node(item)
                    if res:
                        return res
    return obj


def _get_field(d: Dict[str, Any], *candidate_keys: str, default: Any = "") -> Any:
    """Case-insensitive getter for candidate dictionary keys."""
    lookup = {k.lower().replace("_", ""): v for k, v in d.items()}
    for key in candidate_keys:
        normalized = key.lower().replace("_", "")
        if normalized in lookup and lookup[normalized] is not None:
            val = lookup[normalized]
            if isinstance(val, str):
                return val.strip()
            return val
    return default


def is_valid_trial_payload(raw_data: Any) -> tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """Validates if raw data contains a genuine clinical trial record without errors or SOAP faults.

    Returns:
        (is_valid, error_reason, trial_dict)
    """
    if not raw_data:
        return False, "Payload is empty or None", None

    trial_node: Dict[str, Any] = {}

    if isinstance(raw_data, str):
        s = raw_data.strip()
        if not s:
            return False, "Payload string is empty or whitespace", None

        s_lower = s.lower()
        # Check for SOAP Faults and exceptions in text
        if "<soap:fault" in s_lower or "<faultcode" in s_lower:
            return False, "Payload is a SOAP Fault XML", None
        if "user unauthorised" in s_lower or "user unauthorized" in s_lower or "httpresponseexception" in s_lower:
            return False, "Payload indicates unauthorized or server exception", None
        if "managed challenge" in s_lower or "under attack" in s_lower or "<title>403" in s_lower:
            return False, "Payload is a WAF/firewall challenge or 403 page", None
        if "invalidoperationexception" in s_lower:
            return False, "Payload is an InvalidOperationException", None

        try:
            parsed = _xml_to_dict(s)
            trial_node = _extract_trial_node(parsed)
        except Exception as e:
            return False, f"Failed parsing XML payload: {e}", None

    elif isinstance(raw_data, dict):
        if "raw_content" in raw_data and isinstance(raw_data["raw_content"], str):
            return is_valid_trial_payload(raw_data["raw_content"])
        try:
            trial_node = _extract_trial_node(raw_data)
        except Exception as e:
            return False, f"Failed extracting trial node: {e}", None
    else:
        return False, f"Unsupported payload type: {type(raw_data).__name__}", None

    if not trial_node:
        return False, "No clinical trial node found in payload", None

    # Verify at least one essential title field is present and non-empty
    pub_title = _get_field(trial_node, "publictitle", "public_title", "studytitle", "title")
    sci_title = _get_field(trial_node, "scientifictitle", "scientific_title")

    if not pub_title and not sci_title:
        return False, "Trial has neither public_title nor scientific_title (empty trial data)", None

    return True, None, trial_node


def parse_anzctr_trial(raw_data: Any, actrn: str) -> Dict[str, Dict[str, Any]]:
    """Splits raw trial dictionary, XML string, or wrapper payload into 6 standardized domain payloads.

    Validates trial content and raises ValueError if invalid or contains an error.
    """
    is_valid, reason, trial_dict = is_valid_trial_payload(raw_data)
    if not is_valid or trial_dict is None:
        raise ValueError(f"Invalid trial payload for {actrn}: {reason}")

    # Domain 1: meta_data
    meta_data = {
        "actrn": actrn,
        "registry": "ANZCTR",
        "public_title": _get_field(trial_dict, "publictitle", "public_title"),
        "scientific_title": _get_field(trial_dict, "scientifictitle", "scientific_title"),
        "registration_date": _get_field(trial_dict, "dateregistered", "date_registered"),
        "last_updated_date": _get_field(trial_dict, "dateupdated", "date_last_updated"),
        "recruitment_status": _get_field(trial_dict, "recruitmentstatus", "recruitment_status"),
        "trial_type": _get_field(trial_dict, "trialtype", "trial_type", default="Interventional"),
    }

    # Domain 2: summary
    summary = {
        "actrn": actrn,
        "brief_summary": _get_field(trial_dict, "briefsummary", "brief_summary"),
        "primary_sponsor": _get_field(trial_dict, "primarysponsor", "primary_sponsor"),
        "health_condition": _get_field(trial_dict, "healthcondition", "health_condition"),
        "intervention": _get_field(trial_dict, "intervention", "intervention_code"),
        "target_sample_size": _get_field(trial_dict, "targetsamplesize", "target_sample_size"),
        "anticipated_start_date": _get_field(trial_dict, "anticipatedstartdate", "anticipated_start_date"),
        "actual_start_date": _get_field(trial_dict, "actualstartdate", "actual_start_date"),
    }

    # Domain 3: full_trial_information
    full_trial_information = {
        "actrn": actrn,
        "study_type": _get_field(trial_dict, "studytype", "study_type"),
        "phase": _get_field(trial_dict, "phase", default="Not Applicable"),
        "study_design": _get_field(trial_dict, "studydesign", "study_design"),
        "inclusion_criteria": _get_field(trial_dict, "inclusioncriteria", "inclusion_criteria"),
        "exclusion_criteria": _get_field(trial_dict, "exclusioncriteria", "exclusion_criteria"),
        "primary_outcomes": _get_field(trial_dict, "primaryoutcomes", "primary_outcomes", default=[]),
        "secondary_outcomes": _get_field(trial_dict, "secondaryoutcomes", "secondary_outcomes", default=[]),
        "ethics_approval": _get_field(trial_dict, "ethicsapproval", "ethics_approval"),
    }

    # Domain 4: trial_documents
    trial_documents = {
        "actrn": actrn,
        "documents": _get_field(trial_dict, "documents", "attachments", default=[]),
        "protocols": _get_field(trial_dict, "protocols", default=[]),
        "source_url": f"https://www.anzctr.org.au/Trial/Registration/TrialReview.aspx?id={actrn}",
    }

    # Domain 5: trial_results
    trial_results = {
        "actrn": actrn,
        "results_available": bool(_get_field(trial_dict, "results_available", "hasresults", default=False)),
        "results_summary": _get_field(trial_dict, "results_summary", "resultssummary"),
        "publications": _get_field(trial_dict, "publications", default=[]),
    }

    # Domain 6: locations_and_contact_points
    locations_and_contact_points = {
        "actrn": actrn,
        "countries": _get_field(trial_dict, "countries", default=["Australia"]),
        "locations": _get_field(trial_dict, "locations", "sites", default=[]),
        "principal_investigator": _get_field(trial_dict, "principalinvestigator", "principal_investigator", default={}),
        "public_queries_contact": _get_field(trial_dict, "contact", "public_contact", default={}),
    }

    return {
        "meta_data": meta_data,
        "summary": summary,
        "full_trial_information": full_trial_information,
        "trial_documents": trial_documents,
        "trial_results": trial_results,
        "locations_and_contact_points": locations_and_contact_points,
    }

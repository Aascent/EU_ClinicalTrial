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
from typing import Any, Dict

logger = logging.getLogger(__name__)


def parse_anzctr_trial(raw_dict: Dict[str, Any], actrn: str) -> Dict[str, Dict[str, Any]]:
    """Splits raw trial dictionary into 6 standardized domain payloads."""
    # Domain 1: meta_data
    meta_data = {
        "actrn": actrn,
        "registry": "ANZCTR",
        "public_title": raw_dict.get("publictitle") or raw_dict.get("public_title") or "",
        "scientific_title": raw_dict.get("scientifictitle") or raw_dict.get("scientific_title") or "",
        "registration_date": raw_dict.get("date_registered") or raw_dict.get("dateregistered") or "",
        "last_updated_date": raw_dict.get("date_last_updated") or raw_dict.get("dateupdated") or "",
        "recruitment_status": raw_dict.get("recruitmentstatus") or raw_dict.get("recruitment_status") or "",
        "trial_type": raw_dict.get("trialtype") or raw_dict.get("trial_type") or "Interventional",
    }

    # Domain 2: summary
    summary = {
        "actrn": actrn,
        "brief_summary": raw_dict.get("briefsummary") or raw_dict.get("brief_summary") or "",
        "primary_sponsor": raw_dict.get("primarysponsor") or raw_dict.get("primary_sponsor") or "",
        "health_condition": raw_dict.get("healthcondition") or raw_dict.get("health_condition") or "",
        "intervention": raw_dict.get("intervention") or raw_dict.get("intervention_code") or "",
        "target_sample_size": raw_dict.get("targetsamplesize") or raw_dict.get("target_sample_size") or "",
        "anticipated_start_date": raw_dict.get("anticipatedstartdate") or "",
        "actual_start_date": raw_dict.get("actualstartdate") or "",
    }

    # Domain 3: full_trial_information
    full_trial_information = {
        "actrn": actrn,
        "study_type": raw_dict.get("studytype") or raw_dict.get("study_type") or "",
        "phase": raw_dict.get("phase") or "Not Applicable",
        "study_design": raw_dict.get("studydesign") or raw_dict.get("study_design") or "",
        "inclusion_criteria": raw_dict.get("inclusioncriteria") or raw_dict.get("inclusion_criteria") or "",
        "exclusion_criteria": raw_dict.get("exclusioncriteria") or raw_dict.get("exclusion_criteria") or "",
        "primary_outcomes": raw_dict.get("primaryoutcomes") or raw_dict.get("primary_outcomes") or [],
        "secondary_outcomes": raw_dict.get("secondaryoutcomes") or raw_dict.get("secondary_outcomes") or [],
        "ethics_approval": raw_dict.get("ethicsapproval") or raw_dict.get("ethics_approval") or "",
    }

    # Domain 4: trial_documents
    trial_documents = {
        "actrn": actrn,
        "documents": raw_dict.get("documents") or raw_dict.get("attachments") or [],
        "protocols": raw_dict.get("protocols") or [],
        "source_url": f"https://www.anzctr.org.au/Trial/Registration/TrialReview.aspx?id={actrn}",
    }

    # Domain 5: trial_results
    trial_results = {
        "actrn": actrn,
        "results_available": bool(raw_dict.get("results_available") or raw_dict.get("hasresults")),
        "results_summary": raw_dict.get("results_summary") or "",
        "publications": raw_dict.get("publications") or [],
    }

    # Domain 6: locations_and_contact_points
    locations_and_contact_points = {
        "actrn": actrn,
        "countries": raw_dict.get("countries") or ["Australia"],
        "locations": raw_dict.get("locations") or raw_dict.get("sites") or [],
        "principal_investigator": raw_dict.get("principalinvestigator") or raw_dict.get("principal_investigator") or {},
        "public_queries_contact": raw_dict.get("contact") or raw_dict.get("public_contact") or {},
    }

    return {
        "meta_data": meta_data,
        "summary": summary,
        "full_trial_information": full_trial_information,
        "trial_documents": trial_documents,
        "trial_results": trial_results,
        "locations_and_contact_points": locations_and_contact_points,
    }


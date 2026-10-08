"""Parser module to split monolithic CTIS retrieve responses into 6 domain JSON payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _safe_dict(obj: Any, key: Optional[str] = None) -> Dict[str, Any]:
    """Safely extracts a nested dictionary, returning {} if None or wrong type."""
    target = obj.get(key) if key and isinstance(obj, dict) else obj
    return target if isinstance(target, dict) else {}


def _safe_list(obj: Any, key: Optional[str] = None) -> List[Any]:
    """Safely extracts a nested list, returning [] if None or wrong type."""
    target = obj.get(key) if key and isinstance(obj, dict) else obj
    return target if isinstance(target, list) else []


def parse_trial_dossier(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Splits raw trial dictionary into 6 structured target files with resilient fallbacks.

    Handles schema evolution, renamed/missing keys, and unexpected nulls gracefully.
    """
    if not isinstance(raw_data, dict):
        raise ValueError(f"Expected dict for trial dossier, got {type(raw_data).__name__}")

    ct_number = str(raw_data.get("ctNumber") or "").strip()
    auth_app = _safe_dict(raw_data, "authorizedApplication")
    auth_part_i = _safe_dict(auth_app, "authorizedPartI")
    auth_parts_ii = _safe_list(auth_app, "authorizedPartsII")

    trial_details = _safe_dict(auth_part_i, "trialDetails")
    trial_info = _safe_dict(trial_details, "trialInformation")
    trial_category = _safe_dict(trial_info, "trialCategory")

    # 1. meta_data.json
    meta_data = {
        "ctNumber": ct_number,
        "ctStatus": raw_data.get("ctStatus"),
        "decisionDate": raw_data.get("decisionDate"),
        "publishDate": raw_data.get("publishDate"),
        "ctPublicStatusCode": raw_data.get("ctPublicStatusCode"),
        "trialRegion": raw_data.get("trialRegion"),
        "trialRegionCode": raw_data.get("trialRegionCode"),
        "events": raw_data.get("events") if raw_data.get("events") is not None else [],
        "correctiveMeasures": raw_data.get("correctiveMeasures") if raw_data.get("correctiveMeasures") is not None else [],
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": "1.0.0",
    }

    # 2. summary.json
    summary = {
        "ctNumber": ct_number,
        "clinicalTrialIdentifiers": trial_details.get("clinicalTrialIdentifiers") or {},
        "sponsors": _safe_list(auth_part_i, "sponsors"),
        "trialPhase": trial_category.get("trialPhase"),
        "medicalConditions": _safe_list(auth_part_i, "medicalConditions"),
        "therapeuticAreas": _safe_list(auth_part_i, "therapeuticAreas"),
    }

    # 3. full_trial_information.json (Entire Part I preserved as-is without schema loss)
    full_trial_information = auth_part_i

    # 4. trial_documents.json
    trial_documents = _safe_list(raw_data, "documents")

    # 5. trial_results.json
    trial_results = _safe_dict(raw_data, "results")

    # 6. locations_and_contact_points.json
    sponsors = _safe_list(auth_part_i, "sponsors")
    sponsor_contacts: List[Dict[str, Any]] = []
    for sp in sponsors:
        if isinstance(sp, dict):
            sponsor_contacts.append({
                "sponsorOrganisation": sp.get("organisationName") or sp.get("sponsorName"),
                "publicContacts": _safe_list(sp, "publicContacts"),
                "scientificContacts": _safe_list(sp, "scientificContacts"),
            })

    locations_and_contacts = {
        "ctNumber": ct_number,
        "memberStates": auth_parts_ii,
        "sponsorContacts": sponsor_contacts,
    }

    return {
        "meta_data.json": meta_data,
        "summary.json": summary,
        "full_trial_information.json": full_trial_information,
        "trial_documents.json": trial_documents,
        "trial_results.json": trial_results,
        "locations_and_contact_points.json": locations_and_contacts,
    }



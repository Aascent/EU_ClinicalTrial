"""Parser module to split monolithic CTIS retrieve responses into 6 domain JSON payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


def parse_trial_dossier(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Splits raw trial dictionary into 6 structured target files.

    Returns:
        Dict mapping filename to payload:
            - 'meta_data.json'
            - 'summary.json'
            - 'full_trial_information.json'
            - 'trial_documents.json'
            - 'trial_results.json'
            - 'locations_and_contact_points.json'
    """
    ct_number = raw_data.get("ctNumber", "")
    auth_app = raw_data.get("authorizedApplication") or {}
    auth_part_i = auth_app.get("authorizedPartI") or {}
    auth_parts_ii = auth_app.get("authorizedPartsII") or []

    trial_details = auth_part_i.get("trialDetails") or {}
    trial_info = trial_details.get("trialInformation") or {}
    trial_category = trial_info.get("trialCategory") or {}

    # 1. meta_data.json
    meta_data = {
        "ctNumber": ct_number,
        "ctStatus": raw_data.get("ctStatus"),
        "decisionDate": raw_data.get("decisionDate"),
        "publishDate": raw_data.get("publishDate"),
        "ctPublicStatusCode": raw_data.get("ctPublicStatusCode"),
        "trialRegion": raw_data.get("trialRegion"),
        "trialRegionCode": raw_data.get("trialRegionCode"),
        "events": raw_data.get("events", []),
        "correctiveMeasures": raw_data.get("correctiveMeasures"),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": "1.0.0",
    }

    # 2. summary.json
    summary = {
        "ctNumber": ct_number,
        "clinicalTrialIdentifiers": trial_details.get("clinicalTrialIdentifiers", []),
        "sponsors": auth_part_i.get("sponsors", []),
        "trialPhase": trial_category.get("trialPhase"),
        "medicalConditions": auth_part_i.get("medicalConditions", []),
        "therapeuticAreas": auth_part_i.get("therapeuticAreas", []),
    }

    # 3. full_trial_information.json
    full_trial_information = auth_part_i

    # 4. trial_documents.json
    trial_documents = raw_data.get("documents") if raw_data.get("documents") is not None else []

    # 5. trial_results.json
    trial_results = raw_data.get("results") if raw_data.get("results") is not None else {}

    # 6. locations_and_contact_points.json
    sponsors = auth_part_i.get("sponsors") or []
    sponsor_contacts: List[Dict[str, Any]] = []
    for sp in sponsors:
        sponsor_contacts.append({
            "sponsorOrganisation": sp.get("organisationName") or sp.get("sponsorName"),
            "publicContacts": sp.get("publicContacts", []),
            "scientificContacts": sp.get("scientificContacts", []),
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

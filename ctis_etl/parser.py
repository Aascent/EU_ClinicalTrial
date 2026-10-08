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


def generate_gold_analytics(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Generates a flattened, high-value Gold layer record for analytical and BI consumption.

    Extracts core dimensional attributes across clinical, pharmacological, geographic,
    and regulatory domains into a clean, queryable tabular structure.
    """
    if not isinstance(raw_data, dict):
        return {}

    ct_number = str(raw_data.get("ctNumber") or "").strip()
    auth_app = _safe_dict(raw_data, "authorizedApplication")
    auth_part_i = _safe_dict(auth_app, "authorizedPartI")
    auth_parts_ii = _safe_list(auth_app, "authorizedPartsII")

    trial_details = _safe_dict(auth_part_i, "trialDetails")
    trial_info = _safe_dict(trial_details, "trialInformation")
    trial_category = _safe_dict(trial_info, "trialCategory")
    identifiers = _safe_dict(trial_details, "clinicalTrialIdentifiers")

    # Phase mapping
    phase_code = str(trial_category.get("trialPhase") or "")
    phase_mapping = {
        "1": "Phase I",
        "2": "Phase II",
        "3": "Phase III",
        "4": "Phase IV",
        "5": "Phase I/Phase II",
        "6": "Phase IV (Therapeutic use)",
    }
    phase_label = phase_mapping.get(phase_code, phase_code)

    # Sponsors
    sponsors_raw = _safe_list(auth_part_i, "sponsors")
    sponsors: List[str] = []
    sponsor_types: List[str] = []
    for sp in sponsors_raw:
        if isinstance(sp, dict):
            org = _safe_dict(sp, "organisation")
            name = org.get("name") or sp.get("organisationName") or sp.get("sponsorName")
            if name and name not in sponsors:
                sponsors.append(name)
            stype = org.get("type") or sp.get("commercial")
            if stype and stype not in sponsor_types:
                sponsor_types.append(stype)

    # Medicinal Products & Active Substances
    products_raw = _safe_list(auth_part_i, "products")
    active_substances = set()
    product_names = []
    for p in products_raw:
        if isinstance(p, dict):
            pname = p.get("productName")
            if pname and pname not in product_names:
                product_names.append(pname)
            dict_info = _safe_dict(p, "productDictionaryInfo")
            as_name = dict_info.get("activeSubstanceName")
            if as_name:
                for item in str(as_name).split(","):
                    item_clean = item.strip()
                    if item_clean:
                        active_substances.add(item_clean)

    # Geographic footprint & Sites
    countries: List[str] = []
    sites_count = 0
    total_subjects = 0
    for part in auth_parts_ii:
        if isinstance(part, dict):
            msc = _safe_dict(part, "mscInfo")
            cname = msc.get("mscCountry") or msc.get("countryName") or msc.get("countryCode")
            if cname and cname not in countries:
                countries.append(cname)
            sites = _safe_list(part, "trialSites")
            sites_count += len(sites)
            subj = part.get("recruitmentSubjectCount")
            if subj:
                try:
                    total_subjects += int(subj)
                except (ValueError, TypeError):
                    pass

    # Medical Conditions & Therapeutic Areas
    med_conditions_raw = _safe_list(auth_part_i, "medicalConditions")
    med_conditions = [
        mc.get("name") or mc.get("medicalCondition") or str(mc)
        for mc in med_conditions_raw
        if isinstance(mc, (dict, str))
    ]
    ther_areas_raw = _safe_list(auth_part_i, "therapeuticAreas")
    ther_areas = [
        ta.get("name") or ta.get("therapeuticArea") or str(ta)
        for ta in ther_areas_raw
        if isinstance(ta, (dict, str))
    ]

    # Eligibility & Endpoints
    eligibility = _safe_dict(trial_info, "eligibilityCriteria")
    inclusions = [
        ic.get("principalInclusionCriteria")
        for ic in _safe_list(eligibility, "principalInclusionCriteria")
        if isinstance(ic, dict) and ic.get("principalInclusionCriteria")
    ]
    exclusions = [
        ec.get("principalExclusionCriteria")
        for ec in _safe_list(eligibility, "principalExclusionCriteria")
        if isinstance(ec, dict) and ec.get("principalExclusionCriteria")
    ]

    endpoints_obj = _safe_dict(trial_info, "endPoint")
    primary_endpoints = [
        ep.get("endPoint")
        for ep in _safe_list(endpoints_obj, "primaryEndPoints")
        if isinstance(ep, dict) and ep.get("endPoint")
    ]

    duration_obj = _safe_dict(trial_info, "trialDuration")

    return {
        "ct_number": ct_number,
        "full_title": identifiers.get("fullTitle") or identifiers.get("publicTitle") or raw_data.get("ctTitle"),
        "status": raw_data.get("ctStatus"),
        "status_code": raw_data.get("ctPublicStatusCode"),
        "trial_phase_code": phase_code,
        "trial_phase_label": phase_label,
        "is_low_intervention": trial_category.get("isLowIntervention", False),
        "trial_region": raw_data.get("trialRegion"),
        "decision_date": raw_data.get("decisionDate"),
        "publish_date": raw_data.get("publishDate"),
        "sponsors": sponsors,
        "sponsor_types": sponsor_types,
        "active_substances": sorted(list(active_substances)),
        "product_names": product_names,
        "products_count": len(products_raw),
        "participating_countries": countries,
        "trial_sites_count": sites_count,
        "total_recruitment_subjects": total_subjects,
        "medical_conditions": med_conditions,
        "therapeutic_areas": ther_areas,
        "primary_endpoints": primary_endpoints,
        "inclusion_criteria_count": len(inclusions),
        "exclusion_criteria_count": len(exclusions),
        "documents_count": len(_safe_list(raw_data, "documents")),
        "has_results": bool(_safe_dict(raw_data, "results")),
        "estimated_start_date": duration_obj.get("estimatedRecruitmentStartDate"),
        "estimated_end_date": duration_obj.get("estimatedEndDate"),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": "gold",
    }


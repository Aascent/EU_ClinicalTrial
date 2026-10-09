"""Pydantic data models for CTIS schema validation."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class SearchPagination(BaseModel):
    """Search endpoint pagination envelope."""
    total_records: int = Field(default=0, alias="totalRecords")
    current_page: int = Field(default=1, alias="currentPage")
    total_pages: int = Field(default=0, alias="totalPages")
    next_page: bool = Field(default=False, alias="nextPage")
    prev_page: bool = Field(default=False, alias="prevPage")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class SearchTrialItem(BaseModel):
    """Individual trial summary returned by Search API."""
    ct_number: str = Field(alias="ctNumber")
    ct_status: Optional[Any] = Field(default=None, alias="ctStatus")
    ct_title: Optional[str] = Field(default=None, alias="ctTitle")
    decision_date_overall: Optional[str] = Field(default=None, alias="decisionDateOverall")
    decision_date: Optional[str] = Field(default=None, alias="decisionDate")
    last_updated: Optional[str] = Field(default=None, alias="lastUpdated")
    last_publication_update: Optional[str] = Field(default=None, alias="lastPublicationUpdate")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class SearchResponse(BaseModel):
    """Full search endpoint response envelope."""
    show_warning: Optional[bool] = Field(default=None, alias="showWarning")
    pagination: SearchPagination
    data: List[SearchTrialItem] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class RawTrialDossier(BaseModel):
    """Raw trial retrieve response from GET /retrieve/{ctNumber}.

    Validates essential invariants while allowing schema flexibility.
    """
    ct_number: str = Field(alias="ctNumber")
    ct_status: Optional[str] = Field(default=None, alias="ctStatus")
    decision_date: Optional[str] = Field(default=None, alias="decisionDate")
    publish_date: Optional[str] = Field(default=None, alias="publishDate")
    ct_public_status_code: Optional[int] = Field(default=None, alias="ctPublicStatusCode")
    trial_region: Optional[str] = Field(default=None, alias="trialRegion")
    trial_region_code: Optional[int] = Field(default=None, alias="trialRegionCode")
    authorized_application: Optional[Dict[str, Any]] = Field(default=None, alias="authorizedApplication")
    documents: Optional[Any] = Field(default=None, alias="documents")
    results: Optional[Any] = Field(default=None, alias="results")
    events: Optional[Any] = Field(default=None, alias="events")

    model_config = ConfigDict(populate_by_name=True, extra="allow")

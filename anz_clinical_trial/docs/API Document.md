# API Document

## Endpoint & Methodology

- **ASMX Web Services Endpoint**: <https://api.anzctr.org.au/WebServices/AnzctrWebservices.asmx>
- **WSDL Definition**: <https://api.anzctr.org.au/WebServices/AnzctrWebservices.asmx?WSDL>
- **Data Methodology**: <https://clinicaltrialsaustralia.io/methodology>

## Initial APIs to Test (Raw Data Collection)

Currently, the pipeline queries the following 5 API operations to collect pure raw XML responses without Silver layer transformations:

- Search
- result
- detail
- detailv2
- detailresult

### 1. Search (`AnzctrTrialSearch`)

Searches the registry using keyword, condition, and status filters.

- **SOAPAction**: `"http://anzctr.org.au/WebServices/AnzctrWebServices/AnzctrTrialSearch"`
- **Key Parameters**: `searchText`, `purposeOfStudy`, `recruitmentStatus`, `phase`, `studyType`, `paging`, `pageNumber`, `getActrnOnly`.
- **Output**: Raw XML containing list of matching trial records or ACTRN identifiers.

### 2. result (`ActSearchResults`)

Executes specialized registry search queries and returns structured result summaries.

- **SOAPAction**: `"http://anzctr.org.au/WebServices/AnzctrWebServices/ActSearchResults"`
- **Key Parameters**: `keyword`, `cancerType`, `fromDate`, `toDate`, `sponsor`, `recruitmentStatus`, `paging`, `pageNumber`.
- **Output**: Raw XML containing search result items.

### 3. detail (`AnzctrTrialDetails`)

Retrieves detailed trial record(s) by trial identifier.

- **SOAPAction**: `"http://anzctr.org.au/WebServices/AnzctrWebServices/AnzctrTrialDetails"`
- **Key Parameters**: `ids` (comma-separated ACTRN identifiers, e.g. `ACTRN12613000001741`).
- **Output**: Raw XML trial dossier.

### 4. detailv2 (`AnzctrTrialDetailsV2`)

Retrieves version 2 detailed trial schema record(s) by trial identifier.

- **SOAPAction**: `"http://anzctr.org.au/WebServices/AnzctrWebServices/AnzctrTrialDetailsV2"`
- **Key Parameters**: `ids` (ACTRN identifier), `version` (schema version, typically `1` or `2`).
- **Output**: Raw XML comprehensive trial dossier.

### 5. detailresult (`ActMoreInfo`)

Retrieves supplementary details, outcome measures, and result records for a given trial.

- **SOAPAction**: `"http://anzctr.org.au/WebServices/AnzctrWebServices/ActMoreInfo"`
- **Key Parameters**: `id` (trial identifier).
- **Output**: Raw XML additional trial and result metadata.

## Raw Data Storage Layout

All untouched XML payloads are stored under `./data/raw/`:

```text
anz_clinical_trial/data/raw/
├── search/
│   └── ACTRN12613000001741.xml
├── result/
│   └── ACTRN12613000001741.xml
├── detail/
│   └── ACTRN12613000001741.xml
├── detailv2/
│   └── ACTRN12613000001741.xml
└── detailresult/
    └── ACTRN12613000001741.xml
```

## How to Execute Raw Data Collection

### Using the CLI
```bash
# Query all 5 endpoints for a given ACTRN
python -m anz_etl.main --mode raw --actrn ACTRN12613000001741

# With custom search keyword
python -m anz_etl.main --mode raw --actrn ACTRN12613000001741 --keyword "oncology"
```

### Using the Standalone Test Script
```bash
python test_raw_apis.py ACTRN12613000001741 "oncology"
```

### Inside Docker
```bash
docker-compose exec anz_clinical_trial python -m anz_etl.main --mode raw --actrn ACTRN12613000001741
```

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 09:54:02 2025

List generator 5000

@author: lukespagnuolo
"""

# app.py – Export ALL Profiles (universal JSON flattening, with campus-by-city mapping)
# ------------------------------------------------------------------------
CLIENT_ID = "VbcO9bGovprC5YOOFeMJfkLcvNDuANDbATdqDicn"
CLIENT_SECRET = "WEQzHzSfM10pQOMbdCyVWBRpvwyGyOBnvEUyEm4vkZCMnj6CoG5KgMNk144OtKBB0ZGzwSzx0cQcJzcis6uP9XYD9OiC2VD8yneZM7pmklCQoV8dJbwSKW6Htv7xmnun"

SITE = "https://apps.csipacific.ca"

import os

def running_in_spyder() -> bool:
    """Detect Spyder/IPython console execution to avoid dev-server reloader issues."""
    if "SPYDER_ARGS" in os.environ:
        return True
    return any(k.startswith("SPYDER") for k in os.environ)

if os.getenv("APP_URL"):
    APP_URL = os.getenv("APP_URL")
elif os.getenv("CODESPACE_NAME") and os.getenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN"):
    APP_URL = (
        f"https://{os.getenv('CODESPACE_NAME')}-8050."
        f"{os.getenv('GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN')}"
    )
else:
    APP_URL = "http://127.0.0.1:8050"

AUTH_URL = f"{SITE}/o/authorize"
TOKEN_URL = f"{SITE}/o/token/"
PROFILES_URL = f"{SITE}/api/registration/report-rows/"
REPORT_COLUMNS_URL = f"{SITE}/api/registration/report-columns/"

# -------------------------------------------------------------------------
# Networking & retry tuning (UPDATED)
# -------------------------------------------------------------------------
PAGE_LIMIT = 50              # smaller pages reduce risk of slow responses timing out
MAX_RETRIES = 5              # include retries for timeouts/conn errors too
BACKOFF_SEC = 1.5            # base backoff; we also add jitter
REQUEST_TIMEOUT = (10, 90)   # (connect timeout, read timeout) in seconds
RETRYABLE_STATUSES = (502, 503, 504, 524)  # include common gateway/timeouts

# -------------------------------------------------------------------------
# CAMPUS OPTIONS (corrected)
# -------------------------------------------------------------------------
CAMPUS_OPTS = [
    {"label": "CSI Pacific - Victoria",           "value": 1},
    {"label": "CSI Pacific - Vancouver",          "value": 2},
    {"label": "CSI Pacific - Whistler",           "value": 3},
    {"label": "Engage Sport North",               "value": 4},
    {"label": "Pacific Sport - Columbia Basin",   "value": 5},
    {"label": "Pacific Sport - Fraser Valley",    "value": 6},
    {"label": "Pacific Sport - Interior",         "value": 7},
    {"label": "Pacific Sport - Okanagan",         "value": 8},
    {"label": "Pacific Sport - Vancouver Island", "value": 9},
    {"label": "Other",                            "value": 10},
    {"label": "Unsure",                           "value": 11},
    {"label": "Not Applicable",                   "value": 12},
    {"label": "All Campuses",                     "value": "all"},
]
CAMPUS_LABEL_MAP = {
    opt["value"]: opt["label"]
    for opt in CAMPUS_OPTS
    if isinstance(opt["value"], int)
}

# -------------------------------------------------------------------------
# ROLE OPTIONS
# -------------------------------------------------------------------------
ROLE_OPTS = [
    {"label": "(all roles)", "value": ""},
    {"label": "Athlete", "value": "athlete"},
    {"label": "Coach", "value": "coach"},
    {"label": "Staff", "value": "staff"},
]



# -------------------------------------------------------------------------
# IMPORTS & APP INITIALISATION
# -------------------------------------------------------------------------
import io, json, time, zipfile, requests, pandas as pd, random, difflib
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from dash_auth_external import DashAuthExternal
from dash import Dash, html, dcc, dash_table, Input, Output, State, no_update

auth = DashAuthExternal(
    AUTH_URL, TOKEN_URL,
    app_url=APP_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)
server = auth.server
app = Dash(__name__, server=server)

# -------------------------------------------------------------------------
# SAFE STRING + UNIVERSAL FLATTENING
# -------------------------------------------------------------------------

def safe_str(v):
    """Convert any complex or None values to a safe string for DataTable/CSV."""
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def flatten_json(data, prefix=""):
    """Recursively flatten dicts/lists with dot notation, handling all types."""
    out = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
            out.update(flatten_json(v, new_key))
    elif isinstance(data, list):
        out[prefix] = "; ".join([safe_str(i) for i in data])
    else:
        out[prefix] = safe_str(data)
    return out

def _normalize_report_value(raw, col_def):
    """Normalize report values using column metadata (labels/options/multiselect)."""
    if raw is None:
        return ""

    options = col_def.get("options") or []
    opt_map = {str(o.get("value")): o.get("label", safe_str(o.get("value"))) for o in options}
    is_multiselect = bool(col_def.get("is_multiselect"))

    if options:
        if is_multiselect:
            vals = raw if isinstance(raw, list) else [raw]
            labels = [opt_map.get(str(v), safe_str(v)) for v in vals if v not in (None, "")]
            return "; ".join(labels)
        return opt_map.get(str(raw), safe_str(raw))

    if isinstance(raw, list):
        return "; ".join([safe_str(i) for i in raw])

    return safe_str(raw)

def flatten_report_row(row: dict, campus_id, report_columns: list) -> dict:
    """
    Flatten new report-format rows using the provided column metadata.
    Supports values stored in row["cells"] and row["meta"].
    """
    flat = {}
    cells = row.get("cells") if isinstance(row.get("cells"), dict) else {}
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}

    for col in report_columns:
        key = col.get("key")
        if not key:
            continue

        target = col.get("target")
        if target == "cells":
            raw = cells.get(key)
        elif target == "meta":
            raw = meta.get(key)
        else:
            raw = row.get(key)

        # Fallback lookups for inconsistent payloads
        if raw is None and key in row:
            raw = row.get(key)
        if raw is None and key in cells:
            raw = cells.get(key)
        if raw is None and key in meta:
            raw = meta.get(key)

        flat[key] = _normalize_report_value(raw, col)

    # Include unmapped scalar keys to retain visibility of extra data
    for k, v in row.items():
        if k in ("cells", "meta") or k in flat:
            continue
        if isinstance(v, (dict, list)):
            continue
        flat[k] = safe_str(v)

    return flat

def flatten_profile(p, campus_id):
    """
    Fully flatten profile including all nested content + campus/carding mapping,
    and add campus_by_birth / current_campus using the city → campus mapping.
    """
    flat = flatten_json(p)

    return flat

# -------------------------------------------------------------------------
# FETCH PAGINATED RESULTS (UPDATED with robust retries)
# -------------------------------------------------------------------------
def fetch_paginated(url, headers, log):
    """
    Fetch paginated DRF endpoint with robust retries for:
      - HTTP 502/503/504/524
      - ReadTimeout / ConnectTimeout / ConnectionError
    Uses exponential backoff with jitter, smaller page size, and longer read timeout.
    """
    rows, page = [], 0
    report_meta = {"columns": [], "defaults": [], "system": []}
    session = requests.Session()

    while url:
        # ensure limit is set
        if "limit=" not in url:
            url += ("&" if "?" in url else "?") + f"limit={PAGE_LIMIT}"

        page += 1
        retries, wait = 0, BACKOFF_SEC

        while True:
            try:
                resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                status = resp.status_code

                # Retry on retryable HTTP statuses
                if status in RETRYABLE_STATUSES and retries < MAX_RETRIES:
                    log.append(f"[{page}] {url} • {status} → retry {retries+1}/{MAX_RETRIES} in {wait:.1f}s")
                    time.sleep(wait + random.uniform(0, 0.5))  # jitter
                    retries += 1
                    wait *= 2
                    continue

                # Non-retryable HTTP error
                if status != 200:
                    log.append(f"[{page}] {url} • {status}\n{resp.text[:300]}")
                    return rows, report_meta  # bail but keep what we have

                # Success
                log.append(f"[{page}] {url} • 200")
                data = resp.json()

                # New report format: {report: "report_rows", columns: [...], ...}
                if isinstance(data, dict) and ("report" in data or "columns" in data):
                    report_key = data.get("report") or "report_rows"
                    page_rows = data.get(report_key)
                    if not isinstance(page_rows, list):
                        page_rows = data.get("report_rows", [])
                    if isinstance(page_rows, list):
                        rows.extend(page_rows)

                    if isinstance(data.get("columns"), list):
                        report_meta["columns"] = data.get("columns", [])
                    if isinstance(data.get("defaults"), list):
                        report_meta["defaults"] = data.get("defaults", [])
                    if isinstance(data.get("system"), list):
                        report_meta["system"] = data.get("system", [])

                    # Some endpoints may still provide pagination links
                    url = data.get("next")
                    if not url:
                        break
                    continue

                # Legacy DRF format
                rows.extend(data.get("results", []))
                url = data.get("next")
                break  # proceed to next page if any

            except (ReadTimeout, ConnectTimeout, ConnectionError) as e:
                if retries < MAX_RETRIES:
                    log.append(f"[{page}] timeout/conn error: {e.__class__.__name__} → retry {retries+1}/{MAX_RETRIES} in {wait:.1f}s")
                    time.sleep(wait + random.uniform(0, 0.5))
                    retries += 1
                    wait *= 2
                    continue
                else:
                    log.append(f"[{page}] giving up after {MAX_RETRIES} retries: {type(e).__name__}: {str(e)[:200]}")
                    return rows, report_meta  # return what we managed to fetch so far

            except Exception as e:
                # Unexpected error; log and stop
                log.append(f"[{page}] unexpected error: {type(e).__name__}: {str(e)[:200]}")
                return rows, report_meta

    return rows, report_meta

def build_export_columns(df: pd.DataFrame, report_meta: dict) -> list:
    """Build export fields/labels from API metadata; fallback to legacy defaults."""
    if not df.empty and report_meta.get("columns"):
        label_map = {
            c.get("key"): c.get("label", c.get("key"))
            for c in report_meta.get("columns", [])
            if c.get("key")
        }
        defaults = [k for k in report_meta.get("defaults", []) if k in df.columns]
        system = [k for k in report_meta.get("system", []) if k in df.columns and k not in defaults]

        ordered = defaults + system
        ordered += [k for k in df.columns if k not in ordered]

        return [(k, label_map.get(k, k)) for k in ordered]

    cols = [(field, label) for field, label in EXPORT_COLUMNS if field in df.columns]
    extras = [(c, c) for c in df.columns if c not in {f for f, _ in cols}]
    return cols + extras


def fetch_column_defs(headers, log):
    """Fetch available report column definitions from the API."""
    try:
        resp = requests.get(REPORT_COLUMNS_URL, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("results", data.get("columns", []))
        log.append(f"report-columns: HTTP {resp.status_code}")
    except Exception as e:
        log.append(f"report-columns error: {e}")
    return []


# -------------------------------------------------------------------------
# GLOBAL CACHE
# -------------------------------------------------------------------------
cached_df, cached_name = pd.DataFrame(), ""

# -------------------------------------------------------------------------
# EXPORT COLUMN SPECS (field name → pretty label)
# -------------------------------------------------------------------------
EXPORT_COLUMNS = [
    ("role",                    "Role"),
    ("first_name",              "First Name"),
    ("last_name",               "Last Name"),
    ("email",                   "Email"),
    ("dob",                     "Birth Date"),
    ("age",                     "Age"),
    ("guardian_first_name",     "Guardian First Name"),
    ("guardian_last_name",      "Guardian Last Name"),
    ("guardian_relationship",   "Guardian Relationship"),
    ("guardian_email",          "Guardian Email"),
    ("sport",                   "Sport"),
    ("enrollment_status",       "Enrollment Status"),
    ("nomination_organization", "Nomination Organization"),
    ("nomination_fiscal_year",  "Nomination Fiscal Year"),
    ("nomination_end_date",     "Nomination End Date"),
    ("nomination_sport_name",   "Nomination Sport Name"),
    ("nomination_claimed",      "Nomination Claimed"),
    ("nomination_approved",     "Nomination Approved"),
    ("nomination_status",       "Nomination Status"),
    ("athlete_carding",         "Carding Level"),
    ("level_category",          "Level Category"),
    ("current_residence",       "Current Residence"),
    ("birth_city",              "Birth City"),
    ("discipline",              "Discipline"),
    ("sex_of_competition",      "Sex of Competition"),
    ("gender",                  "Gender"),
    ("athlete_ethnicity",       "Ethnicity"),
    ("pronouns",                "Pronouns"),
    ("pronouns_other",          "Pronouns Other"),
    ("disability",              "Disability"),
    ("birth_country",           "Birth Country"),
    ("residence_country",       "Residence Country"),
    ("attending_education",     "Attending Education"),
    ("education_level",         "Education Level"),
    ("education_institution",   "Education Institution"),
    ("css",                     "CSS"),
    ("campus_label",            "Campus Preferred"),
    ("birth_city_campus",       "Campus by Birth"),
    ("residence_city_campus",   "Current Campus"),
    ("nccp_number",             "Nccp Number"),
    ("coach_role",              "Coach Role"),
    ("coach_level",             "Coach Level"),
]

# list of internal field names for filtering
FILTER_COLUMNS = [field for field, _ in EXPORT_COLUMNS]

FIELD_TO_LABEL = {field: label for field, label in EXPORT_COLUMNS}

# Dynamic export mapping refreshed on each successful fetch
ACTIVE_EXPORT_COLUMNS = EXPORT_COLUMNS.copy()
ACTIVE_FILTER_COLUMNS = FILTER_COLUMNS.copy()
ACTIVE_FIELD_TO_LABEL = FIELD_TO_LABEL.copy()

# -------------------------------------------------------------------------
# NATIONAL INVENTORY REPORT COLUMN SPECS
# -------------------------------------------------------------------------
NATIONAL_INVENTORY_COLUMNS = [
    ("season", "Season"),
    ("training_group", "Training Group"),
    ("role", "Role"),
    ("first_name", "First Name"),
    ("last_name", "Last Name"),
    ("sport", "Sport"),
    ("enrollment_status", "Enrollment Status"),
    ("nomination_approved", "Nomination Approved"),
    ("nomination_claimed", "Nomination Claimed"),
    ("athlete_carding", "Carding Level"),
    ("level_category", "Level Category"),
    ("sex_of_competition", "Sex of Competition"),
    ("nccp_number", "Nccp Number"),
]

# -------------------------------------------------------------------------
# ACTIVE ATHLETES – COLUMNS TO EXCLUDE
# -------------------------------------------------------------------------
ACTIVE_ATHLETES_EXCLUDE_KEYS = {
    "profile_id", "nomination_id", "row_kind", "enrollment_id", "row_id",
}

# -------------------------------------------------------------------------
# TEST SPORTS TO EXCLUDE FROM FILTERED DOWNLOAD
# -------------------------------------------------------------------------
TEST_SPORTS = {
    "Cinderball (TEST)",
    "Skimboarding Cross (TEST)",
}
TEST_SPORTS_NORMALIZED = {s.strip().lower() for s in TEST_SPORTS}

SPORT_COLUMNS = [
    "sport",
    "nomination_sport_name",
    "Sport",
    "Nomination Sport Name",
]

# User-configurable LOU defaults (matched against normalized sport names).
DEFAULT_LOU_SPORT_KEYS = {
    "alpine ski",
    "alpine skiing",
    "para alpine ski",
    "para alpine skiing",
    "athletics",
    "cycling",
    "diving",
    "freestyle ski",
    "freestyle skiing",
    "artistic gymnastics",
    "rowing",
    "rugby",
    "sailing",
    "skateboard",
    "skateboarding",
    "snowboard",
    "soccer",
    "swimming",
    "triathlon",
    "para triathlon",
    "volleyball",
    "wheelchair rugby",
}

DEFAULT_ENHANCED_EXCELLENCE_SPORT_KEYS = {
    "alpine ski",
    "alpine skiing",
    "artistic swimming",
    "athletics",
    "diving",
    "basketball",
    "biathlon",
    "artistic gymnastics",
    "canoe/kayak",
    "canoe kayak",
    "cross country skiing",
    "cross-country skiing",
    "cross-country ski",
    "curling",
    "cycling",
    "judo",
    "rowing",
    "field hockey",
    "figure skating",
    "freestyle skiing",
    "freestyle ski",
    "wheelchair athletics",
    "wheelchair rugby",
    "wheelchair tennis",
    "wrestling",
    "rugby",
    "sailing",
    "snowboard",
    "swimming",
    "triathlon",
    "volleyball",
    "wheelchair basketball",
}

def normalize_sport_name(value) -> str:
    """Normalize sport text for stable comparisons."""
    return " ".join(str(value or "").strip().lower().split())


def get_available_sport_columns(df: pd.DataFrame) -> list:
    """Return sport-related columns that exist in the DataFrame."""
    return [c for c in SPORT_COLUMNS if c in df.columns]


def build_unique_sport_options(df: pd.DataFrame) -> list:
    """Build sorted dropdown options from all unique sport values in the dataset."""
    if df.empty:
        return []

    cols = get_available_sport_columns(df)
    if not cols:
        return []

    labels_by_norm = {}
    for col in cols:
        series = df[col].dropna().astype(str).str.strip()
        for val in series:
            if not val or val.lower() == "nan":
                continue
            norm = normalize_sport_name(val)
            if not norm:
                continue
            labels_by_norm.setdefault(norm, val)

    return [
        {"label": labels_by_norm[k], "value": labels_by_norm[k]}
        for k in sorted(labels_by_norm, key=lambda x: labels_by_norm[x].lower())
    ]


def build_default_lou_sports(unique_sport_options: list) -> list:
    """Pick default LOU sports from available options using normalized matching."""
    defaults = []
    for opt in unique_sport_options:
        value = opt.get("value", "")
        if normalize_sport_name(value) in DEFAULT_LOU_SPORT_KEYS:
            defaults.append(value)
    return defaults


def build_default_enhanced_excellence_sports(unique_sport_options: list) -> list:
    """Pick Enhanced Excellence defaults and include para counterparts when present."""
    defaults = []
    for opt in unique_sport_options:
        value = opt.get("value", "")
        norm = normalize_sport_name(value)

        # Exact match against baseline Enhanced Excellence list.
        if norm in DEFAULT_ENHANCED_EXCELLENCE_SPORT_KEYS:
            defaults.append(value)
            continue

        # Include para variants automatically when the non-para sport is listed.
        if norm.startswith("para "):
            base = norm[5:].strip()
            if base in DEFAULT_ENHANCED_EXCELLENCE_SPORT_KEYS:
                defaults.append(value)

    return defaults


VIASPORT_FIXED_FISCAL_YEAR = "2025-2026"
VIASPORT_TRUE_VALUES = {"true", "1", "yes", "y"}
VIASPORT_PERSON_ID_CANDIDATES = ["person_id", "profile_id", "id", "person.id", "profile.id"]
VIASPORT_FIRST_NAME_CANDIDATES = ["first_name", "person.first_name", "First Name"]
VIASPORT_LAST_NAME_CANDIDATES = ["last_name", "person.last_name", "Last Name"]
VIASPORT_SPORT_CANDIDATES = ["sport", "nomination_sport_name", "Sport", "Nomination Sport Name"]
VIASPORT_GENDER_CANDIDATES = ["gender", "Gender"]
VIASPORT_CARD_LEVEL_CANDIDATES = ["athlete_carding", "card_level", "Card level", "Card Level"]
VIASPORT_AGE_CANDIDATES = ["age", "Age"]
VIASPORT_DISCIPLINE_CANDIDATES = ["discipline", "Discipline"]
VIASPORT_ETHNICITY_CANDIDATES = ["athlete_ethnicity", "ethnicity", "Ethnicity"]
VIASPORT_CURRENT_CAMPUS_CANDIDATES = ["current_city_campus", "residence_city_campus", "current_campus", "campus_label"]
VIASPORT_NOMINATION_CLAIMED_CANDIDATES = ["nomination_claimed", "current_nomination.redeemed", "Nomination Claimed"]
VIASPORT_APPROVED_CANDIDATES = ["nomination_approved", "current_nomination.approved"]


def first_existing_column(df: pd.DataFrame, candidates: list) -> str:
    """Return the first column name from candidates that exists in the DataFrame."""
    return next((c for c in candidates if c in df.columns), "")


def normalized_text_series(series: pd.Series) -> pd.Series:
    """Normalize text values for stable equality filtering."""
    return series.fillna("").astype(str).map(normalize_sport_name)


def truthy_mask(series: pd.Series) -> pd.Series:
    """Return a boolean mask for common true-like string values."""
    return series.fillna("").astype(str).str.strip().str.lower().isin(VIASPORT_TRUE_VALUES)


def filter_viasport_base(df: pd.DataFrame, role_value: str, level_category_value: str = "") -> pd.DataFrame:
    """Apply the fixed ViaSport report filters that all prebuilt exports share."""
    if df.empty:
        return df

    out = apply_fiscal_year_filter(df.copy(), VIASPORT_FIXED_FISCAL_YEAR)
    out = remove_test_sports(out)
    if out.empty:
        return out

    role_col = first_existing_column(out, ["role", "Role"])
    if not role_col:
        return out.iloc[0:0]
    out = out[normalized_text_series(out[role_col]) == normalize_sport_name(role_value)]

    approved_col = first_existing_column(out, VIASPORT_APPROVED_CANDIDATES)
    if not approved_col:
        return out.iloc[0:0]
    out = out[truthy_mask(out[approved_col])]

    if level_category_value:
        out = add_level_category(out)
        level_col = first_existing_column(out, ["level_category", "Level Category"])
        if not level_col:
            return out.iloc[0:0]
        out = out[normalized_text_series(out[level_col]) == normalize_sport_name(level_category_value)]

    return out


def build_column_export(df: pd.DataFrame, specs: list) -> pd.DataFrame:
    """Build a DataFrame using ordered output labels and source column fallbacks."""
    out = pd.DataFrame(index=df.index)
    for output_label, candidates in specs:
        actual = first_existing_column(df, candidates)
        if actual:
            out[output_label] = df[actual]
        else:
            out[output_label] = ""
    return out


def build_viasport_sc_carded_export(df: pd.DataFrame) -> pd.DataFrame:
    """Build the SC Carded list export."""
    out = build_column_export(
        df,
        [
            ("Sport", VIASPORT_SPORT_CANDIDATES),
            ("Gender", VIASPORT_GENDER_CANDIDATES),
            ("Card level", VIASPORT_CARD_LEVEL_CANDIDATES),
            ("Nomination Claimed", VIASPORT_NOMINATION_CLAIMED_CANDIDATES),
        ],
    )

    first_name_col = first_existing_column(df, VIASPORT_FIRST_NAME_CANDIDATES)
    last_name_col = first_existing_column(df, VIASPORT_LAST_NAME_CANDIDATES)
    first_name = df[first_name_col].fillna("").astype(str) if first_name_col else pd.Series("", index=df.index)
    last_name = df[last_name_col].fillna("").astype(str) if last_name_col else pd.Series("", index=df.index)
    out.insert(1, "Name", (first_name.str.strip() + " " + last_name.str.strip()).str.strip())

    province = pd.Series("", index=df.index, dtype="object")
    current_campus_col = first_existing_column(df, VIASPORT_CURRENT_CAMPUS_CANDIDATES)
    if current_campus_col:
        has_current_campus = df[current_campus_col].fillna("").astype(str).str.strip() != ""
        province.loc[has_current_campus] = "British Columbia"
    out.insert(4, "Province", province)

    return out[["Sport", "Name", "Gender", "Card level", "Province", "Nomination Claimed"]]


def build_viasport_high_performance_export(df: pd.DataFrame) -> pd.DataFrame:
    """Build the High Performance Athletes list export."""
    return build_column_export(
        df,
        [
            ("Person ID", VIASPORT_PERSON_ID_CANDIDATES),
            ("Sport", VIASPORT_SPORT_CANDIDATES),
            ("Card level", VIASPORT_CARD_LEVEL_CANDIDATES),
            ("Gender", VIASPORT_GENDER_CANDIDATES),
            ("Age", VIASPORT_AGE_CANDIDATES),
            ("Discipline", VIASPORT_DISCIPLINE_CANDIDATES),
            ("Current City Campus", VIASPORT_CURRENT_CAMPUS_CANDIDATES),
            ("Ethnicity", VIASPORT_ETHNICITY_CANDIDATES),
            ("Nomination Claimed", VIASPORT_NOMINATION_CLAIMED_CANDIDATES),
        ],
    )


def build_viasport_coaches_export(df: pd.DataFrame) -> pd.DataFrame:
    """Build the Coaches list export."""
    return build_column_export(
        df,
        [
            ("Person ID", VIASPORT_PERSON_ID_CANDIDATES),
            ("Sport", VIASPORT_SPORT_CANDIDATES),
            ("Gender", VIASPORT_GENDER_CANDIDATES),
            ("Current City Campus", VIASPORT_CURRENT_CAMPUS_CANDIDATES),
            ("Nomination Claimed", VIASPORT_NOMINATION_CLAIMED_CANDIDATES),
        ],
    )


def build_viasport_report_bundle(df: pd.DataFrame) -> dict:
    """Build all ViaSport report exports from the same filtered base DataFrame."""
    return {
        "sc_carded_list": build_viasport_sc_carded_export(df),
        "high_performance_athletes": build_viasport_high_performance_export(df),
        "coaches": build_viasport_coaches_export(df),
    }


def filter_by_selected_sports(df: pd.DataFrame, selected_sports: list) -> pd.DataFrame:
    """Keep rows where any available sport column matches selected sport values."""
    if df.empty or not selected_sports:
        return df

    cols = get_available_sport_columns(df)
    if not cols:
        return df

    selected_norms = {
        normalize_sport_name(s)
        for s in selected_sports
        if normalize_sport_name(s)
    }
    if not selected_norms:
        return df

    keep = pd.Series(False, index=df.index)
    for col in cols:
        norm_col = df[col].fillna("").astype(str).map(normalize_sport_name)
        keep = keep | norm_col.isin(selected_norms)

    return df[keep]


def build_prebuilt_export_df(df: pd.DataFrame) -> pd.DataFrame:
    """Order and relabel columns for prebuilt list exports."""
    if df.empty:
        return df

    ordered_fields = [field for field, _ in ACTIVE_EXPORT_COLUMNS if field in df.columns]
    extra_fields = [c for c in df.columns if c not in ordered_fields]
    fields = ordered_fields + extra_fields

    df_export = df[fields].copy()
    rename_map = {f: ACTIVE_FIELD_TO_LABEL.get(f, f) for f in fields}
    df_export.rename(columns=rename_map, inplace=True)
    return df_export


def remove_test_sports(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where any of the sport columns indicate TEST sports.
    Checks both internal and pretty column names, case-insensitive.
    """
    if df.empty:
        return df

    cols = get_available_sport_columns(df)
    if not cols:
        return df

    mask = pd.Series(True, index=df.index)

    for col in cols:
        s = df[col].fillna("").astype(str).str.strip().str.lower()
        explicit = s.isin(TEST_SPORTS_NORMALIZED)
        contains_test = s.str.contains("(test", case=False, regex=False)
        mask &= ~(explicit | contains_test)

    return df[mask]

# -------------------------------------------------------------------------
# ADD LEVEL CATEGORY COLUMN
# -------------------------------------------------------------------------
def add_level_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'level_category' column derived from the athlete_carding column.

    Rules:
      - Prov Dev 1/2/3 → 'Provincial Development'
      - NSO Affiliated (Uncarded) → 'Canadian Development'
      - SR, SR1, D, C1, etc. → 'Canadian Elite'
      - PSO Affiliated (Uncarded) → 'PSO Affiliated (Non Carded)'
    """
    carding_col = "athlete_carding"

    if carding_col not in df.columns:
        df["level_category"] = ""
        return df

    s = df[carding_col].fillna("").astype(str).str.strip()

    cat = pd.Series("", index=df.index, dtype="object")

    prov_dev_vals = {"Prov Dev 1", "Prov Dev 2", "Prov Dev 3"}
    cat[s.isin(prov_dev_vals)] = "Provincial Development"

    cat[s.str.contains("NSO Affiliated", case=False, na=False)] = "Canadian Development"

    cat[s.str.contains("PSO Affiliated", case=False, na=False)] = "PSO Affiliated (Non Carded)"

    elite_vals = {"SR", "SRI", "SR1", "SR2", "D", "DI", "C", "C1", "C1I", "GamePlan - Retired"}
    cat[s.isin(elite_vals)] = "Canadian Elite"

    df["level_category"] = cat
    return df


def infer_season_from_sport(sport_val: str) -> str:
    """Infer season from a normalized, explicit sport map."""
    s = " ".join(str(sport_val or "").strip().lower().split())
    if not s:
        return ""

    season_by_sport = {
        "alpine ski": "Winter",
        "archery": "Summer",
        "artistic gymnastics": "Summer",
        "artistic swimming": "Summer",
        "athletics": "Summer",
        "badminton": "Summer",
        "baseball": "Summer",
        "basketball": "Summer",
        "beach volleyball": "Summer",
        "biathlon": "Winter",
        "bobsleigh": "Winter",
        "boccia": "Summer",
        "boxing": "Summer",
        "canoe kayak": "Summer",
        "cinderball (test)": "Summer",
        "cricket": "Summer",
        "cross-country ski": "Winter",
        "curling": "Winter",
        "cycling": "Summer",
        "diving": "Summer",
        "equestrian": "Summer",
        "fencing": "Summer",
        "field hockey": "Summer",
        "figure skating": "Winter",
        "football": "Summer",
        "football 5-a-side": "Summer",
        "freestyle ski": "Winter",
        "goalball": "Summer",
        "golf": "Summer",
        "hockey": "Winter",
        "judo": "Summer",
        "karate": "Summer",
        "lacrosse": "Summer",
        "luge": "Winter",
        "multisport": "Summer",
        "netball": "Summer",
        "nordic combined": "Winter",
        "nordic vaulting (test)": "Winter",
        "para alpine ski": "Winter",
        "para athletics": "Summer",
        "para badminton": "Summer",
        "para cross-country ski": "Winter",
        "para cycling": "Summer",
        "para equestrian": "Summer",
        "para ice hockey": "Winter",
        "para rowing": "Summer",
        "para sailing": "Summer",
        "para snowboard": "Winter",
        "para swimming": "Summer",
        "para table tennis": "Summer",
        "para triathlon": "Summer",
        "racquetball": "Summer",
        "rhythmic gymnastics": "Summer",
        "ringette": "Winter",
        "rowing": "Summer",
        "rugby": "Summer",
        "sailing": "Summer",
        "shooting para sport": "Summer",
        "sitting volleyball": "Summer",
        "skateboard": "Summer",
        "skeleton": "Winter",
        "ski cross": "Winter",
        "ski jumping": "Winter",
        "skimboarding cross (test)": "Summer",
        "snowboard": "Winter",
        "soccer": "Summer",
        "softball": "Summer",
        "speed skating": "Winter",
        "sport cheer": "Summer",
        "sport climbing": "Summer",
        "squash": "Summer",
        "surfing": "Summer",
        "swimming": "Summer",
        "table tennis": "Summer",
        "taekwondo": "Summer",
        "tennis": "Summer",
        "trampoline": "Summer",
        "triathlon": "Summer",
        "ultimate": "Summer",
        "volleyball": "Summer",
        "wakeboard": "Summer",
        "water polo": "Summer",
        "water ski": "Summer",
        "weightlifting": "Summer",
        "wheelchair athletics": "Summer",
        "wheelchair basketball": "Summer",
        "wheelchair curling": "Winter",
        "wheelchair rugby": "Summer",
        "wheelchair tennis": "Summer",
        "wrestling": "Summer",
    }

    return season_by_sport.get(s, "")

# -------------------------------------------------------------------------
# NAME-MATCH PAIR FINDER
# -------------------------------------------------------------------------
def find_name_matched_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find pairs of rows where one has row_kind='profile' and another has
    row_kind='nomination', with matching or similar first/last names.
    Returns a DataFrame of those matched rows, sorted so pairs appear adjacent.
    """
    SIMILARITY_THRESHOLD = 0.82

    row_kind_col = next(
        (c for c in ["row_kind", "kind", "row_type", "type"] if c in df.columns),
        None,
    )
    if row_kind_col is None:
        return pd.DataFrame()

    first_name_col = next(
        (c for c in ["first_name", "person.first_name", "First Name"] if c in df.columns),
        None,
    )
    last_name_col = next(
        (c for c in ["last_name", "person.last_name", "Last Name"] if c in df.columns),
        None,
    )
    if not first_name_col or not last_name_col:
        return pd.DataFrame()

    def norm(row):
        first = str(row.get(first_name_col, "") or "").strip().lower()
        last = str(row.get(last_name_col, "") or "").strip().lower()
        return f"{first} {last}".strip()

    kinds = df[row_kind_col].fillna("").str.strip().str.lower()
    profiles = df[kinds == "profile"].copy()
    nominations = df[kinds == "nomination"].copy()

    if profiles.empty or nominations.empty:
        return pd.DataFrame()

    profiles["_norm_name"] = profiles.apply(norm, axis=1)
    nominations["_norm_name"] = nominations.apply(norm, axis=1)

    # Resolve sport columns (profile sport vs nomination sport)
    profile_sport_col = next(
        (c for c in ["sport", "sport.name", "Sport"] if c in df.columns), None
    )
    nomination_sport_col = next(
        (c for c in ["nomination_sport_name", "current_nomination.sport.name", "Nomination Sport Name"]
         if c in df.columns),
        profile_sport_col,  # fallback to same column if no separate nomination sport column
    )

    def norm_sport(val):
        return str(val or "").strip().lower()

    def sports_compatible(p_row, n_row):
        """Return True if sports match or either side is multisport/blank."""
        p_sport = norm_sport(p_row.get(profile_sport_col) if profile_sport_col else "")
        n_sport = norm_sport(n_row.get(nomination_sport_col) if nomination_sport_col else "")
        if not p_sport or not n_sport:
            return True  # can't determine — allow
        if "multisport" in p_sport or "multisport" in n_sport:
            return True
        return p_sport == n_sport

    email_col = next((c for c in ["email"] if c in df.columns), None)
    guardian_email_col = next((c for c in ["guardian_email"] if c in df.columns), None)

    def already_linked(p_row, n_row):
        """Return True if both email and guardian_email match — pair is already connected."""
        if not email_col or not guardian_email_col:
            return False
        p_email = str(p_row.get(email_col) or "").strip().lower()
        n_email = str(n_row.get(email_col) or "").strip().lower()
        p_guard = str(p_row.get(guardian_email_col) or "").strip().lower()
        n_guard = str(n_row.get(guardian_email_col) or "").strip().lower()
        if not p_email or not n_email or not p_guard or not n_guard:
            return False
        return p_email == n_email and p_guard == n_guard

    enrollment_col = next(
        (c for c in ["enrollment_status", "Enrollment Status"] if c in df.columns), None
    )

    matched_pairs = []  # (profile_idx, nomination_idx, sort_name)
    for p_idx, p_row in profiles.iterrows():
        # Only include pairs where the profile's enrollment status is Pending
        if enrollment_col:
            enroll_val = str(p_row.get(enrollment_col) or "").strip().lower()
            if enroll_val != "pending":
                continue

        p_name = p_row["_norm_name"]
        for n_idx, n_row in nominations.iterrows():
            n_name = n_row["_norm_name"]

            # Both blank → count as a match (no name data on either side)
            if not p_name and not n_name:
                if sports_compatible(p_row, n_row) and not already_linked(p_row, n_row):
                    matched_pairs.append((p_idx, n_idx, p_name))
                continue

            # Only one side is blank → can't determine a match
            if not p_name or not n_name:
                continue

            ratio = difflib.SequenceMatcher(None, p_name, n_name).ratio()
            if ratio >= SIMILARITY_THRESHOLD and sports_compatible(p_row, n_row) and not already_linked(p_row, n_row):
                matched_pairs.append((p_idx, n_idx, p_name))

    if not matched_pairs:
        return pd.DataFrame()

    # Sort pairs alphabetically by profile name so output is ordered
    matched_pairs.sort(key=lambda x: x[2])

    # Build ordered index: profile row then its matching nomination row
    seen: set = set()
    ordered_indices = []
    for p_idx, n_idx, _ in matched_pairs:
        for idx in (p_idx, n_idx):
            if idx not in seen:
                seen.add(idx)
                ordered_indices.append(idx)

    return df.loc[ordered_indices].copy()


# -------------------------------------------------------------------------
# CAMPUS FILTER HELPER
# -------------------------------------------------------------------------
def apply_campus_filters(df, campus_val, birth_campus_val, current_campus_val):
    """
    Apply three campus-based filters to a DataFrame:
      - campus_val          → filters by API campus (campus_label column)
      - birth_campus_val    → filters by mapped birth campus (campus_by_birth)
      - current_campus_val  → filters by mapped current campus (current_campus)
    Empty/None values mean "no filter" for that dimension.
    """
    out = df

    # Filter by API campus / campus_label (using campus id)
    if isinstance(campus_val, int) and "campus_label" in out.columns:
        label = CAMPUS_LABEL_MAP.get(campus_val)
        if label:
            out = out[out["campus_label"] == label]

    # Filter by birth city campus (direct API column)
    if birth_campus_val and "birth_city_campus" in out.columns:
        out = out[out["birth_city_campus"] == birth_campus_val]

    # Filter by residence city campus (direct API column)
    if current_campus_val and "residence_city_campus" in out.columns:
        out = out[out["residence_city_campus"] == current_campus_val]

    return out


def apply_fiscal_year_filter(df: pd.DataFrame, fiscal_year_val: str) -> pd.DataFrame:
    """Filter DataFrame by fiscal year when a specific fiscal year is selected."""
    if df.empty or not fiscal_year_val:
        return df

    fy_cols = [c for c in ["nomination_fiscal_year", "fiscal_year", "Fiscal Year"] if c in df.columns]
    if not fy_cols:
        return df

    target = str(fiscal_year_val).strip().lower()
    mask = pd.Series(False, index=df.index)
    for col in fy_cols:
        s = df[col].fillna("").astype(str).str.strip().str.lower()
        mask = mask | (s == target)

    return df[mask]


def build_report_guide_tab() -> html.Div:
    """Build the explanatory tab that documents each export button and its rules."""
    guide_md = """
### Common rules

- All download buttons use the data from the last successful **Fetch Rows** action.
- The **Fiscal year** dropdown applies to every download action on the page.
- Several exports remove test sports before writing the file.
- When a report uses a fixed column set, extra columns from the API are ignored.

### Fetch controls

- **Fetch Columns**: loads the API's report-column definitions so you can choose which fields to request.
- **Fetch Rows**: pulls the selected report rows from the API and fills the preview, filters, and download cache.

### Download buttons

#### Download CSV (full)

- Pulls the cached data exactly as fetched, then applies the fiscal year filter only.
- It does not remove test sports.
- It does not apply campus, role, or column selection filters.

#### Download Filtered CSV

- Pulls the cached data and applies fiscal year, birth campus, current campus, role, and selected column filters.
- Removes test sports.
- Recalculates the `level_category` column before export.
- Uses the selected export columns if any are chosen; otherwise it uses the full export column set.

#### Download Pending Unmatched Report

- Applies the fiscal year filter and removes test sports.
- Keeps only rows where the profile side is pending.
- Excludes rows where a nomination is already claimed.
- Pairs profile and nomination rows when names are similar and sports are compatible.
- Skips pairs that already look linked by matching email and guardian email.
- Exports a fixed set of contact and status fields.

#### Download Unclaimed Nominations

- Applies the fiscal year filter and removes test sports.
- Keeps rows where `nomination_approved` is true and `nomination_claimed` is false.
- Drops rows where both approved and claimed are blank.
- Sorts by last name and removes duplicates using first name plus last name.
- Exports a fixed nomination contact list.

#### Download Mail Merge CSV

- Uses the same base filter as **Unclaimed Nominations**.
- Sorts by last name and removes duplicates using first name plus last name.
- Builds a `To` field that uses guardian email for athletes under 19, otherwise athlete email.
- Exports first name, last name, and recipient email.
- Splits into multiple files inside a ZIP if the result exceeds 450 rows.

#### Download National Inventory Data Set

- Applies the fiscal year filter and removes test sports.
- Keeps only rows where `nomination_approved` is true.
- Removes duplicate rows using role, name, sport, and nomination status fields when available.
- Fills missing season values from sport names.
- Exports only the national inventory fields defined in the page.

#### Download Active Athletes

- Applies the fiscal year filter and removes test sports.
- Keeps only rows where `nomination_claimed` is true.
- Removes internal IDs and other excluded keys.
- Exports the configured active-athlete column set plus any remaining non-excluded fields.

### ViaSport reporting prebuilt lists

- These reports always use fiscal year `2025-2026`.
- They require `role` and `nomination_approved` to match the report rules below.
- The SC Carded list fills `Province` with `British Columbia` for rows that have a current city campus value.
- The combined download bundles the individual report CSVs into a single ZIP.

#### SC Carded list (2A)

- Filters to `role = athlete`, `nomination_approved = true`, and `level_category = Canadian Elite`.
- Exports `Sport`, `Name`, `Gender`, `Card level`, `Province`, and `Nomination Claimed`.

#### High Performance Athletes (2B)

- Filters to `role = athlete`, `nomination_approved = true`, and `level_category = Provincial Development`.
- Exports `Person ID`, `Sport`, `Card level`, `Gender`, `Age`, `Discipline`, `Current City Campus`, `Ethnicity`, and `Nomination Claimed`.

#### Coaches (4B)

- Filters to `role = Coach` and `nomination_approved = true`.
- Exports `Person ID`, `Sport`, `Gender`, `Current City Campus`, and `Nomination Claimed`.
"""

    return html.Div(
        style={
            "maxWidth": "980px",
            "marginTop": "1rem",
            "padding": "1rem 1.1rem",
            "backgroundColor": "#ffffff",
            "border": "1px solid #d9e2ec",
            "borderRadius": "10px",
            "boxShadow": "0 1px 3px rgba(0, 0, 0, 0.04)",
        },
        children=[
            html.H2(
                "Report Guide",
                style={"marginTop": "0", "marginBottom": "0.6rem", "color": "#003366"},
            ),
            dcc.Markdown(guide_md, style={"fontSize": "0.95rem", "lineHeight": "1.55"}),
        ],
    )

# -------------------------------------------------------------------------
# LAYOUT
# -------------------------------------------------------------------------
app.layout = html.Div(
    style={"fontFamily": "Arial", "margin": "2rem"},
    children=[
        dcc.Tabs(
            id="page-tabs",
            value="tab-exports",
            children=[
                dcc.Tab(
                    label="Exports",
                    value="tab-exports",
                    style={"padding": "0.8rem 1rem", "fontWeight": "bold"},
                    selected_style={"padding": "0.8rem 1rem", "fontWeight": "bold", "borderTop": "3px solid #0072B2"},
                ),
                dcc.Tab(
                    label="Report Guide",
                    value="tab-guide",
                    style={"padding": "0.8rem 1rem", "fontWeight": "bold"},
                    selected_style={"padding": "0.8rem 1rem", "fontWeight": "bold", "borderTop": "3px solid #0072B2"},
                ),
            ],
            style={"marginBottom": "1rem"},
        ),
        html.Div(
            id="export-tab-content",
            children=[
                # Title & subtitle
                html.H1(
                    "List Generator 5000",
                    style={
                        "marginBottom": "0.1rem",
                        "fontSize": "2rem",
                        "color": "#003366",
                    },
                ),
                html.Div(
                    "Campus filter options",
                    style={
                        "marginBottom": "1rem",
                        "fontSize": "0.95rem",
                        "color": "#555555",
                    },
                ),

                # Fetch Columns button + column selector + Fetch Rows button
                html.Div(
                    [
                        html.Button(
                            "Fetch Columns",
                            id="btn-fetch-columns",
                            style={
                                "padding": "0.45rem 1.2rem",
                                "height": "40px",
                                "whiteSpace": "nowrap",
                            },
                        ),
                        dcc.Dropdown(
                            id="fetch-columns-select",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Click ‘Fetch Columns’ first, then choose which columns to include…",
                            style={"flex": "1", "minWidth": "200px", "marginLeft": "0.6rem", "marginRight": "0.6rem"},
                        ),
                        html.Button(
                            "Fetch Rows",
                            id="btn-fetch-rows",
                            style={
                                "padding": "0.45rem 1.2rem",
                                "height": "40px",
                                "whiteSpace": "nowrap",
                                "backgroundColor": "#0072B2",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "marginBottom": "0.3rem",
                    },
                ),
                html.Div(
                    id="col-fetch-status",
                    style={"marginBottom": "1rem", "fontSize": "0.85rem", "color": "#555"},
                ),

                # Data preview section (loading + main preview table)
                dcc.Loading(
                    id="loading-spinner",
                    type="circle",
                    color="#0072B2",
                    fullscreen=True,
                    children=[
                        html.Div(
                            id="loading-message",
                            style={
                                "textAlign": "center",
                                "fontWeight": "bold",
                                "color": "#0072B2",
                                "marginTop": "0.5rem",
                                "fontSize": "1rem",
                            },
                        ),
                        dash_table.DataTable(
                            id="preview",
                            page_size=10,   # 10 rows per page for fetched profiles
                            style_table={
                                "overflowX": "auto",
                                "marginTop": "0.8rem",
                                "width": "100%",
                            },
                            style_header={
                                "backgroundColor": "#0072B2",
                                "color": "white",
                                "fontWeight": "bold",
                            },
                            style_cell={
                                "textAlign": "left",
                                "fontSize": "0.8rem",
                                "padding": "5px",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"row_index": "odd"},
                                    "backgroundColor": "#f9f9f9",
                                }
                            ],
                        ),
                    ],
                ),

                # Download options section
                html.Hr(style={"marginTop": "1.8rem", "marginBottom": "1rem"}),
                html.H3(
                    "Download options",
                    style={
                        "marginBottom": "0.4rem",
                        "fontSize": "1.2rem",
                        "color": "#003366",
                    },
                ),

                # Download filters (campus + role)
                html.Div(
                    [
                        dcc.Dropdown(
                            id="fiscal-year-dd",
                            options=[{"label": "(all fiscal years)", "value": ""}],
                            value="",
                            placeholder="Filter: fiscal year",
                            style={"width": "220px"},
                        ),
                        dcc.Dropdown(
                            id="birth-campus-dd",
                            options=[{"label": "(all birth campuses)", "value": ""}],
                            value="",
                            placeholder="Filter: birth city campus",
                            style={"width": "260px", "marginLeft": "0.6rem"},
                        ),
                        dcc.Dropdown(
                            id="current-campus-dd",
                            options=[{"label": "(all current campuses)", "value": ""}],
                            value="",
                            placeholder="Filter: residence campus",
                            style={"width": "260px", "marginLeft": "0.6rem"},
                        ),
                        dcc.Dropdown(
                            id="role-dd",
                            options=ROLE_OPTS,
                            value="",
                            placeholder="Filter: role",
                            style={"width": "160px", "marginLeft": "0.6rem"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "marginBottom": "1rem",
                    },
                ),

                html.Details(
                    [
                        html.Summary(
                            "LOU sports selector",
                            style={"cursor": "pointer", "fontSize": "0.9rem", "color": "#555", "fontWeight": "bold"},
                        ),
                        html.Div(
                            "Toggle which sports count as LOU",
                            style={"marginTop": "0.6rem", "marginBottom": "0.4rem", "fontSize": "0.85rem", "color": "#555555"},
                        ),
                        dcc.Dropdown(
                            id="lou-sports-dd",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="LOU sports (auto-filled from unique sports)",
                            style={"width": "100%", "marginBottom": "0.2rem"},
                        ),
                    ],
                    open=False,
                    style={"marginBottom": "0.9rem"},
                ),

                html.Details(
                    [
                        html.Summary(
                            "Enhanced Excellence sport selector",
                            style={"cursor": "pointer", "fontSize": "0.9rem", "color": "#555", "fontWeight": "bold"},
                        ),
                        html.Div(
                            "Toggle which sports count as Enhanced Excellence",
                            style={"marginTop": "0.6rem", "marginBottom": "0.4rem", "fontSize": "0.85rem", "color": "#555555"},
                        ),
                        dcc.Dropdown(
                            id="enhanced-excellence-sports-dd",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Enhanced Excellence sports (auto-filled from unique sports)",
                            style={"width": "100%", "marginBottom": "0.2rem"},
                        ),
                    ],
                    open=False,
                    style={"marginBottom": "0.9rem"},
                ),

                # Filtered CSV preview
                html.Div(
                    "Filtered CSV preview (first 10 rows)",
                    style={
                        "marginBottom": "0.4rem",
                        "fontSize": "0.9rem",
                        "color": "#555555",
                    },
                ),
                dash_table.DataTable(
                    id="filtered-preview",
                    page_size=10,   # 10 rows per page for filtered preview
                    style_table={
                        "overflowX": "auto",
                        "marginBottom": "0.8rem",
                        "width": "100%",
                    },
                    style_header={
                        "backgroundColor": "#444444",
                        "color": "white",
                        "fontWeight": "bold",
                    },
                    style_cell={
                        "textAlign": "left",
                        "fontSize": "0.8rem",
                        "padding": "5px",
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#f9f9f9",
                        }
                    ],
                ),

                # Download buttons
                html.Div(
                    [
                        html.Button(
                            "Download CSV (full)",
                            id="btn-dl",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginRight": "0.8rem",
                            },
                        ),
                        html.Button(
                            "Download Filtered CSV",
                            id="btn-dl-filter",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "backgroundColor": "#e0e0e0",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "marginBottom": "0.7rem",
                    },
                ),

                # Column multiselect for filtered CSV (full width)
                html.Div(
                    [
                        dcc.Dropdown(
                            id="column-select",
                            options=[
                                {"label": label, "value": field}
                                for field, label in EXPORT_COLUMNS
                            ],
                            value=[field for field, _ in EXPORT_COLUMNS],  # default: all
                            multi=True,
                            placeholder="Select columns for filtered CSV",
                            style={"width": "100%"},
                        )
                    ],
                    style={"marginBottom": "0.6rem"},
                ),

                # ── Prebuilt Reports ──────────────────────────────────────────────
                html.Hr(style={"marginTop": "1.8rem", "marginBottom": "1rem"}),
                html.H3(
                    "Prebuilt Reports",
                    style={"marginBottom": "0.4rem", "fontSize": "1.2rem", "color": "#003366"},
                ),
                html.Div(
                    [
                        html.Button(
                            "Download Pending Unmatched Report",
                            id="btn-dl-pending-unmatched",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "backgroundColor": "#5c2d91",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download Unclaimed Nominations",
                            id="btn-dl-unclaimed-nominations",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#b83c00",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download Mail Merge CSV",
                            id="btn-dl-mailmerge",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#1a6b3c",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download National Inventory Data Set",
                            id="btn-dl-national-inventory",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#0a4f7a",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download Active Athletes",
                            id="btn-dl-active-athletes",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#2e7d32",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "rowGap": "0.6rem",
                        "marginBottom": "1rem",
                    },
                ),

                # ── ViaSport Sport Selectors ────────────────────────────────────
                html.H4(
                    "ViaSport sport selectors",
                    style={"marginBottom": "0.4rem", "fontSize": "1.05rem", "color": "#003366"},
                ),
                html.Div(
                    [
                        html.Button(
                            "Download ViaSport LOU List",
                            id="btn-dl-viasport-lou",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "backgroundColor": "#7b1e3a",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download ViaSport Enhanced Excellence List",
                            id="btn-dl-viasport-enhanced-excellence",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#005f73",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "rowGap": "0.6rem",
                        "marginBottom": "1rem",
                    },
                ),

                # ── ViaSport Reporting Prebuilt Lists ───────────────────────────
                html.H4(
                    "ViaSport reporting prebuilt lists",
                    style={"marginBottom": "0.4rem", "fontSize": "1.05rem", "color": "#003366"},
                ),
                html.Div(
                    [
                        html.Button(
                            "Download SC Carded List (2A)",
                            id="btn-dl-viasport-sc-carded",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "backgroundColor": "#7b1e3a",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download High Performance Athletes (2B)",
                            id="btn-dl-viasport-high-performance",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#005f73",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download Coaches (4B)",
                            id="btn-dl-viasport-coaches",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#2e7d32",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Download All ViaSport Reports",
                            id="btn-dl-viasport-all",
                            n_clicks=0,
                            disabled=True,
                            style={
                                "padding": "0.45rem 1.2rem",
                                "marginLeft": "0.8rem",
                                "backgroundColor": "#0a4f7a",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "rowGap": "0.6rem",
                        "marginBottom": "1rem",
                    },
                ),

                # Collapsible technical log at the very bottom
                html.Details(
                    [
                        html.Summary(
                            "Technical request log (advanced)",
                            style={
                                "cursor": "pointer",
                                "fontSize": "0.9rem",
                                "color": "#555",
                                "fontWeight": "bold",
                            },
                        ),
                        html.Pre(
                            id="log",
                            style={
                                "whiteSpace": "pre-wrap",
                                "background": "#f7f7f7",
                                "height": "25vh",
                                "overflow": "auto",
                                "padding": "0.7rem",
                                "fontSize": "0.8rem",
                                "border": "1px solid #ddd",
                                "marginTop": "0.8rem",
                            },
                        ),
                    ],
                    open=False,
                    style={"marginTop": "1.0rem"},
                ),

                dcc.Store(id="col-defs-store", data=[]),
                dcc.Download(id="csv-file"),
                dcc.Download(id="csv-file-filtered"),
                dcc.Download(id="csv-file-pending-unmatched"),
                dcc.Download(id="csv-file-unclaimed-nominations"),
                dcc.Download(id="csv-file-mailmerge"),
                dcc.Download(id="csv-file-national-inventory"),
                dcc.Download(id="csv-file-active-athletes"),
                dcc.Download(id="csv-file-viasport-lou"),
                dcc.Download(id="csv-file-viasport-enhanced-excellence"),
                dcc.Download(id="csv-file-viasport-sc-carded"),
                dcc.Download(id="csv-file-viasport-high-performance"),
                dcc.Download(id="csv-file-viasport-coaches"),
                dcc.Download(id="csv-file-viasport-all"),
            ],
        ),
        html.Div(id="guide-tab-content", children=build_report_guide_tab(), style={"display": "none"}),
    ],
)


@app.callback(
    Output("export-tab-content", "style"),
    Output("guide-tab-content", "style"),
    Input("page-tabs", "value"),
)
def switch_page_tab(tab_value):
    if tab_value == "tab-guide":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}

# -------------------------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------------------------
@app.callback(
    Output("col-defs-store", "data"),
    Output("fetch-columns-select", "options"),
    Output("fetch-columns-select", "value"),
    Output("col-fetch-status", "children"),
    Input("btn-fetch-columns", "n_clicks"),
    prevent_initial_call=True,
)
def fetch_columns_callback(_):
    token = auth.get_token()
    if not token:
        return [], [], [], "No OAuth token – log in."
    headers = {"Authorization": f"Bearer {token}"}
    log = []
    col_defs = fetch_column_defs(headers, log)
    options = [
        {"label": c.get("label", c.get("key", "")), "value": c.get("key", "")}
        for c in col_defs
        if c.get("key")
    ]
    values = [o["value"] for o in options]
    if options:
        status = f"Loaded {len(options)} column(s). Select which to include, then click Fetch Rows."
    else:
        reason = "; ".join(log) if log else "unknown error"
        status = f"Failed to load columns: {reason}"
    return col_defs, options, values, status


@app.callback(
    Output("preview", "data"),
    Output("preview", "columns"),
    Output("btn-dl", "disabled"),
    Output("btn-dl-filter", "disabled"),
    Output("btn-dl-pending-unmatched", "disabled"),
    Output("btn-dl-unclaimed-nominations", "disabled"),
    Output("btn-dl-mailmerge", "disabled"),
    Output("btn-dl-national-inventory", "disabled"),
    Output("btn-dl-active-athletes", "disabled"),
    Output("btn-dl-viasport-lou", "disabled"),
    Output("btn-dl-viasport-enhanced-excellence", "disabled"),
    Output("btn-dl-viasport-sc-carded", "disabled"),
    Output("btn-dl-viasport-high-performance", "disabled"),
    Output("btn-dl-viasport-coaches", "disabled"),
    Output("btn-dl-viasport-all", "disabled"),
    Output("log", "children"),
    Output("loading-message", "children"),
    Output("column-select", "options"),
    Output("column-select", "value"),
    Output("fiscal-year-dd", "options"),
    Output("fiscal-year-dd", "value"),
    Output("birth-campus-dd", "options"),
    Output("birth-campus-dd", "value"),
    Output("current-campus-dd", "options"),
    Output("current-campus-dd", "value"),
    Output("lou-sports-dd", "options"),
    Output("lou-sports-dd", "value"),
    Output("enhanced-excellence-sports-dd", "options"),
    Output("enhanced-excellence-sports-dd", "value"),
    Input("btn-fetch-rows", "n_clicks"),
    State("fetch-columns-select", "value"),
    State("col-defs-store", "data"),
    prevent_initial_call=True,
)
def fetch_profiles(_, fetch_col_val, col_defs_data):
    token = auth.get_token()
    if not token:
        return (
            no_update,
            no_update,
            *([True] * 13),
            "No OAuth token – log in.",
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    headers = {"Authorization": f"Bearer {token}"}

    flattened, log_lines = [], []

    # Use col_defs from the store (populated by Fetch Columns button)
    col_defs = col_defs_data or []

    # Build report_meta from col_defs so flatten_report_row has column metadata
    report_meta = {
        "columns": col_defs,
        "defaults": [],
        "system": [],
    }

    # Use user-selected columns if any; otherwise use all from store
    if fetch_col_val:
        selected_keys = [k for k in fetch_col_val if k]
    else:
        selected_keys = [c.get("key") for c in col_defs if c.get("key")]

    # Build the fetch URL with the ?columns= query param
    fetch_url = PROFILES_URL
    if selected_keys:
        fetch_url += "?columns=" + ",".join(selected_keys)

    log_lines.append(f"\nPulling report rows: {len(selected_keys)} column(s) requested")

    batch, batch_meta = fetch_paginated(fetch_url, headers, log_lines)
    if batch_meta.get("columns"):
        report_meta["columns"] = batch_meta["columns"]

    for r in batch:
        # report-rows payloads store values in row['cells'] and row['meta'].
        if isinstance(r.get("cells"), dict) or isinstance(r.get("meta"), dict) or report_meta.get("columns"):
            flattened.append(flatten_report_row(r, None, report_meta.get("columns", [])))
        else:
            # Fallback for legacy profile payloads
            flattened.append(flatten_profile(r, 0))

    log_lines.append(f"  added {len(batch)} rows")

    if not flattened:
        return (
            [],
            [],
            *([True] * 13),
            "\n".join(log_lines),
            "No profiles found.",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    df = pd.DataFrame(flattened)

    df = add_level_category(df)

    global cached_df, cached_name
    global ACTIVE_EXPORT_COLUMNS, ACTIVE_FILTER_COLUMNS, ACTIVE_FIELD_TO_LABEL
    cached_df = df.copy()
    ACTIVE_EXPORT_COLUMNS = build_export_columns(df, report_meta)
    ACTIVE_FILTER_COLUMNS = [field for field, _ in ACTIVE_EXPORT_COLUMNS]
    ACTIVE_FIELD_TO_LABEL = {field: label for field, label in ACTIVE_EXPORT_COLUMNS}
    cached_name = "profiles_all.csv"

    columns = [{"name": ACTIVE_FIELD_TO_LABEL.get(c, c), "id": c} for c in df.columns]
    loading_msg = f"Fetched {len(df)} profiles."

    col_options = [
        {"label": label, "value": field}
        for field, label in ACTIVE_EXPORT_COLUMNS
    ]
    col_values = [field for field, _ in ACTIVE_EXPORT_COLUMNS]

    # Build fiscal year options from the full cached df
    fiscal_year_options = [{"label": "(all fiscal years)", "value": ""}]
    fy_col = next((c for c in ["nomination_fiscal_year", "fiscal_year", "Fiscal Year"] if c in df.columns), None)
    if fy_col:
        fy_vals = sorted(v for v in df[fy_col].dropna().astype(str).str.strip().unique() if v and v != "nan")
        fiscal_year_options += [{"label": v, "value": v} for v in fy_vals]

    # Build birth city campus options from the full cached df
    birth_campus_options = [{"label": "(all birth campuses)", "value": ""}]
    if "birth_city_campus" in df.columns:
        bvals = sorted(v for v in df["birth_city_campus"].dropna().astype(str).unique() if v and v != "nan")
        birth_campus_options += [{"label": v, "value": v} for v in bvals]

    # Build residence city campus options from the full cached df
    res_campus_options = [{"label": "(all current campuses)", "value": ""}]
    if "residence_city_campus" in df.columns:
        rvals = sorted(v for v in df["residence_city_campus"].dropna().astype(str).unique() if v and v != "nan")
        res_campus_options += [{"label": v, "value": v} for v in rvals]

    lou_sport_options = build_unique_sport_options(df)
    lou_sport_values = build_default_lou_sports(lou_sport_options)
    ee_sport_options = lou_sport_options
    ee_sport_values = build_default_enhanced_excellence_sports(ee_sport_options)

    return (
        df.to_dict("records"),
        columns,
        *([False] * 13),
        "\n".join(log_lines),
        loading_msg,
        col_options,
        col_values,
        fiscal_year_options,
        "",   # reset fiscal year filter
        birth_campus_options,
        "",   # reset birth campus filter
        res_campus_options,
        "",   # reset current campus filter
        lou_sport_options,
        lou_sport_values,
        ee_sport_options,
        ee_sport_values,
    )

@app.callback(
    Output("csv-file", "data"),
    Input("btn-dl", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_csv(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_out = apply_fiscal_year_filter(cached_df, fiscal_year_val)
    if df_out.empty:
        return no_update

    return dcc.send_data_frame(df_out.to_csv, cached_name, index=False)

@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    State("role-dd", "value"),
    State("column-select", "value"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_filtered_csv(
    _,
    birth_campus_val,
    current_campus_val,
    role_val,
    selected_fields,
    fiscal_year_val,
):
    if cached_df.empty:
        return no_update

    df_out = apply_campus_filters(
        cached_df, None, birth_campus_val, current_campus_val
    )
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)

    if role_val and "role" in df_out.columns:
        df_out = df_out[df_out["role"].str.lower() == role_val.lower()]

    # Strip out TEST sports before selecting columns / renaming
    df_out = remove_test_sports(df_out)
    df_out = add_level_category(df_out)

    # Use selected fields if provided, otherwise default to all export columns
    if not selected_fields:
        selected_fields = ACTIVE_FILTER_COLUMNS

    # Only keep fields that actually exist in df
    fields = [f for f in selected_fields if f in df_out.columns]
    if not fields:
        return no_update

    df_filtered = df_out[fields].copy()

    # Rename to pretty labels
    rename_map = {field: ACTIVE_FIELD_TO_LABEL.get(field, field) for field in fields}
    df_filtered.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_filtered.csv")
    return dcc.send_data_frame(df_filtered.to_csv, filename, index=False)

@app.callback(
    Output("csv-file-pending-unmatched", "data"),
    Input("btn-dl-pending-unmatched", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_pending_unmatched(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_base = cached_df.copy()
    df_base = apply_fiscal_year_filter(df_base, fiscal_year_val)
    df_base = remove_test_sports(df_base)
    if df_base.empty:
        return no_update

    # Exclude rows where nomination has already been claimed
    nomination_col = next(
        (c for c in ["nomination_claimed", "current_nomination.redeemed"] if c in df_base.columns),
        None,
    )
    if nomination_col:
        s = df_base[nomination_col].fillna("").astype(str).str.strip().str.lower()
        df_base = df_base[~s.isin(["true"])]

    # Find pairs of profile+nomination rows with matching/similar names
    df_out = find_name_matched_pairs(df_base)

    if df_out.empty:
        return no_update

    # Fixed columns for this report
    pending_cols = [
        ("role",           "Role"),
        ("first_name",     "First Name"),
        ("last_name",      "Last Name"),
        ("guardian_email", "Guardian Email"),
        ("sport",          "Sport"),
        ("enrollment_status", "Enrollment Status"),
        ("email",          "Email"),
    ]
    fields = [f for f, _ in pending_cols if f in df_out.columns]
    rename_map = {f: lbl for f, lbl in pending_cols if f in df_out.columns}
    df_out = df_out[fields].copy()
    df_out.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_pending_unmatched.csv")
    return dcc.send_data_frame(df_out.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-unclaimed-nominations", "data"),
    Input("btn-dl-unclaimed-nominations", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_unclaimed_nominations(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)
    df_out = remove_test_sports(df_out)
    if df_out.empty:
        return no_update

    # Keep only rows where nomination_approved = True AND nomination_claimed = False
    # Exclude rows where both values are blank
    claimed_col = next(
        (c for c in ["nomination_claimed", "current_nomination.redeemed"] if c in df_out.columns),
        None,
    )
    approved_col = next(
        (c for c in ["nomination_approved"] if c in df_out.columns),
        None,
    )

    claimed_s = (
        df_out[claimed_col].fillna("").astype(str).str.strip().str.lower()
        if claimed_col else pd.Series("", index=df_out.index)
    )
    approved_s = (
        df_out[approved_col].fillna("").astype(str).str.strip().str.lower()
        if approved_col else pd.Series("", index=df_out.index)
    )

    both_blank = (claimed_s == "") & (approved_s == "")
    mask = (~both_blank) & (claimed_s == "false") & (approved_s == "true")
    df_out = df_out[mask]

    # Sort alphabetically by last name
    last_name_col = next(
        (c for c in ["last_name", "person.last_name", "Last Name"] if c in df_out.columns),
        None,
    )
    if last_name_col:
        df_out = df_out.sort_values(by=last_name_col, ascending=True, key=lambda s: s.str.lower().fillna(""))

    # Deduplicate by first + last name (keep first occurrence after sort)
    first_name_col = next(
        (c for c in ["first_name", "person.first_name", "First Name"] if c in df_out.columns),
        None,
    )
    dedup_cols = [c for c in [first_name_col, last_name_col] if c]
    if dedup_cols:
        df_out = df_out.drop_duplicates(subset=dedup_cols, keep="first")

    # Fixed columns for this report
    unclaimed_cols = [
        ("role",                "Role"),
        ("first_name",         "First Name"),
        ("last_name",          "Last Name"),
        ("email",              "Email"),
        ("guardian_first_name", "Guardian First Name"),
        ("guardian_last_name",  "Guardian Last Name"),
        ("guardian_email",     "Guardian Email"),
        ("age",                "Age"),
    ]
    fields = [f for f, _ in unclaimed_cols if f in df_out.columns]
    rename_map = {f: lbl for f, lbl in unclaimed_cols if f in df_out.columns}
    df_out = df_out[fields].copy()
    df_out.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_unclaimed_nominations.csv")
    return dcc.send_data_frame(df_out.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-mailmerge", "data"),
    Input("btn-dl-mailmerge", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_mailmerge(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)
    df_out = remove_test_sports(df_out)
    if df_out.empty:
        return no_update

    # Same filter as unclaimed nominations
    claimed_col = next(
        (c for c in ["nomination_claimed", "current_nomination.redeemed"] if c in df_out.columns),
        None,
    )
    approved_col = next(
        (c for c in ["nomination_approved"] if c in df_out.columns),
        None,
    )

    claimed_s = (
        df_out[claimed_col].fillna("").astype(str).str.strip().str.lower()
        if claimed_col else pd.Series("", index=df_out.index)
    )
    approved_s = (
        df_out[approved_col].fillna("").astype(str).str.strip().str.lower()
        if approved_col else pd.Series("", index=df_out.index)
    )

    both_blank = (claimed_s == "") & (approved_s == "")
    mask = (~both_blank) & (claimed_s == "false") & (approved_s == "true")
    df_out = df_out[mask]

    # Sort by last name
    last_name_col = next(
        (c for c in ["last_name", "person.last_name", "Last Name"] if c in df_out.columns),
        None,
    )
    if last_name_col:
        df_out = df_out.sort_values(by=last_name_col, ascending=True, key=lambda s: s.str.lower().fillna(""))

    # Deduplicate by first + last name
    first_name_col = next(
        (c for c in ["first_name", "person.first_name", "First Name"] if c in df_out.columns),
        None,
    )
    dedup_cols = [c for c in [first_name_col, last_name_col] if c]
    if dedup_cols:
        df_out = df_out.drop_duplicates(subset=dedup_cols, keep="first")

    if df_out.empty:
        return no_update

    # Determine "To" email: under 19 → guardian_email, 19+ → email
    age_col = next((c for c in ["age"] if c in df_out.columns), None)
    email_col = next((c for c in ["email"] if c in df_out.columns), None)
    guardian_email_col = next((c for c in ["guardian_email"] if c in df_out.columns), None)

    def get_to_email(row):
        try:
            age_num = float(str(row[age_col]).strip()) if age_col else None
        except (ValueError, TypeError):
            age_num = None
        athlete_em = str(row[email_col] or "").strip() if email_col else ""
        guardian_em = str(row[guardian_email_col] or "").strip() if guardian_email_col else ""
        if age_num is not None and age_num < 19:
            return guardian_em if guardian_em else athlete_em
        return athlete_em if athlete_em else guardian_em

    df_out["To"] = df_out.apply(get_to_email, axis=1)

    # Build 3-column output
    merge_data = {}
    if first_name_col:
        merge_data["First Name"] = df_out[first_name_col].values
    if last_name_col:
        merge_data["Last Name"] = df_out[last_name_col].values
    merge_data["To"] = df_out["To"].values
    df_merge = pd.DataFrame(merge_data)

    # Split into chunks of max 450
    CHUNK_SIZE = 450
    chunks = [df_merge.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df_merge), CHUNK_SIZE)]
    base = cached_name.replace(".csv", "")

    if len(chunks) == 1:
        return dcc.send_data_frame(chunks[0].to_csv, f"{base}_mailmerge.csv", index=False)

    # Multiple chunks → zip
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, chunk in enumerate(chunks, 1):
            zf.writestr(f"{base}_mailmerge_part{i}.csv", chunk.to_csv(index=False).encode("utf-8"))
    zip_buffer.seek(0)
    return dcc.send_bytes(zip_buffer.read(), f"{base}_mailmerge.zip")


@app.callback(
    Output("csv-file-national-inventory", "data"),
    Input("btn-dl-national-inventory", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_national_inventory(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)
    df_out = remove_test_sports(df_out)
    if df_out.empty:
        return no_update
    df_out = add_level_category(df_out)

    approved_col = next(
        (c for c in ["nomination_approved", "current_nomination.approved"] if c in df_out.columns),
        None,
    )
    if approved_col:
        approved_s = df_out[approved_col].fillna("").astype(str).str.strip().str.lower()
        df_out = df_out[approved_s == "true"]
    else:
        return no_update

    if df_out.empty:
        return no_update

    dedup_fields = [
        field for field in ["role", "first_name", "last_name", "sport", "nomination_claimed", "current_nomination.redeemed"]
        if field in df_out.columns
    ]
    if dedup_fields:
        df_out = df_out.drop_duplicates(subset=dedup_fields, keep="first")

    # Ensure season is populated from sport when missing/blank.
    sport_col = next((c for c in ["sport", "Sport"] if c in df_out.columns), None)
    season_col = next((c for c in ["season", "Season"] if c in df_out.columns), None)
    if sport_col:
        if season_col:
            season_s = df_out[season_col].fillna("").astype(str).str.strip()
            missing_season = season_s == ""
            if missing_season.any():
                df_out.loc[missing_season, season_col] = df_out.loc[missing_season, sport_col].apply(
                    infer_season_from_sport
                )
        else:
            df_out["season"] = df_out[sport_col].apply(infer_season_from_sport)

    # Allow fallback aliases for fields that may differ by endpoint version.
    field_aliases = {
        "training_group": ["training_group", "training_groups", "training_group_name"],
    }

    selected = []
    rename_map = {}
    for target_field, target_label in NATIONAL_INVENTORY_COLUMNS:
        candidates = field_aliases.get(target_field, [target_field])
        actual = next((c for c in candidates if c in df_out.columns), None)
        if actual:
            selected.append(actual)
            rename_map[actual] = target_label

    if not selected:
        return no_update

    df_export = df_out[selected].copy()
    df_export.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_national_inventory_dataset.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


# Preview of the filtered CSV (first 10 rows, same logic as download)
@app.callback(
    Output("filtered-preview", "data"),
    Output("filtered-preview", "columns"),
    Input("fiscal-year-dd", "value"),
    Input("birth-campus-dd", "value"),
    Input("current-campus-dd", "value"),
    Input("role-dd", "value"),
    Input("column-select", "value"),
)
def update_filtered_preview(
    fiscal_year_val,
    birth_campus_val,
    current_campus_val,
    role_val,
    selected_fields,
):
    if cached_df.empty:
        return [], []

    df_out = apply_campus_filters(
        cached_df, None, birth_campus_val, current_campus_val
    )
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)

    if role_val and "role" in df_out.columns:
        df_out = df_out[df_out["role"].str.lower() == role_val.lower()]

    # Remove TEST sports
    df_out = remove_test_sports(df_out)
    df_out = add_level_category(df_out)

    if df_out.empty:
        return [], []

    # Use selected fields if provided, otherwise default to all export columns
    if not selected_fields:
        selected_fields = ACTIVE_FILTER_COLUMNS

    # Only keep fields that actually exist in df
    fields = [f for f in selected_fields if f in df_out.columns]
    if not fields:
        return [], []

    df_filtered = df_out[fields].copy()

    # Rename to pretty labels (same as CSV)
    rename_map = {field: ACTIVE_FIELD_TO_LABEL.get(field, field) for field in fields}
    df_filtered.rename(columns=rename_map, inplace=True)

    # Only show first 10 rows in preview
    df_preview = df_filtered.head(10)

    columns = [{"name": c, "id": c} for c in df_preview.columns]
    data = df_preview.to_dict("records")

    return data, columns

@app.callback(
    Output("csv-file-active-athletes", "data"),
    Input("btn-dl-active-athletes", "n_clicks"),
    State("fiscal-year-dd", "value"),
    prevent_initial_call=True,
)
def download_active_athletes(_, fiscal_year_val):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)

    # Remove test sports
    df_out = remove_test_sports(df_out)
    if df_out.empty:
        return no_update

    df_out = add_level_category(df_out)

    # Keep only rows where nomination_claimed is True
    claimed_col = next(
        (c for c in ["nomination_claimed", "current_nomination.redeemed"] if c in df_out.columns),
        None,
    )
    if claimed_col:
        claimed_s = df_out[claimed_col].fillna("").astype(str).str.strip().str.lower()
        df_out = df_out[claimed_s == "true"]

    if df_out.empty:
        return no_update

    # Build ordered column list using ACTIVE_EXPORT_COLUMNS, excluding unwanted keys
    ordered_fields = [
        field for field, _ in ACTIVE_EXPORT_COLUMNS
        if field in df_out.columns and field not in ACTIVE_ATHLETES_EXCLUDE_KEYS
    ]
    # Append any remaining columns not in ACTIVE_EXPORT_COLUMNS (also excluding unwanted)
    extra_fields = [
        c for c in df_out.columns
        if c not in ordered_fields and c not in ACTIVE_ATHLETES_EXCLUDE_KEYS
    ]
    fields = ordered_fields + extra_fields

    if not fields:
        return no_update

    df_export = df_out[fields].copy()

    # Rename to pretty labels
    rename_map = {f: ACTIVE_FIELD_TO_LABEL.get(f, f) for f in fields}
    df_export.rename(columns=rename_map, inplace=True)

    filename = cached_name.replace(".csv", "_active_athletes.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-lou", "data"),
    Input("btn-dl-viasport-lou", "n_clicks"),
    State("fiscal-year-dd", "value"),
    State("lou-sports-dd", "value"),
    prevent_initial_call=True,
)
def download_viasport_lou(_, fiscal_year_val, lou_sports):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)
    df_out = remove_test_sports(df_out)
    df_out = filter_by_selected_sports(df_out, lou_sports)
    df_out = add_level_category(df_out)

    if df_out.empty:
        return no_update

    df_export = build_prebuilt_export_df(df_out)
    filename = cached_name.replace(".csv", "_viasport_lou_list.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-enhanced-excellence", "data"),
    Input("btn-dl-viasport-enhanced-excellence", "n_clicks"),
    State("fiscal-year-dd", "value"),
    State("enhanced-excellence-sports-dd", "value"),
    prevent_initial_call=True,
)
def download_viasport_enhanced_excellence(_, fiscal_year_val, enhanced_excellence_sports):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()
    df_out = apply_fiscal_year_filter(df_out, fiscal_year_val)
    df_out = remove_test_sports(df_out)
    df_out = filter_by_selected_sports(df_out, enhanced_excellence_sports)
    df_out = add_level_category(df_out)

    if df_out.empty:
        return no_update

    df_export = build_prebuilt_export_df(df_out)
    filename = cached_name.replace(".csv", "_viasport_enhanced_excellence_list.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-sc-carded", "data"),
    Input("btn-dl-viasport-sc-carded", "n_clicks"),
    prevent_initial_call=True,
)
def download_viasport_sc_carded(_):
    if cached_df.empty:
        return no_update

    df_out = filter_viasport_base(cached_df, "athlete", "Canadian Elite")
    if df_out.empty:
        return no_update

    df_export = build_viasport_sc_carded_export(df_out)
    filename = cached_name.replace(".csv", "_viasport_sc_carded_list.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-high-performance", "data"),
    Input("btn-dl-viasport-high-performance", "n_clicks"),
    prevent_initial_call=True,
)
def download_viasport_high_performance(_):
    if cached_df.empty:
        return no_update

    df_out = filter_viasport_base(cached_df, "athlete", "Provincial Development")
    if df_out.empty:
        return no_update

    df_export = build_viasport_high_performance_export(df_out)
    filename = cached_name.replace(".csv", "_viasport_high_performance_athletes.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-coaches", "data"),
    Input("btn-dl-viasport-coaches", "n_clicks"),
    prevent_initial_call=True,
)
def download_viasport_coaches(_):
    if cached_df.empty:
        return no_update

    df_out = filter_viasport_base(cached_df, "coach")
    if df_out.empty:
        return no_update

    df_export = build_viasport_coaches_export(df_out)
    filename = cached_name.replace(".csv", "_viasport_coaches.csv")
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)


@app.callback(
    Output("csv-file-viasport-all", "data"),
    Input("btn-dl-viasport-all", "n_clicks"),
    prevent_initial_call=True,
)
def download_viasport_all(_):
    if cached_df.empty:
        return no_update

    bundles = {
        "sc_carded_list": filter_viasport_base(cached_df, "athlete", "Canadian Elite"),
        "high_performance_athletes": filter_viasport_base(cached_df, "athlete", "Provincial Development"),
        "coaches": filter_viasport_base(cached_df, "coach"),
    }

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for key, report_df in bundles.items():
            if key == "sc_carded_list":
                export_df = build_viasport_sc_carded_export(report_df)
            elif key == "high_performance_athletes":
                export_df = build_viasport_high_performance_export(report_df)
            else:
                export_df = build_viasport_coaches_export(report_df)
            zf.writestr(
                f"{cached_name.replace('.csv', '')}_viasport_{key}.csv",
                export_df.to_csv(index=False).encode("utf-8"),
            )
    zip_buffer.seek(0)
    return dcc.send_bytes(zip_buffer.read(), cached_name.replace(".csv", "_viasport_reports.zip"))


# -------------------------------------------------------------------------
if __name__ == "__main__":
    in_spyder = running_in_spyder()
    run_host = os.getenv("HOST", "127.0.0.1" if in_spyder else "0.0.0.0")
    run_port = int(os.getenv("PORT", "8050"))
    debug_env = os.getenv("DASH_DEBUG")
    run_debug = (debug_env == "1") if debug_env is not None else (not in_spyder)
    app.run(
        debug=run_debug,
        host=run_host,
        port=run_port,
        use_reloader=not in_spyder,
    )

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
import json, time, requests, pandas as pd, random, difflib
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
# TEST SPORTS TO EXCLUDE FROM FILTERED DOWNLOAD
# -------------------------------------------------------------------------
TEST_SPORTS = {
    "Cinderball (TEST)",
    "Skimboarding Cross (TEST)",
}
TEST_SPORTS_NORMALIZED = {s.strip().lower() for s in TEST_SPORTS}

def remove_test_sports(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where any of the sport columns indicate TEST sports.
    Checks both internal and pretty column names, case-insensitive.
    """
    if df.empty:
        return df

    cols = [
        c for c in [
            "sport",
            "nomination_sport_name",
            "Sport",
            "Nomination Sport Name",
        ]
        if c in df.columns
    ]
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

# -------------------------------------------------------------------------
# LAYOUT
# -------------------------------------------------------------------------
app.layout = html.Div(
    style={"fontFamily": "Arial", "margin": "2rem"},
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
                    id="birth-campus-dd",
                    options=[{"label": "(all birth campuses)", "value": ""}],
                    value="",
                    placeholder="Filter: birth city campus",
                    style={"width": "260px"},
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
            ],
            style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "marginBottom": "1rem"},
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
    ],
)

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
    Output("log", "children"),
    Output("loading-message", "children"),
    Output("column-select", "options"),
    Output("column-select", "value"),
    Output("birth-campus-dd", "options"),
    Output("birth-campus-dd", "value"),
    Output("current-campus-dd", "options"),
    Output("current-campus-dd", "value"),
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
            True,
            True,
            True,
            True,
            "No OAuth token – log in.",
            "",
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
            True,
            True,
            True,
            True,
            "\n".join(log_lines),
            "No profiles found.",
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

    return (
        df.to_dict("records"),
        columns,
        False,
        False,
        False,
        False,
        "\n".join(log_lines),
        loading_msg,
        col_options,
        col_values,
        birth_campus_options,
        "",   # reset birth campus filter
        res_campus_options,
        "",   # reset current campus filter
    )

@app.callback(
    Output("csv-file", "data"),
    Input("btn-dl", "n_clicks"),
    prevent_initial_call=True,
)
def download_csv(_):
    if cached_df.empty:
        return no_update
    return dcc.send_data_frame(cached_df.to_csv, cached_name, index=False)

@app.callback(
    Output("csv-file-filtered", "data"),
    Input("btn-dl-filter", "n_clicks"),
    State("birth-campus-dd", "value"),
    State("current-campus-dd", "value"),
    State("role-dd", "value"),
    State("column-select", "value"),
    prevent_initial_call=True,
)
def download_filtered_csv(
    _, birth_campus_val, current_campus_val, role_val, selected_fields
):
    if cached_df.empty:
        return no_update

    df_out = apply_campus_filters(
        cached_df, None, birth_campus_val, current_campus_val
    )

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
    prevent_initial_call=True,
)
def download_pending_unmatched(_):
    if cached_df.empty:
        return no_update

    df_base = cached_df.copy()

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
    prevent_initial_call=True,
)
def download_unclaimed_nominations(_):
    if cached_df.empty:
        return no_update

    df_out = cached_df.copy()

    # Keep only rows where nomination claimed is explicitly True (redeemed=False means unclaimed)
    nomination_col = next(
        (c for c in ["nomination_claimed", "current_nomination.redeemed"] if c in df_out.columns),
        None,
    )
    if nomination_col:
        s = df_out[nomination_col].fillna("").astype(str).str.strip().str.lower()
        df_out = df_out[s == "false"]

    # Sort alphabetically by last name
    last_name_col = next(
        (c for c in ["last_name", "person.last_name", "Last Name"] if c in df_out.columns),
        None,
    )
    if last_name_col:
        df_out = df_out.sort_values(by=last_name_col, ascending=True, key=lambda s: s.str.lower().fillna(""))

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


# Preview of the filtered CSV (first 10 rows, same logic as download)
@app.callback(
    Output("filtered-preview", "data"),
    Output("filtered-preview", "columns"),
    Input("birth-campus-dd", "value"),
    Input("current-campus-dd", "value"),
    Input("role-dd", "value"),
    Input("column-select", "value"),
)
def update_filtered_preview(
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

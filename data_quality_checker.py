import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import plotly.express as px
import plotly.graph_objects as go
import yaml
import pathlib
import re

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Cocoa Campaign Data Quality Checker", layout="wide")

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #175259 0%, #2d6a4f 100%);
        padding: 2rem; border-radius: 12px; margin-bottom: 2rem;
        color: white; text-align: center;
    }
    .section-header {
        font-size: 1.1rem; font-weight: 700; color: #175259;
        border-bottom: 2px solid #2d6a4f;
        padding-bottom: 0.4rem; margin: 1.5rem 0 1rem 0;
    }
    .issue-badge {
        display:inline-block; padding:2px 8px; border-radius:4px;
        font-size:0.78rem; font-weight:600;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1 style="margin:0;font-size:2rem;">🌿 Cocoa Campaign Data Quality Checker</h1>
    <p style="margin:0.5rem 0 0 0;opacity:0.9;">
        Upload your campaign Excel file · Select sheet & columns · Run domain-aware checks · Export flagged records
    </p>
</div>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
BATCH_SIZE = 50

CHART_COLORS = {
    "missing":   "#E74C3C", "duplicate": "#BCDB34",
    "outlier":   "#F39C12", "dtype":     "#9B59B6",
    "banned":    "#C0392B", "cross":     "#E67E22",
    "clean":     "#2ECC71", "primary":   "#175259",
}

# ── Load rules from YAML ───────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_rules(yaml_path: str) -> dict:
    """Load domain rules and mandatory fields from quality_rules.yaml."""
    path = pathlib.Path(yaml_path)
    if not path.exists():
        st.error(f"Rules file not found: {yaml_path}\n"
                 "Place `quality_rules.yaml` in the same folder as this script.")
        st.stop()
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Resolve YAML path relative to this script so the app works from any cwd
_SCRIPT_DIR  = pathlib.Path(__file__).parent
_RULES_PATH  = _SCRIPT_DIR / "quality_rules.yaml"
_RULES       = load_rules(str(_RULES_PATH))

# domain_rules is a list of dicts; convert to list for iteration
DOMAIN_RULES:    list = _RULES.get("domain_rules", [])
# mandatory_fields is a dict keyed by sheet substring
MANDATORY_MAP:   dict = _RULES.get("mandatory_fields", {})
DISPLAY_COLS:    list = MANDATORY_MAP.get("display_columns", [])

OUTLIER_CFG:   dict = _RULES.get("outlier_detection", {})
OUTLIER_COLS:  list = OUTLIER_CFG.get("cols", [])
OUTLIER_GROUP: str  = OUTLIER_CFG.get("country_col", "Country")


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe_str(df):
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            types = out[col].dropna().map(type).unique()
            if len(types) > 1:
                out[col] = out[col].astype(str)
    return out


def coerce_cell(v):
    if isinstance(v, (np.integer,)):   return int(v)
    if isinstance(v, (np.floating,)):  return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_,)):     return bool(v)
    if isinstance(v, float) and np.isnan(v): return None
    if not isinstance(v, (int, float, str, bool, type(None))): return str(v)
    return v


def safe_concat(frames, base_cols):
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    subset = [c for c in base_cols if c in combined.columns]
    if subset:
        combined = combined.drop_duplicates(subset=subset)
    return combined


def slim_view(issue_df, all_cols, checked_cols=None):
    """
    Returns exactly:
      1. DISPLAY_COLS from quality_rules.yaml (those that exist in this sheet)
      2. ALL column(s) mentioned in the issue text (1, 2, 3 or more)
      3. __issue_type__ and __issues__
    """
    if issue_df.empty:
        return issue_df

    # Anchor columns from yaml
    anchor = [c for c in DISPLAY_COLS if c in issue_df.columns]

    # ALL flagged columns across ALL issue rows
    def flagged_cols(txt):
        return [
            c for c in issue_df.columns
            if c not in anchor
            and c not in ("__issue_type__", "__issue__")
            and re.search(re.escape(c), str(txt)) 
        ]

    issue_cols = list(dict.fromkeys(
        c
        for t in issue_df["__issues__"]      # iterate every issue row
        for c in flagged_cols(t)             # find ALL col matches in that text
    ))

    # Meta always last
    meta = ["__issue_type__", "__issues__"]

    keep = anchor + issue_cols + meta
    return issue_df[[c for c in keep if c in issue_df.columns]]


# ── Core checks ────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def check_missing(df, cols):
    cols = list(cols)
    mask = df[cols].isnull().any(axis=1)
    result = df[mask].copy()
    result["__issues__"] = result.apply(
        lambda r: "Missing: " + ", ".join(c for c in cols if pd.isnull(r[c])), axis=1)
    result["__issue_type__"] = "Missing Value"
    return result


@st.cache_data(show_spinner=False)
def check_duplicates(df, cols, keep="first"):
    cols = list(cols)
    keep_arg = False if keep == "none" else keep
    mask = df.duplicated(subset=cols, keep=keep_arg)
    result = df[mask].copy()
    if result.empty: return result
    def find_orig(idx):
        row_vals = df.loc[idx, cols]
        others = [i + 2 for i in df[(df[cols] == row_vals).all(axis=1)].index if i != idx]
        return f"Duplicate of row(s): {others}"
    result["__issues__"] = [find_orig(i) for i in result.index]
    result["__issue_type__"] = "Duplicate"
    return result


@st.cache_data(show_spinner=False)
def check_outliers_batch(df, cols, group_col, method="IQR"):
    flagged_rows = []
    issues_list  = []
    types_list   = []

    methods_cfg = OUTLIER_CFG.get("methods", {})
    has_group   = group_col and group_col in df.columns

    for col in cols:
        if col not in df.columns:
            continue

        s_full = pd.to_numeric(df[col], errors="coerce")
        groups = df[group_col].unique() if has_group else ["_all_"]

        for group in groups:
            group_mask  = (df[group_col] == group) if has_group else pd.Series(True, index=df.index)
            group_label = group if has_group else "all"

            s = s_full[group_mask].dropna()
            if len(s) < 4:
                continue

            if method == "IQR":
                cfg = methods_cfg.get("IQR", {})
                q1  = s.quantile(cfg.get("lower_quantile", 0.25))
                q3  = s.quantile(cfg.get("upper_quantile", 0.75))
                mul = cfg.get("iqr_multiplier", 1.5)
                lo, hi = q1 - mul * (q3 - q1), q3 + mul * (q3 - q1)
                label  = "IQR"
            else:
                cfg        = methods_cfg.get("Z-Score", {})
                mean, std  = s.mean(), s.std()
                if std == 0:
                    continue
                mul        = cfg.get("std_multiplier", 3)
                lo, hi     = mean - mul * std, mean + mul * std
                label      = "Z-Score"

            outlier_mask = group_mask & s_full.notna() & ((s_full < lo) | (s_full > hi))

            for idx in df[outlier_mask].index:
                flagged_rows.append(idx)
                issues_list.append(
                    f"Outlier ({label}) within {group_label}: '{col}' = {df.at[idx, col]} "
                    f"outside [{lo:.2f}, {hi:.2f}]"
                )
                types_list.append("Statistical Outlier")

    if not flagged_rows:
        return pd.DataFrame()

    result = df.loc[flagged_rows].copy()
    result["__issues__"]     = issues_list
    result["__issue_type__"] = types_list
    return result.drop_duplicates(subset=list(df.columns))


@st.cache_data(show_spinner=False)
def check_dtype_batch(df, cols):
    cols = list(cols)
    rows, issues = [], []
    for col in cols:
        expected = df[col].dtype
        for idx, val in df[col].items():
            if pd.isnull(val): continue
            try:
                if pd.api.types.is_numeric_dtype(expected):
                    if not isinstance(val, (int, float, np.integer, np.floating)):
                        float(val)
                elif pd.api.types.is_datetime64_any_dtype(expected):
                    pd.to_datetime(val)
            except (ValueError, TypeError):
                rows.append(idx)
                issues.append(f"Type mismatch in '{col}': got '{type(val).__name__}'")
    if not rows: return pd.DataFrame()
    result = df.loc[rows].copy()
    result["__issues__"] = issues
    result["__issue_type__"] = "Data Type Issue"
    return result


def check_dtype(df, cols):
    batches = [cols[i:i+BATCH_SIZE] for i in range(0, len(cols), BATCH_SIZE)]
    return safe_concat([check_dtype_batch(df, tuple(b)) for b in batches], list(df.columns))


@st.cache_data(show_spinner=False)
def check_domain_rules(df, sheet_name):
    """Apply domain/business rules loaded from quality_rules.yaml."""

    flagged_rows = []
    issues_list = []
    types_list = []

    # DOMAIN_RULES is a list of dicts
    for rule in DOMAIN_RULES:

        col = rule.get("column", "")
        rule_type = rule.get("type", "")

        # Skip column existence check for prefix/group rules
        if rule_type not in ["conditional_any_notnull", "prefix_range"]:
            if col not in df.columns:
                continue

        # ---------------------------------------------------
        # RANGE CHECK
        # ---------------------------------------------------
        if rule_type == "range":

            lo = rule.get("min", -np.inf)
            hi = rule.get("max", np.inf)

            s = pd.to_numeric(df[col], errors="coerce")

            mask = s.notna() & ((s < lo) | (s > hi))

            for idx in df[mask].index:

                flagged_rows.append(idx)

                issues_list.append(
                    f"Domain: '{col}' = {df.at[idx, col]} — {rule['rule']}"
                )

                types_list.append("Domain Rule Violation")

        elif rule_type == "conditional_range_by_value":

            condition_col = rule.get("condition_col")
            ranges = rule.get("ranges", {})

            if condition_col and condition_col in df.columns:

                s = pd.to_numeric(df[col], errors="coerce")

                for country, limits in ranges.items():

                    lo = limits.get("min", -np.inf)
                    hi = limits.get("max", np.inf)

                    mask = (
                        (df[condition_col] == country)
                        & s.notna()
                        & ((s < lo) | (s > hi))
                    )

                    for idx in df[mask].index:

                        flagged_rows.append(idx)

                        issues_list.append(
                            f"Domain: '{col}' = {df.at[idx, col]} "
                            f"outside allowed range for {country} "
                            f"({lo} - {hi}) — {rule['rule']}"
                        )

                        types_list.append("Conditional Range Violation")

        # ---------------------------------------------------
        # BANNED VALUE CHECK
        # ---------------------------------------------------
        elif rule_type == "banned_value":

            banned = rule.get("banned", [])

            mask = df[col].isin(banned)

            for idx in df[mask].index:

                flagged_rows.append(idx)

                issues_list.append(
                    f"Domain: '{col}' = '{df.at[idx, col]}' — {rule['rule']}"
                )

                types_list.append("Banned Value")

        # ---------------------------------------------------
        # CONDITIONAL NOT NULL CHECK
        # ---------------------------------------------------
        elif rule_type == "conditional_notnull":

            trigger_col = rule.get("trigger_col")
            trigger_val = rule.get("trigger_val")

            if trigger_col and trigger_col in df.columns:

                mask = (
                    (df[trigger_col] == trigger_val)
                    & df[col].isnull()
                )

                for idx in df[mask].index:

                    flagged_rows.append(idx)

                    issues_list.append(
                        f"Domain: '{col}' must not be blank when "
                        f"'{trigger_col}' = '{trigger_val}' — {rule['rule']}"
                    )

                    types_list.append("Conditional Missing")

        # ---------------------------------------------------
        # CONDITIONAL GROUP NOT NULL CHECK
        # ---------------------------------------------------
        elif rule_type == "conditional_any_notnull":

            prefix = rule.get("column_prefix")
            trigger_col = rule.get("trigger_col")
            trigger_val = rule.get("trigger_val")

            if prefix and trigger_col and trigger_col in df.columns:

                matching_cols = [
                    c for c in df.columns
                    if c.startswith(prefix)
                ]

                if matching_cols:

                    mask = (
                        (df[trigger_col] == trigger_val)
                        & (
                            df[matching_cols]
                            .isnull()
                            .all(axis=1)
                        )
                    )

                    for idx in df[mask].index:

                        flagged_rows.append(idx)

                        issues_list.append(
                            f"At least one column starting with "
                            f"'{prefix}' must be filled when "
                            f"'{trigger_col}' = '{trigger_val}' — {rule['rule']}"
                        )

                        types_list.append("Conditional Missing Group")

        # ---------------------------------------------------
        # PREFIX RANGE CHECK
        # ---------------------------------------------------
        elif rule_type == "prefix_range":

            prefix = rule.get("column_prefix", "")
            lo = rule.get("min", -np.inf)
            hi = rule.get("max", np.inf)

            matching_cols = [c for c in df.columns if c.startswith(prefix)]

            for col in matching_cols:
                s = pd.to_numeric(df[col], errors="coerce")
                mask = s.notna() & ((s < lo) | (s > hi))

                for idx in df[mask].index:
                    flagged_rows.append(idx)
                    issues_list.append(
                        f"Domain: '{col}' = {df.at[idx, col]} — {rule['rule']}"
                    )
                    types_list.append("Domain Rule Violation")

    # ---------------------------------------------------
    # RETURN RESULTS
    # ---------------------------------------------------
    if not flagged_rows:
        return pd.DataFrame()

    result = df.loc[flagged_rows].copy()

    result["__issues__"] = issues_list
    result["__issue_type__"] = types_list

    return result.drop_duplicates(subset=list(df.columns))


def check_mandatory(df, sheet_name):
    """Check mandatory fields using sheet-aware rules from quality_rules.yaml."""
    # Find the first key in MANDATORY_MAP whose substring matches this sheet name
    mand_fields = []
    for key, fields in MANDATORY_MAP.items():
        if key.lower() in sheet_name.lower():
            mand_fields = fields
            break

    mand = [c for c in mand_fields if c in df.columns]
    if not mand:
        return pd.DataFrame()

    mask = df[mand].isnull().any(axis=1)
    result = df[mask].copy()
    result["__issues__"] = result.apply(
        lambda r: "Mandatory field missing: " + ", ".join(c for c in mand if pd.isnull(r[c])),
        axis=1,
    )
    result["__issue_type__"] = "Mandatory Field Missing"
    return result


# -------------- File load ---------------------------

@st.cache_data(show_spinner="📂 Loading file…")
def load_excel(file_bytes):
    return pd.read_excel(BytesIO(file_bytes), sheet_name=None)


# -------------- Excel export --------------

def build_excel_export(original, results_dict, selected_cols):
    wb = Workbook()
    wb.remove(wb.active)

    COLORS = {
        "header_bg": "175259", "header_fg": "FFFFFF",
        "missing":   "FDECEA", "duplicate": "E8F4FD",
        "outlier":   "FEF3E2", "dtype":     "F3E5F5",
        "domain":    "FFF3CD", "mandatory": "FCE4EC",
        "alt_row":   "F8F9FA",
    }
    thin = Side(style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    def style_header(ws):
        for cell in ws[1]:
            cell.font      = Font(bold=True, color=COLORS["header_fg"], name="Arial", size=10)
            cell.fill      = PatternFill("solid", start_color=COLORS["header_bg"])
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border    = border
        ws.row_dimensions[1].height = 30

    def write_sheet(ws, sheet_df, fill_color):
        if sheet_df.empty:
            ws.append(["No issues found."])
            return
        data_cols  = [c for c in sheet_df.columns if not c.startswith("__")]
        write_cols = data_cols + ["__issue_type__", "__issues__"]
        headers    = [c.replace("__issue_type__","Issue Type").replace("__issues__","Issue Detail") for c in write_cols]
        ws.append(headers)
        style_header(ws)
        fill = PatternFill("solid", start_color=fill_color)
        alt  = PatternFill("solid", start_color=COLORS["alt_row"])
        for i, (_, row) in enumerate(sheet_df[write_cols].iterrows(), start=2):
            ws.append([coerce_cell(v) for v in row])
            for cell in ws[i]:
                cell.fill      = fill if i % 2 == 0 else alt
                cell.border    = border
                cell.alignment = Alignment(vertical="center")
                cell.font      = Font(name="Arial", size=9)
        for ci, col in enumerate(write_cols, 1):
            hdr_len = len(str(headers[ci-1]))
            val_len = int(sheet_df[col].astype(str).str.len().fillna(0).max()) if col in sheet_df.columns else 0
            ws.column_dimensions[get_column_letter(ci)].width = min(max(hdr_len, val_len) + 4, 40)

    all_frames = list(results_dict.values())
    all_issues = safe_concat(all_frames, list(original.columns))

    # Summary
    ws_sum = wb.create_sheet("Summary")
    summary_data = [
        ["Cocoa Campaign Data Quality Report", ""],
        ["", ""],
        ["Metric", "Value"],
        ["Total Rows Checked", len(original)],
        ["Columns Checked", ", ".join(selected_cols)],
    ]
    for k, v in results_dict.items():
        summary_data.append([k, len(v)])
    summary_data.append(["Total Unique Issue Rows", len(all_issues)])
    thin2 = Side(style="thin", color="CCCCCC")
    b2 = Border(left=thin2, right=thin2, top=thin2, bottom=thin2)
    for r, row in enumerate(summary_data, 1):
        ws_sum.append(row)
        for cell in ws_sum[r]:
            cell.font      = Font(name="Arial", size=10, bold=(r in (1, 3)))
            cell.border    = b2
            cell.alignment = Alignment(horizontal="left", vertical="center")
        if r == 1:
            ws_sum[r][0].font = Font(name="Arial", size=14, bold=True, color=COLORS["header_bg"])
    ws_sum.column_dimensions["A"].width = 35
    ws_sum.column_dimensions["B"].width = 50

    color_map = {
        "Missing Values":          COLORS["missing"],
        "Mandatory Field Missing":  COLORS["mandatory"],
        "Duplicates":              COLORS["duplicate"],
        "Outliers":                COLORS["outlier"],
        "Data Type Issues":        COLORS["dtype"],
        "Domain Rule Violations":  COLORS["domain"],
        "All Issues":              COLORS["alt_row"],
    }
    for sheet_label, frame in results_dict.items():
        write_sheet(wb.create_sheet(sheet_label[:31]), frame, color_map.get(sheet_label, COLORS["alt_row"]))

    write_sheet(wb.create_sheet("All Issues"), all_issues, COLORS["alt_row"])

    ws_orig = wb.create_sheet("Original Data")
    ws_orig.append(list(original.columns))
    style_header(ws_orig)
    for i, row in enumerate(original.itertuples(index=False), 2):
        ws_orig.append([coerce_cell(v) for v in row])
        for cell in ws_orig[i]:
            cell.font = Font(name="Arial", size=9)
            cell.border = border
    for ci, col in enumerate(original.columns, 1):
        max_len = max(len(str(col)), int(original[col].astype(str).str.len().fillna(0).max()))
        ws_orig.column_dimensions[get_column_letter(ci)].width = min(max_len + 4, 40)

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------Charts---------------------

PLOTLY_LAYOUT = dict(
    font_family="Arial, sans-serif",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=40, b=40, l=40, r=40),
)

def chart_overview(counts, total):
    labels, values, colors = [], [], []
    color_seq = [CHART_COLORS["missing"], CHART_COLORS["dtype"],
                 CHART_COLORS["duplicate"], CHART_COLORS["outlier"],
                 CHART_COLORS["banned"], CHART_COLORS["cross"]]
    for i, (k, v) in enumerate(counts.items()):
        labels.append(k); values.append(v); colors.append(color_seq[i % len(color_seq)])
    max_v = max(values) if any(values) else 1
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h", marker_color=colors,
        text=[f"{v:,}  ({v/total*100:.1f}%)" if total else f"{v:,}" for v in values],
        textposition="outside", cliponaxis=False,
    ))
    fig.update_layout(title="Issue Count by Type", xaxis_title="Rows Affected",
                      xaxis=dict(gridcolor="#e8e8e8", range=[0, max_v*1.4]),
                      height=300, **PLOTLY_LAYOUT)
    return fig


def chart_health(total, issue_total):
    clean = max(0, total - issue_total)
    fig = go.Figure(go.Pie(
        labels=["Clean Rows", "Rows with Issues"], values=[clean, issue_total],
        hole=0.55, marker_colors=[CHART_COLORS["clean"], "#E74C3C"],
        textinfo="label+percent",
    ))
    pct = f"{clean/total*100:.0f}%<br>clean" if total else "—"
    fig.add_annotation(text=pct, x=0.5, y=0.5, showarrow=False,
                       font=dict(size=14, color=CHART_COLORS["primary"]))
    fig.update_layout(title="Overall Data Health", height=300,
                      showlegend=True, legend=dict(orientation="h", y=-0.15),
                      **PLOTLY_LAYOUT)
    return fig


def chart_completeness(df, selected_cols):
    null_pct = [(col, df[col].isnull().mean()*100) for col in selected_cols]
    null_pct = sorted(null_pct, key=lambda x: x[1], reverse=True)[:20]
    cols, pcts = zip(*null_pct) if null_pct else ([], [])
    fig = go.Figure(go.Bar(
        x=pcts, y=[c[:40] for c in cols], orientation="h",
        marker_color=[CHART_COLORS["missing"] if p > 10 else "#27AE60" for p in pcts],
        text=[f"{p:.1f}%" for p in pcts], textposition="outside",
    ))
    fig.update_layout(title="Top 20 Columns by Null %", xaxis_title="Null %",
                      xaxis=dict(range=[0, 110]), height=max(300, len(cols)*25+80),
                      **PLOTLY_LAYOUT)
    return fig


def show_table(tab, issue_df, label, all_cols, checked_cols):
    with tab:
        if issue_df.empty:
            st.success(f"✅ No {label} detected.")
        else:
            disp = slim_view(issue_df, all_cols, checked_cols)
            disp = disp.rename(columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"})
            st.dataframe(safe_str(disp), use_container_width=True, height=320)
            st.caption(f"{len(issue_df):,} row(s) flagged · Full data in export.")


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Settings")
    outlier_method = st.radio("Outlier Detection Method", ["IQR", "Z-Score"],
        help="IQR = 1.5×IQR rule. Z-Score = 3 std devs from mean.")
    st.markdown("---")
    st.markdown("**Checks to Run**")
    run_missing    = st.checkbox("Missing Values",                value=True)
    run_mandatory  = st.checkbox("Mandatory Fields (sheet-aware)", value=True)
    run_duplicate  = st.checkbox("Duplicates",                    value=True)
    run_outlier    = st.checkbox("Outliers",                      value=True)
    run_dtype      = st.checkbox("Data Type Issues",              value=True)
    run_domain     = st.checkbox("Domain / Business Rules",       value=True,
                                  help="Reads rules from quality_rules.yaml")

    if run_duplicate:
        st.markdown("**Duplicate Settings**")
        dup_keep_label = st.radio("Flag", ["All except first", "All except last", "Every occurrence"])
        keep_map = {"All except first": "first", "All except last": "last", "Every occurrence": "none"}
        dup_keep = keep_map[dup_keep_label]
    else:
        dup_keep = "first"

    with st.expander("📋 Active domain rules", expanded=False):
        st.caption(f"{len(DOMAIN_RULES)} rules loaded from `quality_rules.yaml`")
        for r in DOMAIN_RULES:
            st.markdown(f"- **{r.get('column','')}** `{r.get('type','')}` — {r.get('rule','')}")

    st.markdown("---")
    st.markdown("**About**")
    st.caption(
        "Rules are defined in `quality_rules.yaml` — edit that file to add or "
        "change validations without touching any Python code."
    )


# ── File Upload ────────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    "📂 Upload Campaign Excel File (.xlsx)",
    type=["xlsx", "xls"],
    help="Upload your Campaign Excel file (e.g. Campaign_Cocoa_2025_LATAM.xlsx)",
)

if uploaded is None:
    st.info("Upload your campaign Excel file to begin.")
    st.stop()

try:
    file_bytes = uploaded.read()
    all_sheets = load_excel(file_bytes)
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

sheet_names = [s for s, df in all_sheets.items() if not df.empty]
if not sheet_names:
    st.error("No non-empty sheets found.")
    st.stop()

# ── Sheet Selection ────────────────────────────────────────────────────────────
def on_sheet_change():
    prev = st.session_state.get("last_sheet")
    if prev and f"col_select_{prev}" in st.session_state:
        del st.session_state[f"col_select_{prev}"]

if len(sheet_names) > 1:
    sheet_name = st.selectbox("Select Sheet", sheet_names, key="sheet_sel", on_change=on_sheet_change)
else:
    sheet_name = sheet_names[0]

if st.session_state.get("last_sheet") != sheet_name:
    st.session_state["last_sheet"] = sheet_name
    k = f"col_select_{sheet_name}"
    if k in st.session_state:
        del st.session_state[k]

df = all_sheets[sheet_name]

st.markdown(
    f"<div class='section-header'>📋 Data Preview — <em>{sheet_name}</em> "
    f"({len(df):,} rows × {len(df.columns)} cols)</div>",
    unsafe_allow_html=True
)
st.dataframe(safe_str(df.head(50)), use_container_width=True, height=220)

# ── Column Selection ───────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Select Columns to Check</div>", unsafe_allow_html=True)

all_cols = df.columns.tolist()
widget_key = f"col_select_{sheet_name}"
if widget_key not in st.session_state:
    st.session_state[widget_key] = []

c1, c2, c3 = st.columns([3, 1, 1])
with c2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("✅ Select All", use_container_width=True):
        st.session_state[widget_key] = all_cols
        st.rerun()
with c3:
    st.markdown("<br>", unsafe_allow_html=True)
    # Suggest key columns using MANDATORY_MAP loaded from quality_rules.yaml
    key_suggestion = []
    for _key, _fields in MANDATORY_MAP.items():
        if _key.lower() in sheet_name.lower():
            key_suggestion = [c for c in _fields if c in all_cols]
            break
    if key_suggestion and st.button("🎯 Key Fields", use_container_width=True):
        st.session_state[widget_key] = key_suggestion
        st.rerun()

with c1:
    selected_cols = st.multiselect(
        "Choose columns to analyse",
        options=all_cols,
        default=st.session_state[widget_key],
        key=widget_key,
    )

if not selected_cols:
    st.info("Select at least one column above to run quality checks.")
    st.stop()

# ── Run Checks ─────────────────────────────────────────────────────────────────
with st.spinner("Running quality checks…"):
    results = {}

    if run_missing:
        results["Missing Values"] = check_missing(df, tuple(selected_cols))

    if run_mandatory:
        mand_df = check_mandatory(df, sheet_name)
        if not mand_df.empty:
            results["Mandatory Field Missing"] = mand_df

    if run_duplicate:
        results["Duplicates"] = check_duplicates(df, tuple(df.columns.tolist()), keep=dup_keep)

    if run_outlier:
        outlier_targets = [
            c for c in OUTLIER_COLS
            if c in df.columns
            and pd.api.types.is_numeric_dtype(df[c])
        ]
        if outlier_targets:
            results["Outliers"] = check_outliers_batch(
                df, outlier_targets, OUTLIER_GROUP, method=outlier_method
            )

    if run_dtype:
        results["Data Type Issues"] = check_dtype(df, selected_cols)

    if run_domain:
        domain_df = check_domain_rules(df, sheet_name)
        if not domain_df.empty:
            results["Domain Rule Violations"] = domain_df


# ── Metrics ────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>📊 Quality Summary</div>", unsafe_allow_html=True)

all_issue_df = safe_concat(list(results.values()), list(df.columns))
total_issues = len(all_issue_df)

cols_m = st.columns(len(results) + 1)
cols_m[0].metric("🗂️ Total Rows", f"{len(df):,}")
for i, (label, frame) in enumerate(results.items(), 1):
    icon = {"Missing Values": "❌", "Mandatory Field Missing": "⚠️", "Duplicates": "🔁",
            "Outliers": "📉", "Data Type Issues": "🔠", "Domain Rule Violations": "🚫"}.get(label, "🔍")
    cols_m[i].metric(f"{icon} {label}", f"{len(frame):,}")

# ── Charts ─────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>📈 Visual Analytics</div>", unsafe_allow_html=True)

issue_counts = {k: len(v) for k, v in results.items()}
c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(chart_health(len(df), total_issues), use_container_width=True, config={"displayModeBar": False})
with c2:
    st.plotly_chart(chart_overview(issue_counts, len(df)), use_container_width=True, config={"displayModeBar": False})

with st.expander("📊 Column Completeness (Null % by Column)", expanded=False):
    st.plotly_chart(chart_completeness(df, selected_cols), use_container_width=True, config={"displayModeBar": False})

# ── Column Stats ───────────────────────────────────────────────────────────────
with st.expander("📈 Column-Level Statistics", expanded=False):
    stats = []
    for col in selected_cols:
        null_n = int(df[col].isnull().sum())
        dup_n  = int(df.duplicated(subset=[col], keep=False).sum())
        stats.append({
            "Column": col, "Dtype": str(df[col].dtype),
            "Nulls": null_n, "Null %": f"{null_n/len(df)*100:.1f}%",
            "Duplicates": dup_n, "Unique": int(df[col].nunique()),
            "Sample": str(df[col].dropna().iloc[0]) if df[col].notna().any() else "—",
        })
    st.dataframe(pd.DataFrame(stats), use_container_width=True)

# ── Issue Tabs ─────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Issue Details</div>", unsafe_allow_html=True)

tab_labels = list(results.keys()) + ["📋 All Issues"]
tabs = st.tabs(tab_labels)

for tab, (label, frame) in zip(tabs[:-1], results.items()):
    show_table(tab, frame, label, all_cols, selected_cols)

with tabs[-1]:
    if all_issue_df.empty:
        st.success("✅ No issues detected — data looks clean!")
    else:
        disp = slim_view(all_issue_df, all_cols, selected_cols)
        disp = disp.rename(columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"})
        st.dataframe(safe_str(disp), use_container_width=True, height=380)
        st.caption(f"{total_issues:,} total unique issue row(s)")

# ── Export ─────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>⬇️ Export Results</div>", unsafe_allow_html=True)

if all_issue_df.empty:
    st.success("✅ No issues found — nothing to export!")
else:
    excel_bytes = build_excel_export(df, results, selected_cols)
    st.download_button(
        label="📥 Download Quality Report (.xlsx)",
        data=excel_bytes,
        file_name=f"quality_report_{sheet_name.replace(' ', '_')[:20]}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.caption("Report includes: Summary · " + " · ".join(results.keys()) + " · All Issues · Original Data")
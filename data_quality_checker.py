import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Data Quality Checker",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    .section-header {
        font-size: 1.2rem;
        font-weight: 700;
        color: #1e3a5f;
        border-bottom: 2px solid #2d6a9f;
        padding-bottom: 0.4rem;
        margin: 1.5rem 0 1rem 0;
    }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1 style="margin:0;font-size:2.2rem;">🔍 Data Quality Checker</h1>
    <p style="margin:0.5rem 0 0 0;font-size:1rem;opacity:0.9;">
        Upload an Excel file · Select columns · Detect issues · Export results
    </p>
</div>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_convert_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast mixed-type object columns to str."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            types = out[col].dropna().map(type).unique()
            if len(types) > 1:
                out[col] = out[col].astype(str)
    return out


def detect_missing(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    mask = df[cols].isnull().any(axis=1)
    result = df[mask].copy()
    result["__issues__"] = result.apply(
        lambda row: "Missing: " + ", ".join(c for c in cols if pd.isnull(row[c])),
        axis=1,
    )
    result["__issue_type__"] = "Missing Value"
    return result


def detect_duplicates(df: pd.DataFrame, cols: list, keep: str = "first") -> pd.DataFrame:
    keep_arg = False if keep == "none" else keep
    dup_mask = df.duplicated(subset=cols, keep=keep_arg)
    result   = df[dup_mask].copy()

    if result.empty:
        return result

    def find_original(row_idx):
        row_vals = df.loc[row_idx, cols]
        matches  = df[(df[cols] == row_vals).all(axis=1)].index.tolist()
        others   = [i + 2 for i in matches if i != row_idx]
        return f"Duplicate of row(s): {others}"

    result["__issues__"]     = [find_original(i) for i in result.index]
    result["__issue_type__"] = "Duplicate"
    return result


def detect_outliers(df: pd.DataFrame, cols: list, method: str = "IQR") -> pd.DataFrame:
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    if not num_cols:
        return pd.DataFrame()

    outlier_mask = pd.Series(False, index=df.index)
    issue_texts  = pd.Series("", index=df.index)

    for col in num_cols:
        series = df[col].dropna()
        if len(series) < 4:
            continue
        if method == "IQR":
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            iqr    = q3 - q1
            lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        else:
            mean, std = series.mean(), series.std()
            if std == 0:
                continue
            lo, hi = mean - 3 * std, mean + 3 * std

        col_mask = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        outlier_mask |= col_mask
        issue_texts[col_mask] += f"Outlier in '{col}'; "

    result = df[outlier_mask].copy()
    result["__issues__"]     = issue_texts[outlier_mask].str.rstrip("; ")
    result["__issue_type__"] = "Outlier"
    return result


def detect_dtype_issues(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    rows, issues = [], []
    for col in cols:
        expected = df[col].dtype
        for idx, val in df[col].items():
            if pd.isnull(val):
                continue
            try:
                if pd.api.types.is_numeric_dtype(expected):
                    if not isinstance(val, (int, float, np.integer, np.floating)):
                        float(val)
                elif pd.api.types.is_datetime64_any_dtype(expected):
                    pd.to_datetime(val)
            except (ValueError, TypeError):
                rows.append(idx)
                issues.append(f"Type mismatch in '{col}': got '{type(val).__name__}'")

    if not rows:
        return pd.DataFrame()
    result = df.loc[rows].copy()
    result["__issues__"]     = issues
    result["__issue_type__"] = "Data Type Issue"
    return result


def safe_concat_dedup(frames: list, base_cols: list) -> pd.DataFrame:
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    subset   = [c for c in base_cols if c in combined.columns]
    if subset:
        combined = combined.drop_duplicates(subset=subset)
    return combined


def coerce_cell(v):
    if isinstance(v, (np.integer,)):          return int(v)
    if isinstance(v, (np.floating,)):         return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_,)):            return bool(v)
    if isinstance(v, float) and np.isnan(v): return None
    if not isinstance(v, (int, float, str, bool, type(None))): return str(v)
    return v


def build_excel_export(
    original: pd.DataFrame,
    missing_df: pd.DataFrame,
    duplicate_df: pd.DataFrame,
    outlier_df: pd.DataFrame,
    dtype_df: pd.DataFrame,
    selected_cols: list,
) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)

    COLORS = {
        "header_bg":  "1e3a5f",
        "header_fg":  "FFFFFF",
        "missing":    "FDECEA",
        "duplicate":  "E8F4FD",
        "outlier":    "FEF3E2",
        "dtype":      "F3E5F5",
        "summary_bg": "EBF5FB",
        "alt_row":    "F8F9FA",
    }
    thin   = Side(style="thin", color="CCCCCC")
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
        headers    = [
            c.replace("__issue_type__", "Issue Type").replace("__issues__", "Issue Detail")
            for c in write_cols
        ]
        ws.append(headers)
        style_header(ws)
        fill = PatternFill("solid", start_color=fill_color)
        alt  = PatternFill("solid", start_color=COLORS["alt_row"])
        for i, (_, row) in enumerate(sheet_df[write_cols].iterrows(), start=2):
            ws.append([coerce_cell(v) for v in row])
            row_fill = fill if i % 2 == 0 else alt
            for cell in ws[i]:
                cell.fill      = row_fill
                cell.border    = border
                cell.alignment = Alignment(vertical="center")
                cell.font      = Font(name="Arial", size=9)
        for col_idx, col in enumerate(write_cols, start=1):
            hdr_len = len(str(headers[col_idx - 1]))
            val_len = int(sheet_df[col].astype(str).str.len().fillna(0).max()) if col in sheet_df.columns else 0
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(hdr_len, val_len) + 4, 40)

    ws_sum = wb.create_sheet("Summary")
    combined_count = len(
        safe_concat_dedup([missing_df, duplicate_df, outlier_df, dtype_df], list(original.columns))
    )
    summary_data = [
        ["Data Quality Report", ""],
        ["", ""],
        ["Metric",                  "Value"],
        ["Total Rows Checked",      len(original)],
        ["Columns Checked",         ", ".join(selected_cols)],
        ["Missing Value Rows",      len(missing_df)],
        ["Duplicate Rows",          len(duplicate_df)],
        ["Outlier Rows",            len(outlier_df)],
        ["Data Type Issue Rows",    len(dtype_df)],
        ["Total Unique Issue Rows", combined_count],
    ]
    for r, row in enumerate(summary_data, start=1):
        ws_sum.append(row)
        for cell in ws_sum[r]:
            cell.font      = Font(name="Arial", size=10, bold=(r in (1, 3)))
            cell.border    = border
            cell.alignment = Alignment(horizontal="left", vertical="center")
        if r == 1:
            ws_sum[r][0].font = Font(name="Arial", size=14, bold=True, color=COLORS["header_bg"])
        if r == 3:
            for cell in ws_sum[r]:
                cell.fill = PatternFill("solid", start_color=COLORS["summary_bg"])
    ws_sum.column_dimensions["A"].width = 28
    ws_sum.column_dimensions["B"].width = 50

    write_sheet(wb.create_sheet("Missing Values"),   missing_df,   COLORS["missing"])
    write_sheet(wb.create_sheet("Duplicates"),       duplicate_df, COLORS["duplicate"])
    write_sheet(wb.create_sheet("Outliers"),         outlier_df,   COLORS["outlier"])
    write_sheet(wb.create_sheet("Data Type Issues"), dtype_df,     COLORS["dtype"])
    write_sheet(
        wb.create_sheet("All Issues"),
        safe_concat_dedup([missing_df, duplicate_df, outlier_df, dtype_df], list(original.columns)),
        COLORS["alt_row"],
    )

    ws_orig = wb.create_sheet("Original Data")
    ws_orig.append(list(original.columns))
    style_header(ws_orig)
    for i, row in enumerate(original.itertuples(index=False), start=2):
        ws_orig.append([coerce_cell(v) for v in row])
        for cell in ws_orig[i]:
            cell.font      = Font(name="Arial", size=9)
            cell.border    = border
            cell.alignment = Alignment(vertical="center")
    for col_idx, col in enumerate(original.columns, start=1):
        max_len = max(len(str(col)), int(original[col].astype(str).str.len().fillna(0).max()))
        ws_orig.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 40)

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def show_issue_table(tab, issue_df, label):
    with tab:
        if issue_df.empty:
            st.success(f"No {label} detected in the selected columns.")
        else:
            disp = issue_df.drop(
                columns=[c for c in issue_df.columns
                         if c.startswith("__") and c not in ("__issue_type__", "__issues__")]
            )
            disp = disp.rename(columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"})
            st.dataframe(safe_convert_df(disp), use_container_width=True, height=300)
            st.caption(f"{len(issue_df):,} row(s) flagged")


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Settings")

    outlier_method = st.radio(
        "Outlier Detection Method",
        ["IQR", "Z-Score"],
        help="IQR flags values beyond 1.5×IQR from Q1/Q3. Z-Score flags values > 3 std devs from mean.",
    )

    st.markdown("---")
    st.markdown("**Issue Types**")
    check_missing   = st.checkbox("Missing Values",   value=True)
    check_duplicate = st.checkbox("Duplicates",       value=True)
    check_outlier   = st.checkbox("Outliers",         value=True)
    check_dtype     = st.checkbox("Data Type Issues", value=True)

    if check_duplicate:
        st.markdown("**Duplicate Settings**")
        dup_scope = st.radio(
            "Check duplicates across",
            ["Selected columns only", "All columns"],
            help="'Selected columns only' flags rows where the chosen columns match. "
                 "'All columns' requires every column to match.",
        )
        dup_keep = st.radio(
            "Which occurrences to flag",
            ["Flag all except first", "Flag all except last", "Flag every occurrence"],
            help="'Flag all except first' keeps the earliest record and flags the rest.",
        )
        keep_map = {
            "Flag all except first":  "first",
            "Flag all except last":   "last",
            "Flag every occurrence":  "none",
        }
        dup_keep_arg = keep_map[dup_keep]

    st.markdown("---")
    st.markdown("**About**")
    st.markdown(
        "Upload any Excel file, pick the columns to audit, "
        "and download a formatted quality report."
    )


# ── File Upload ───────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    "📂 Upload Excel File (.xlsx / .xls)",
    type=["xlsx", "xls"],
    help="The file is processed locally — no data is stored.",
)

if uploaded is None:
    st.info("Upload an Excel file to get started.")
    st.stop()

# ── Load Data ─────────────────────────────────────────────────────────────────
try:
    all_sheets = pd.read_excel(uploaded, sheet_name=None)
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

sheet_names = list(all_sheets.keys())

# ── Sheet Selection ───────────────────────────────────────────────────────────
# KEY FIX: use on_change to detect sheet switches and reset column state
def on_sheet_change():
    """Reset column selection and Select All trigger when sheet changes."""
    st.session_state["select_all_triggered"]  = False
    st.session_state["selected_cols_default"] = []

if len(sheet_names) > 1:
    sheet_name = st.selectbox(
        "Select Sheet",
        sheet_names,
        key="sheet_selector",
        on_change=on_sheet_change,
    )
else:
    sheet_name = sheet_names[0]

# Track last loaded sheet to detect changes (fallback safety)
if st.session_state.get("last_sheet") != sheet_name:
    st.session_state["last_sheet"]            = sheet_name
    st.session_state["select_all_triggered"]  = False
    st.session_state["selected_cols_default"] = []

df = all_sheets[sheet_name]

st.markdown(
    f"<div class='section-header'>📋 Data Preview — <em>{sheet_name}</em> "
    f"({len(df):,} rows × {len(df.columns)} cols)</div>",
    unsafe_allow_html=True,
)
st.dataframe(safe_convert_df(df.head(50)), use_container_width=True, height=240)


# ── Column Selection ──────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Select Columns to Check</div>", unsafe_allow_html=True)

# Initialise session state keys safely
for key, default in [
    ("select_all_triggered",  False),
    ("selected_cols_default", []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

col1, col2 = st.columns([3, 1])
with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("✅ Select All"):
        st.session_state["select_all_triggered"]  = True
        st.session_state["selected_cols_default"] = df.columns.tolist()

with col1:
    # Use the stored default so switching sheets starts fresh
    default_cols = (
        df.columns.tolist()
        if st.session_state["select_all_triggered"]
        else st.session_state.get("selected_cols_default", [])
    )

    # Only keep defaults that exist in the CURRENT sheet's columns
    default_cols = [c for c in default_cols if c in df.columns]

    selected_cols = st.multiselect(
        "Choose one or more columns",
        options=df.columns.tolist(),
        default=default_cols,
        key=f"col_select_{sheet_name}",   # ← unique key per sheet forces fresh render
        help="Only selected columns will be analysed for issues.",
    )

# Reset Select All trigger once applied
if st.session_state["select_all_triggered"] and selected_cols:
    st.session_state["select_all_triggered"]  = False
    st.session_state["selected_cols_default"] = selected_cols

if not selected_cols:
    st.info("Please select at least one column above to run quality checks.")
    st.stop()


# ── Run Checks ────────────────────────────────────────────────────────────────
with st.spinner("Running quality checks…"):
    missing_df = detect_missing(df, selected_cols) if check_missing else pd.DataFrame()

    if check_duplicate:
        dup_cols     = selected_cols if dup_scope == "Selected columns only" else df.columns.tolist()
        duplicate_df = detect_duplicates(df, dup_cols, keep=dup_keep_arg)
    else:
        duplicate_df = pd.DataFrame()

    outlier_df = detect_outliers(df, selected_cols, outlier_method) if check_outlier else pd.DataFrame()
    dtype_df   = detect_dtype_issues(df, selected_cols)             if check_dtype   else pd.DataFrame()


# ── Summary Metrics ───────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>📊 Quality Summary</div>", unsafe_allow_html=True)
m1, m2, m3, m4, m5 = st.columns(5)

total_issues = len(
    safe_concat_dedup([missing_df, duplicate_df, outlier_df, dtype_df], list(df.columns))
)

m1.metric("🗂️ Total Rows",        f"{len(df):,}")
m2.metric("❌ Missing Value Rows", f"{len(missing_df):,}")
m3.metric("🔁 Duplicate Rows",     f"{len(duplicate_df):,}")
m4.metric("⚠️ Outlier Rows",       f"{len(outlier_df):,}")
m5.metric("🔠 Type Issue Rows",    f"{len(dtype_df):,}")


# ── Per-Column Stats ──────────────────────────────────────────────────────────
with st.expander("📈 Column-Level Statistics", expanded=False):
    stats_rows = []
    for col in selected_cols:
        null_count = int(df[col].isnull().sum())
        dup_count  = int(df.duplicated(subset=[col], keep=False).sum())
        stats_rows.append({
            "Column":        col,
            "Dtype":         str(df[col].dtype),
            "Nulls":         null_count,
            "Null %":        f"{null_count / len(df) * 100:.1f}%",
            "Duplicates":    dup_count,
            "Dup %":         f"{dup_count / len(df) * 100:.1f}%",
            "Unique Values": int(df[col].nunique()),
            "Sample":        str(df[col].dropna().iloc[0]) if df[col].notna().any() else "—",
        })
    st.dataframe(pd.DataFrame(stats_rows), use_container_width=True)


# ── Issue Tabs ────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Issue Details</div>", unsafe_allow_html=True)
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "❌ Missing Values",
    "🔁 Duplicates",
    "⚠️ Outliers",
    "🔠 Type Issues",
    "📋 All Issues",
])

show_issue_table(tab1, missing_df,   "missing values")
show_issue_table(tab2, duplicate_df, "duplicates")
show_issue_table(tab3, outlier_df,   "outliers")
show_issue_table(tab4, dtype_df,     "data type issues")

with tab5:
    if missing_df.empty and duplicate_df.empty and outlier_df.empty and dtype_df.empty:
        st.success("🎉 No issues detected — your data looks clean!")
    else:
        combined_all = safe_concat_dedup(
            [missing_df, duplicate_df, outlier_df, dtype_df], list(df.columns)
        )
        disp = combined_all.drop(
            columns=[c for c in combined_all.columns
                     if c.startswith("__") and c not in ("__issue_type__", "__issues__")]
        )
        disp = disp.rename(columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"})
        st.dataframe(safe_convert_df(disp), use_container_width=True, height=350)
        st.caption(f"{len(combined_all):,} total unique issue row(s)")


# ── Export ────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>⬇️ Export Results</div>", unsafe_allow_html=True)

if missing_df.empty and duplicate_df.empty and outlier_df.empty and dtype_df.empty:
    st.success("✅ No issues found — nothing to export!")
else:
    excel_bytes = build_excel_export(
        df, missing_df, duplicate_df, outlier_df, dtype_df, selected_cols
    )
    st.download_button(
        label="📥 Download Quality Report (.xlsx)",
        data=excel_bytes,
        file_name=f"data_quality_report_{sheet_name}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.caption(
        "Report includes: Summary · Missing Values · Duplicates · "
        "Outliers · Type Issues · All Issues · Original Data"
    )

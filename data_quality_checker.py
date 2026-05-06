import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# Page config
st.set_page_config(
    page_title="Data Quality Checker",
    layout="wide",
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #175259 100%, #2d6a9f 0%);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    .section-header {
        font-size: 1.2rem;
        font-weight: 700;
        color: #175259;
        border-bottom: 2px solid #2d6a9f;
        padding-bottom: 0.4rem;
        margin: 1.5rem 0 1rem 0;
    }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1 style="margin:0;font-size:2.2rem;">🔍 Data Quality Checker</h1>
    <p style="margin:0.5rem 0 0 0;font-size:1rem;opacity:0.9;">
        Upload an Excel file · Select columns · Detect issues · Export results
    </p>
</div>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
BATCH_SIZE = 50  # columns per batch for large files


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_convert_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert mixed-type object columns to string for safe display."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            types = out[col].dropna().map(type).unique()
            if len(types) > 1:
                out[col] = out[col].astype(str)
    return out


# ── Slim-view helper ──────────────────────────────────────────────────────────

def slim_view(
    issue_df: pd.DataFrame,
    all_cols: list,
    checked_cols: list,
    n_anchor: int = 10,
) -> pd.DataFrame:
    """
    Reduce an issue DataFrame to a readable subset of columns.

    Keeps:
      - First `n_anchor` columns of the original dataset (row identity anchors)
      - Any checked column that appears in the __issues__ text for that row
      - __issue_type__ and __issues__ metadata columns

    This avoids displaying all 700+ columns when only a handful are relevant.

    Args:
        issue_df:     DataFrame returned by a detect_* function.
        all_cols:     Ordered list of ALL columns in the original dataset.
        checked_cols: Columns the user selected for quality checks.
        n_anchor:     How many leading columns to always include (default 10).
    """
    if issue_df.empty:
        return issue_df

    anchor_cols = list(all_cols[:n_anchor])

    # For each row find which checked columns are actually mentioned in __issues__
    def relevant_cols_for_row(issue_text: str) -> list:
        return [c for c in checked_cols if c in str(issue_text)]

    issue_col_sets = issue_df["__issues__"].apply(relevant_cols_for_row)
    all_issue_cols = list(dict.fromkeys(c for cols in issue_col_sets for c in cols))

    # Build final ordered column list — anchor → issue cols → metadata
    meta_cols   = ["__issue_type__", "__issues__"]
    keep        = anchor_cols.copy()
    for c in all_issue_cols:
        if c not in keep:
            keep.append(c)
    for c in meta_cols:
        if c in issue_df.columns and c not in keep:
            keep.append(c)

    # Only keep columns that actually exist in the DataFrame
    keep = [c for c in keep if c in issue_df.columns]
    return issue_df[keep]


# ── Cached file loader ─────────────────────────────────────────────────────────

@st.cache_data(show_spinner="📂 Loading file…")
def load_excel(file_bytes: bytes) -> dict[str, pd.DataFrame]:
    """
    Load all sheets from an Excel file.

    Cached by file content so re-runs don't re-parse the file.
    Returns a dict of {sheet_name: DataFrame}.
    """
    return pd.read_excel(BytesIO(file_bytes), sheet_name=None)


# ── Cached per-sheet detection functions ──────────────────────────────────────

@st.cache_data(show_spinner=False)
def detect_missing(df: pd.DataFrame, cols: tuple) -> pd.DataFrame:
    """
    Detect rows with missing values in specified columns.

    Args:
        df:   Input DataFrame.
        cols: Tuple of column names to check (tuple for hashability).
    """
    cols = list(cols)
    mask = df[cols].isnull().any(axis=1)
    result = df[mask].copy()
    result["__issues__"] = result.apply(
        lambda row: "Missing: " + ", ".join(c for c in cols if pd.isnull(row[c])),
        axis=1,
    )
    result["__issue_type__"] = "Missing Value"
    return result


@st.cache_data(show_spinner=False)
def detect_duplicates(df: pd.DataFrame, cols: tuple, keep: str = "first") -> pd.DataFrame:
    """
    Detect duplicate rows based on specified columns.

    Args:
        df:   Input DataFrame.
        cols: Tuple of column names (tuple for hashability).
        keep: 'first', 'last', or 'none'.
    """
    cols = list(cols)
    keep_arg = False if keep == "none" else keep
    dup_mask = df.duplicated(subset=cols, keep=keep_arg)
    result = df[dup_mask].copy()

    if result.empty:
        return result

    def find_original(row_idx):
        row_vals = df.loc[row_idx, cols]
        matches = df[(df[cols] == row_vals).all(axis=1)].index.tolist()
        others = [i + 2 for i in matches if i != row_idx]
        return f"Duplicate of row(s): {others}"

    result["__issues__"] = [find_original(i) for i in result.index]
    result["__issue_type__"] = "Duplicate"
    return result


@st.cache_data(show_spinner=False)
def _detect_outliers_batch(
    df: pd.DataFrame, cols: tuple, method: str = "IQR"
) -> pd.DataFrame:
    """
    Detect outliers in a batch of numeric columns.

    Internal batched helper — call detect_outliers_batched() instead.
    """
    cols = list(cols)
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    if not num_cols:
        return pd.DataFrame()

    outlier_mask = pd.Series(False, index=df.index)
    issue_texts = pd.Series("", index=df.index)

    for col in num_cols:
        series = df[col].dropna()
        if len(series) < 4:
            continue
        if method == "IQR":
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            iqr = q3 - q1
            low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        else:
            mean, std = series.mean(), series.std()
            if std == 0:
                continue
            low, high = mean - 3 * std, mean + 3 * std

        col_mask = df[col].notna() & ((df[col] < low) | (df[col] > high))
        outlier_mask |= col_mask
        issue_texts[col_mask] += f"Outlier in '{col}'; "

    result = df[outlier_mask].copy()
    result["__issues__"] = issue_texts[outlier_mask].str.rstrip("; ")
    result["__issue_type__"] = "Outlier"
    return result


def detect_outliers_batched(
    df: pd.DataFrame, cols: list, method: str = "IQR", batch_size: int = BATCH_SIZE
) -> pd.DataFrame:
    """
    Detect outliers across all columns using batched processing.

    Splits large column lists into chunks of `batch_size`, runs cached
    detection on each chunk, then merges results. This avoids re-running
    the full detection when only some columns change.

    Args:
        df:         Input DataFrame.
        cols:       List of column names to check.
        method:     'IQR' or 'Z-Score'.
        batch_size: Number of columns per batch.
    """
    batches = [cols[i: i + batch_size] for i in range(0, len(cols), batch_size)]
    frames = []
    for batch in batches:
        frames.append(_detect_outliers_batch(df, tuple(batch), method))
    return safe_concat_dedup(frames, list(df.columns))


@st.cache_data(show_spinner=False)
def _detect_dtype_issues_batch(df: pd.DataFrame, cols: tuple) -> pd.DataFrame:
    """
    Detect data type inconsistencies in a batch of columns.

    Internal batched helper — call detect_dtype_issues_batched() instead.
    """
    cols = list(cols)
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
    result["__issues__"] = issues
    result["__issue_type__"] = "Data Type Issue"
    return result


def detect_dtype_issues_batched(
    df: pd.DataFrame, cols: list, batch_size: int = BATCH_SIZE
) -> pd.DataFrame:
    """
    Detect data type issues using batched column processing.

    Args:
        df:         Input DataFrame.
        cols:       List of column names to check.
        batch_size: Number of columns per batch.
    """
    batches = [cols[i: i + batch_size] for i in range(0, len(cols), batch_size)]
    frames = []
    for batch in batches:
        frames.append(_detect_dtype_issues_batch(df, tuple(batch)))
    return safe_concat_dedup(frames, list(df.columns))


# ── Utilities ─────────────────────────────────────────────────────────────────

def safe_concat_dedup(frames: list, base_cols: list) -> pd.DataFrame:
    """Concatenate DataFrames and remove duplicate rows."""
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    combined = pd.concat(non_empty, ignore_index=True)
    subset = [c for c in base_cols if c in combined.columns]
    if subset:
        combined = combined.drop_duplicates(subset=subset)
    return combined


def coerce_cell(v):
    """Convert a cell value to a JSON-serializable type for Excel export."""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, float) and np.isnan(v):
        return None
    if not isinstance(v, (int, float, str, bool, type(None))):
        return str(v)
    return v


# ── Excel Export ──────────────────────────────────────────────────────────────

def build_excel_export(
    original: pd.DataFrame,
    missing_df: pd.DataFrame,
    duplicate_df: pd.DataFrame,
    outlier_df: pd.DataFrame,
    dtype_df: pd.DataFrame,
    selected_cols: list,
) -> bytes:
    """Generate a formatted multi-sheet Excel quality report."""
    wb = Workbook()
    wb.remove(wb.active)

    COLORS = {
        "header_bg": "1e3a5f",
        "header_fg": "FFFFFF",
        "missing":   "FDECEA",
        "duplicate": "E8F4FD",
        "outlier":   "FEF3E2",
        "dtype":     "F3E5F5",
        "summary_bg":"EBF5FB",
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
            val_len = (
                int(sheet_df[col].astype(str).str.len().fillna(0).max())
                if col in sheet_df.columns else 0
            )
            ws.column_dimensions[get_column_letter(col_idx)].width = min(
                max(hdr_len, val_len) + 4, 40
            )

    # Summary sheet
    ws_sum = wb.create_sheet("Summary")
    combined_count = len(
        safe_concat_dedup(
            [missing_df, duplicate_df, outlier_df, dtype_df], list(original.columns)
        )
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
            ws_sum[r][0].font = Font(
                name="Arial", size=14, bold=True, color=COLORS["header_bg"]
            )
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
        safe_concat_dedup(
            [missing_df, duplicate_df, outlier_df, dtype_df], list(original.columns)
        ),
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
        max_len = max(
            len(str(col)),
            int(original[col].astype(str).str.len().fillna(0).max()),
        )
        ws_orig.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 40)

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Issue table display ────────────────────────────────────────────────────────

def show_issue_table(tab, issue_df, label, all_cols, checked_cols):
    """
    Display an issue DataFrame in a Streamlit tab.

    Shows only: first 10 original columns + flagged column(s) + issue metadata.
    Full data is preserved in the export.
    """
    with tab:
        if issue_df.empty:
            st.success(f"No {label} detected in the selected columns.")
        else:
            disp = slim_view(issue_df, all_cols, checked_cols)
            disp = disp.rename(
                columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"}
            )
            st.dataframe(safe_convert_df(disp), use_container_width=True, height=300)
            st.caption(
                f"{len(issue_df):,} row(s) flagged · "
                f"Showing first 10 columns + flagged column(s). "
                f"Full data available in the export."
            )


# ── Sidebar ────────────────────────────────────────────────────────────────────
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
            "Flag all except first": "first",
            "Flag all except last":  "last",
            "Flag every occurrence": "none",
        }
        dup_keep_arg = keep_map[dup_keep]
    else:
        dup_scope    = "Selected columns only"
        dup_keep_arg = "first"

    st.markdown("---")
    st.markdown("**Performance**")
    batch_size = st.slider(
        "Batch size (columns)",
        min_value=10,
        max_value=200,
        value=BATCH_SIZE,
        step=10,
        help="Number of columns processed per batch. Larger = fewer cache entries but "
             "more work per batch. 50 is a good default for wide files.",
    )

    st.markdown("---")
    st.markdown("**About**")
    st.markdown(
        "Upload any Excel file, pick the columns to audit, "
        "and download a formatted quality report."
    )


# ── File Upload ────────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    "📂 Upload Excel File (.xlsx / .xls)",
    type=["xlsx", "xls"],
    help="The file is processed locally — no data is stored.",
)

if uploaded is None:
    st.info("Upload an Excel file to get started.")
    st.stop()

# ── Load data (cached by file bytes) ──────────────────────────────────────────
try:
    file_bytes = uploaded.read()
    all_sheets = load_excel(file_bytes)
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

sheet_names = list(all_sheets.keys())


# ── Sheet Selection ────────────────────────────────────────────────────────────

def on_sheet_change():
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


# ── Column Selection ───────────────────────────────────────────────────────────
st.markdown(
    "<div class='section-header'>Select Columns to Check</div>",
    unsafe_allow_html=True,
)

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
    default_cols = (
        df.columns.tolist()
        if st.session_state["select_all_triggered"]
        else st.session_state.get("selected_cols_default", [])
    )
    default_cols = [c for c in default_cols if c in df.columns]

    selected_cols = st.multiselect(
        "Choose one or more columns",
        options=df.columns.tolist(),
        default=default_cols,
        key=f"col_select_{sheet_name}",
        help="Only selected columns will be analysed for issues.",
    )

if st.session_state["select_all_triggered"] and selected_cols:
    st.session_state["select_all_triggered"]  = False
    st.session_state["selected_cols_default"] = selected_cols

if not selected_cols:
    st.info("Please select at least one column above to run quality checks.")
    st.stop()


# ── Run Checks ─────────────────────────────────────────────────────────────────
#
# All detection functions are either:
#   @st.cache_data  — results are memoised; identical inputs skip recomputation
#   batched         — wide files are split into chunks so cache entries stay small
#                     and partial column changes don't invalidate the whole result
#
n_cols   = len(selected_cols)
n_batches = max(1, (n_cols + batch_size - 1) // batch_size)
spinner_msg = (
    f"Running quality checks on {n_cols} columns "
    f"({n_batches} batch{'es' if n_batches > 1 else ''})…"
)

with st.spinner(spinner_msg):
    # Missing — single cached call (operates row-wise, no benefit to batching)
    missing_df = (
        detect_missing(df, tuple(selected_cols))
        if check_missing
        else pd.DataFrame()
    )

    # Duplicates — single cached call
    if check_duplicate:
        dup_cols     = selected_cols if dup_scope == "Selected columns only" else df.columns.tolist()
        duplicate_df = detect_duplicates(df, tuple(dup_cols), keep=dup_keep_arg)
    else:
        duplicate_df = pd.DataFrame()

    # Outliers — batched + cached per batch
    outlier_df = (
        detect_outliers_batched(df, selected_cols, outlier_method, batch_size)
        if check_outlier
        else pd.DataFrame()
    )

    # Data type issues — batched + cached per batch
    dtype_df = (
        detect_dtype_issues_batched(df, selected_cols, batch_size)
        if check_dtype
        else pd.DataFrame()
    )


# ── Summary Metrics ────────────────────────────────────────────────────────────
st.markdown(
    "<div class='section-header'>📊 Quality Summary</div>",
    unsafe_allow_html=True,
)
m1, m2, m3, m4, m5 = st.columns(5)

total_issues = len(
    safe_concat_dedup(
        [missing_df, duplicate_df, outlier_df, dtype_df], list(df.columns)
    )
)

m1.metric("🗂️ Total Rows",        f"{len(df):,}")
m2.metric("❌ Missing Value Rows", f"{len(missing_df):,}")
m3.metric("🔁 Duplicate Rows",     f"{len(duplicate_df):,}")
m4.metric("⚠️ Outlier Rows",       f"{len(outlier_df):,}")
m5.metric("🔠 Type Issue Rows",    f"{len(dtype_df):,}")


# ── Per-Column Stats ───────────────────────────────────────────────────────────
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


# ── Issue Tabs ─────────────────────────────────────────────────────────────────
st.markdown(
    "<div class='section-header'>Issue Details</div>",
    unsafe_allow_html=True,
)
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "❌ Missing Values",
    "🔁 Duplicates",
    "⚠️ Outliers",
    "🔠 Type Issues",
    "📋 All Issues",
])

show_issue_table(tab1, missing_df,   "missing values",      df.columns.tolist(), selected_cols)
show_issue_table(tab2, duplicate_df, "duplicates",          df.columns.tolist(), selected_cols)
show_issue_table(tab3, outlier_df,   "outliers",            df.columns.tolist(), selected_cols)
show_issue_table(tab4, dtype_df,     "data type issues",    df.columns.tolist(), selected_cols)

with tab5:
    if missing_df.empty and duplicate_df.empty and outlier_df.empty and dtype_df.empty:
        st.success("🎉 No issues detected — your data looks clean!")
    else:
        combined_all = safe_concat_dedup(
            [missing_df, duplicate_df, outlier_df, dtype_df], list(df.columns)
        )
        disp = slim_view(combined_all, df.columns.tolist(), selected_cols)
        disp = disp.rename(
            columns={"__issue_type__": "Issue Type", "__issues__": "Issue Detail"}
        )
        st.dataframe(safe_convert_df(disp), use_container_width=True, height=350)
        st.caption(
            f"{len(combined_all):,} total unique issue row(s) · "
            f"Showing first 10 columns + flagged column(s). "
            f"Full data available in the export."
        )


# ── Export ─────────────────────────────────────────────────────────────────────
st.markdown(
    "<div class='section-header'>⬇️ Export Results</div>",
    unsafe_allow_html=True,
)

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
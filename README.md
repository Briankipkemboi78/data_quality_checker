# 🔍 Data Quality Checker — Local Setup Guide

A Streamlit app that audits Excel files for missing values, outliers, and data type issues, then exports a formatted Excel report.

---

## ✅ Prerequisites

- Python 3.9 or higher
- pip

---

## 🚀 Quick Start

### 1. Create a virtual environment (recommended)

```bash
python -m venv venv

# Activate on macOS/Linux:
source venv/bin/activate

# Activate on Windows:
venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the app

```bash
streamlit run data_quality_checker.py
```

The app will open automatically at **http://localhost:8501**

---

## 🧭 How to Use

1. **Upload** an `.xlsx` or `.xls` file using the file uploader
2. **Select a sheet** (if your file has multiple sheets)
3. **Choose columns** to audit via the multi-select dropdown
4. **Configure settings** in the left sidebar:
   - Toggle which issue types to check (missing, outliers, type issues)
   - Choose outlier detection method (IQR or Z-Score)
5. **Review results** in the tabbed issue viewer and column stats expander
6. **Download** the full Excel quality report with one click

---

## 📦 Output Excel Report Structure

The exported `.xlsx` file contains 6 sheets:

| Sheet             | Contents                                  |
|-------------------|-------------------------------------------|
| **Summary**       | High-level counts and metadata            |
| **Missing Values**| Rows with null/blank cells                |
| **Outliers**      | Numeric rows beyond IQR or Z-Score bounds |
| **Data Type Issues** | Rows where values don't match column dtype |
| **All Issues**    | Deduplicated union of all issue rows      |
| **Original Data** | Full original dataset for reference       |

---

## 🔬 Detection Methods

### Missing Values
Flags any row where at least one selected column has a `null`, `NaN`, or blank value.

### Outliers (numeric columns only)
- **IQR**: Flags values outside `[Q1 − 1.5×IQR, Q3 + 1.5×IQR]`
- **Z-Score**: Flags values more than 3 standard deviations from the mean

### Data Type Issues
Flags cells that cannot be coerced to the expected column dtype (e.g., text in a numeric column).

---

## 📁 File Structure

```
data_quality_checker.py   ← Main Streamlit application
requirements.txt          ← Python dependencies
README.md                 ← This guide
```

---

## 🛠️ Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| `.xls` files not reading | Ensure `xlrd>=2.0.1` is installed |
| Port already in use | Run `streamlit run data_quality_checker.py --server.port 8502` |
| Large files are slow | Pre-filter your file or select fewer columns |

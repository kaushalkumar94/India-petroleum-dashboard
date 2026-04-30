# =============================================================================
# India Petroleum Dashboard — ETL Script
# =============================================================================
# What this script does:
#   1. Reads the new PPAC Excel files (2024-25 and 2025-26)
#   2. Cleans and transforms them to match the existing CSV format
#   3. Outputs 4 clean CSV files ready to load into Power BI
#
# Run this script whenever new PPAC Excel files are available.
# Output files go into the data/processed/ folder.
# =============================================================================

import pandas as pd
import os

# =============================================================================
# SECTION 1 — CONFIGURATION
# All file paths and settings are defined here so they are easy to update
# =============================================================================

# Input folders
RAW_EXCEL_FOLDER = "data/raw/ppac_excel"
RAW_CSV_FOLDER   = "data/raw/original_csvs"

# Output folder
OUTPUT_FOLDER = "data/processed"

# Input file names
FILES = {
    # Petroleum Products (product-wise refinery output)
    "petrol_2425": "2024-2025_Crude_Oil_Processed_by_Refineries.xlsx",
    "petrol_2526": "2025-2026_Production_of_Petroleum_Products_by_Refineries___Fractionators.xlsx",

    # Indigenous Crude Oil Production (company-wise)
    "indigenous_2425": "2024-2025_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx",
    "indigenous_2526": "2025-2026_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx",
}

# Original CSVs (2022-2024 baseline data)
ORIGINAL_INDIGENOUS_CSV = os.path.join(
    RAW_CSV_FOLDER, "Monthly_Indigenous_Crude_Oil_Production.csv"
)

# Month abbreviation → (full name, month number)
# Indian fiscal year: APR is month 1 (April), MAR is month 12 (March)
MONTH_INFO = {
    "APR":  ("April",     4),
    "MAY":  ("May",       5),
    "JUN":  ("June",      6),
    "JULY": ("July",      7),
    "AUG":  ("August",    8),
    "SEP":  ("September", 9),
    "OCT":  ("October",  10),
    "NOV":  ("November", 11),
    "DEC":  ("December", 12),
    "JAN":  ("January",   1),
    "FEB":  ("February",  2),
    "MAR":  ("March",     3),
}

# The month columns in order (as they appear in the Excel files)
MONTH_COLUMNS = ["APR", "MAY", "JUN", "JULY", "AUG", "SEP",
                 "OCT", "NOV", "DEC", "JAN", "FEB", "MAR"]

# Product name → Product Group mapping
# Used to group similar products (e.g. HSD-VI + HSD Others → "HSD")
PRODUCT_GROUPS = {
    "HSD":     "HSD",
    "MS":      "MS / Petrol",
    "LPG":     "LPG",
    "NAPHTHA": "Naphtha",
    "ATF":     "ATF",
    "FO":      "Fuel Oil",
    "LUBES":   "Lubes",
    "BITUMEN": "Bitumen",
    "RPC":     "Petcoke",
    "LSHS":    "LSHS",
    "SKO":     "SKO",
    "LDO":     "LDO",
}


# =============================================================================
# SECTION 2 — HELPER FUNCTIONS
# Small reusable functions used throughout the script
# =============================================================================

def get_calendar_year(month_col, fiscal_year_start):
    """
    Convert a fiscal year month to an actual calendar year.

    In India, fiscal year runs April to March.
    Example: fiscal year 2024-25 means:
        APR to DEC → calendar year 2024
        JAN to MAR → calendar year 2025

    Parameters:
        month_col        : month abbreviation e.g. "APR", "JAN"
        fiscal_year_start: the starting year e.g. 2024 for FY 2024-25
    """
    month_number = MONTH_INFO[month_col][1]
    if month_number >= 4:
        return fiscal_year_start       # APR–DEC stay in the start year
    else:
        return fiscal_year_start + 1   # JAN–MAR move to the next year


def make_date_string(month_col, fiscal_year_start):
    """
    Build a date string in DD-MM-YYYY format for the first of each month.

    Example: month_col="JUN", fiscal_year_start=2024 → "01-06-2024"
    """
    month_number = MONTH_INFO[month_col][1]
    calendar_year = get_calendar_year(month_col, fiscal_year_start)
    return f"01-{str(month_number).zfill(2)}-{calendar_year}"


def get_product_group(product_name):
    """
    Map a raw product name to a clean Product Group label.

    Example: "HSD-VI"    → "HSD"
             "MS Others" → "MS / Petrol"
             "BITUMEN"   → "Bitumen"
    """
    product_upper = str(product_name).upper()
    for keyword, group in PRODUCT_GROUPS.items():
        if keyword in product_upper:
            return group
    return "Others"


def ensure_output_folder():
    """Create the output folder if it does not already exist."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    print(f"Output folder ready: {OUTPUT_FOLDER}")


# =============================================================================
# SECTION 3 — PETROLEUM PRODUCTS PROCESSING
# Reads the wide-format Excel files and converts them to long format
# matching the FactRefinery table structure in Power BI
# =============================================================================

# Rows to skip in the product tables (junk rows like TOTAL, REFINERIES etc.)
PRODUCT_ROWS_TO_SKIP = ["TOTAL,", "TOTAL", "REFINERIES", "FRACTIONATORS", "of which"]


def process_petroleum_products(filepath, fiscal_year_start):
    """
    Read one petroleum products Excel file and return a clean long-format DataFrame.

    Input (wide format):
        PRODUCTS | APR  | MAY  | JUN  | ...
        LPG      | 1037 | 1074 | 1077 | ...
        NAPHTHA  | 1535 | 1565 | 1458 | ...

    Output (long format):
        Month | Year | Products | Quantity_000MT | Date       | Product Group
        April | 2024 | LPG      | 1037.6         | 01-04-2024 | LPG
        May   | 2024 | LPG      | 1074.3         | 01-05-2024 | LPG

    Parameters:
        filepath         : path to the Excel file
        fiscal_year_start: e.g. 2024 for fiscal year 2024-25
    """
    print(f"  Reading: {os.path.basename(filepath)}")

    # Row 8 (index 8) is the header row in all PPAC petroleum product files
    df = pd.read_excel(filepath, header=None)
    data_rows = df.iloc[9:].copy()    # Data starts from row 9
    data_rows.columns = df.iloc[8].values
    data_rows = data_rows.rename(columns={data_rows.columns[0]: "Products"})

    # Remove rows that are totals or summaries — not actual products
    data_rows = data_rows[data_rows["Products"].notna()]
    data_rows = data_rows[~data_rows["Products"].astype(str).str.strip().isin(PRODUCT_ROWS_TO_SKIP)]
    data_rows = data_rows[data_rows["Products"].astype(str).str.strip() != "nan"]

    # Convert from wide format to long format — one row per product per month
    output_rows = []
    for _, row in data_rows.iterrows():
        product = str(row["Products"]).strip()
        if not product or product == "nan":
            continue

        for month_col in MONTH_COLUMNS:
            if month_col not in data_rows.columns:
                continue

            value = row[month_col]
            if pd.isna(value):
                continue

            output_rows.append({
                "Month":          MONTH_INFO[month_col][0],
                "Year":           get_calendar_year(month_col, fiscal_year_start),
                "Products":       product,
                "Quantity_000MT": round(float(value), 3),
                "Date":           make_date_string(month_col, fiscal_year_start),
                "Product Group":  get_product_group(product),
            })

    result = pd.DataFrame(output_rows)
    print(f"    → {len(result)} rows extracted")
    return result


# =============================================================================
# SECTION 4 — INDIGENOUS CRUDE OIL PROCESSING
# Reads PPAC indigenous production files (units = Million MT)
# and converts to 000 MT to match the existing data
# =============================================================================

# Row indices for each company/category in the indigenous production Excel files
# These are fixed positions in all PPAC indigenous production files
INDIGENOUS_COMPANY_ROWS = {
    "ONGC":         11,   # Nomination Blocks - ONGC
    "OIL":          12,   # Nomination Blocks - OIL
    "JVC/ Private": 15,   # PSC/RSC regime (private players)
    "Condensate":   18,   # Condensate production
}

INDIGENOUS_TOTAL_ROWS = {
    "Total crude oil":                  16,   # Crude oil total (without condensate)
    "Total ( Crude oil + Condensate)":  19,   # Grand total including condensate
}


def process_indigenous_production(filepath, fiscal_year_start):
    """
    Read one indigenous crude oil Excel file and return two DataFrames:
        1. company_df  — individual company rows (ONGC, OIL, JVC, Condensate)
        2. totals_df   — aggregate total rows

    IMPORTANT: Source values are in Million Metric Tonnes (MMT)
    We multiply by 1000 to convert to 000 Metric Tonnes (matching Power BI tables)

    Parameters:
        filepath         : path to the Excel file
        fiscal_year_start: e.g. 2024 for fiscal year 2024-25
    """
    print(f"  Reading: {os.path.basename(filepath)}")

    df = pd.read_excel(filepath, header=None)

    def extract_rows(row_map):
        """Extract specified rows from the Excel file and build long-format records."""
        output_rows = []
        for entity_name, row_index in row_map.items():
            for col_index, month_col in enumerate(MONTH_COLUMNS):
                value = df.iloc[row_index, col_index + 1]   # +1 to skip the label column

                if pd.isna(value):
                    continue

                output_rows.append({
                    "Month":          MONTH_INFO[month_col][0],
                    "Year":           get_calendar_year(month_col, fiscal_year_start),
                    "Company Name":   entity_name,
                    "Quantity_000MT": round(float(value) * 1000, 3),  # MMT → 000 MT
                    "Date":           make_date_string(month_col, fiscal_year_start),
                })

        return pd.DataFrame(output_rows)

    company_df = extract_rows(INDIGENOUS_COMPANY_ROWS)
    totals_df  = extract_rows(INDIGENOUS_TOTAL_ROWS)

    print(f"    → {len(company_df)} company rows, {len(totals_df)} total rows extracted")
    return company_df, totals_df


# =============================================================================
# SECTION 5 — ORIGINAL CSV PROCESSING
# The original 2022-2024 indigenous CSV also uses Million MT (mislabelled).
# We fix the units here before combining with new data.
# =============================================================================

def process_original_indigenous_csv(filepath):
    """
    Read the original Monthly_Indigenous_Crude_Oil_Production.csv and fix units.

    The column header says "000 Metric Tonnes" but values are actually
    in Million Metric Tonnes (e.g. 2.26 instead of 2260).
    We multiply by 1000 to correct this.

    Also splits the data into:
        - company rows (ONGC, OIL, JVC/Private, Condensate)
        - total rows   (Total crude oil, Total Crude oil + Condensate)
    """
    print(f"  Reading original CSV: {os.path.basename(filepath)}")

    df = pd.read_csv(filepath)

    # Fix the unit — values are in MMT, convert to 000 MT
    df["Quantity_000MT"] = df["Quantity (000 Metric Tonnes)"] * 1000

    # Drop columns we don't need
    df = df.drop(columns=["Quantity (000 Metric Tonnes)", "last_updated"])

    # Add a proper Date column
    month_to_number = {name: num for _, (name, num) in MONTH_INFO.items()}
    df["Date"] = df.apply(
        lambda row: f"01-{str(month_to_number.get(row['Month'], 0)).zfill(2)}-{row['Year']}",
        axis=1
    )

    # Split into companies vs totals
    is_total = df["Company Name"].str.lower().str.contains("total")
    company_df = df[~is_total].copy()
    totals_df  = df[is_total].copy()

    print(f"    → {len(company_df)} company rows, {len(totals_df)} total rows extracted")
    return company_df, totals_df


# =============================================================================
# SECTION 6 — MAIN SCRIPT
# Runs all the steps in order and saves the final output files
# =============================================================================

def main():
    print("\n" + "=" * 60)
    print("  India Petroleum Dashboard — ETL Script")
    print("=" * 60)

    ensure_output_folder()

    # ------------------------------------------------------------------
    # STEP 1: Process Petroleum Products (FactRefinery extension)
    # ------------------------------------------------------------------
    print("\n[Step 1] Processing petroleum products data...")

    petrol_2425 = process_petroleum_products(
        filepath          = os.path.join(RAW_EXCEL_FOLDER, "2024-205_Production_of_Petroleum_Products_by_Refineries___Fractionators.xlsx"),
        fiscal_year_start = 2024
    )

    petrol_2526 = process_petroleum_products(
        filepath          = os.path.join(RAW_EXCEL_FOLDER, "2025-2026_Production_of_Petroleum_Products_by_Refineries___Fractionators.xlsx"),
        fiscal_year_start = 2025
    )

    # Combine both years into one file
    fact_refinery_new = pd.concat([petrol_2425, petrol_2526], ignore_index=True)
    print(f"\n  Combined petroleum products: {len(fact_refinery_new)} total rows")

    # ------------------------------------------------------------------
    # STEP 2: Process Indigenous Production (new PPAC files)
    # ------------------------------------------------------------------
    print("\n[Step 2] Processing new indigenous production data (2024-26)...")

    companies_2425, totals_2425 = process_indigenous_production(
        filepath          = os.path.join(RAW_EXCEL_FOLDER, "2024-2025_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx"),
        fiscal_year_start = 2024
    )

    companies_2526, totals_2526 = process_indigenous_production(
        filepath          = os.path.join(RAW_EXCEL_FOLDER, "2025-2026_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx"),
        fiscal_year_start = 2025
    )

    # ------------------------------------------------------------------
    # STEP 3: Process Original Indigenous CSV (2022-2024 baseline)
    # ------------------------------------------------------------------
    print("\n[Step 3] Processing original indigenous CSV (2022-2024)...")

    orig_companies, orig_totals = process_original_indigenous_csv(ORIGINAL_INDIGENOUS_CSV)

    # ------------------------------------------------------------------
    # STEP 4: Combine all years together
    # ------------------------------------------------------------------
    print("\n[Step 4] Combining all years...")

    indigenous_companies_final = pd.concat(
        [orig_companies, companies_2425, companies_2526],
        ignore_index=True
    )

    indigenous_totals_final = pd.concat(
        [orig_totals, totals_2425, totals_2526],
        ignore_index=True
    )

    print(f"  Indigenous companies (all years): {len(indigenous_companies_final)} rows")
    print(f"  Indigenous totals   (all years): {len(indigenous_totals_final)} rows")

    # ------------------------------------------------------------------
    # STEP 5: Save output files
    # ------------------------------------------------------------------
    print("\n[Step 5] Saving output files...")

    output_files = {
        "FactRefinery_2024_2026.csv":                fact_refinery_new,
        "IndigenousProduction_Companies_FINAL.csv":  indigenous_companies_final,
        "IndigenousProduction_Totals_FINAL.csv":     indigenous_totals_final,
    }

    for filename, dataframe in output_files.items():
        output_path = os.path.join(OUTPUT_FOLDER, filename)
        dataframe.to_csv(output_path, index=False)
        print(f"  Saved: {output_path}  ({len(dataframe)} rows)")

    # ------------------------------------------------------------------
    # DONE
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  ETL complete! Load these files into Power BI:")
    print("  • Append FactRefinery_2024_2026.csv → FactRefinery")
    print("  • Replace IndigenousProduction_Companies_FINAL.csv")
    print("  • Replace IndigenousProduction_Totals_FINAL.csv")
    print("=" * 60 + "\n")


# Run the script
if __name__ == "__main__":
    main()

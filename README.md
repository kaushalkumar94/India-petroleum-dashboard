# India Petroleum Dashboard — Power BI

> An end-to-end data analytics project built on government petroleum data published by the **Petroleum Planning & Analysis Cell (PPAC), Ministry of Petroleum & Natural Gas, Government of India.**

---

## Table of Contents

- [Project Overview](#project-overview)
- [Key Insights](#key-insights)
- [Dashboard Pages](#dashboard-pages)
- [Data Sources](#data-sources)
- [ETL Pipeline](#etl-pipeline)
- [Data Model](#data-model)
- [DAX Measures](#dax-measures)
- [Challenges & Solutions](#challenges--solutions)
- [Directory Structure](#directory-structure)
- [How To Run](#how-to-run)
- [Tools Used](#tools-used)

---

## Project Overview

This project builds a 3-page interactive Power BI dashboard analyzing India's petroleum sector from **April 2022 to March 2026**, covering:

- Monthly refinery production across 13 petroleum product categories
- Indigenous crude oil production by company (ONGC, OIL, JVC/Private)
- Refinery-wise crude oil processing across 21 refineries across India
- Year-over-Year and Month-over-Month production trends
- India's crude oil import dependency analysis

The dashboard is designed as a **government-level analytical tool** for policymakers, energy sector analysts, and researchers.

**Data span:** April 2022 — March 2026 (2025-26 data is Provisional)
**New data added:** 6 PPAC Excel files processed via Python ETL pipeline

---

## Key Insights

### Executive Level
| Metric | Value |
|---|---|
| Total Refinery Production (2022–2026) | ~1.11 Million (000 MT) |
| Average Monthly Production | ~23.15K (000 MT/month) |
| YoY Production Growth | ~34.47% |
| Import Dependency | ~89.63% |
| Refinery output growth (Apr 2022 → Mar 2026) | ~22K → ~25K (000 MT/month) |

### Product Mix
| Finding | Detail |
|---|---|
| HSD dominates output | 42.19% of all production — diesel dependency is critical |
| Top 2 fuels combined | HSD + MS/Petrol = nearly 60% of all output |
| Fastest growing | ATF +55% from 2022 to 2025 — India's aviation boom post-COVID |
| Declining products | LSHS and LDO nearly phased out — decarbonization signal |

### Indigenous Production & Refineries
| Finding | Detail |
|---|---|
| ONGC dominance | 61.49% of all domestic crude — concentrated supply risk |
| PSU control | ONGC + OIL = 81.67% — private sector barely present |
| Largest refinery | RIL Jamnagar — 69K (000 MT), world's largest refining complex |
| Domestic output trend | Completely flat across 4 years — all refinery growth met by imports |

### Critical Policy Finding
> **India imports approximately 89.63% of its crude oil needs.** While refinery infrastructure grew ~14% over 4 years, domestic crude production remained completely flat. Every additional barrel of refinery throughput came from imported crude — making India's energy security heavily exposed to global oil price shocks and geopolitical risk.

---

## Dashboard Pages

### Page 1 — Executive Overview
High-level snapshot for leadership and policymakers.

- 5 KPI cards: Total Refinery Production, Total Indigenous Production, YoY Change %, Avg Monthly Production, Import Dependency %
- Dual-axis line chart: Refinery output vs Indigenous production (Apr 2022 → Mar 2026)
- Slicers: Year, Product Group

### Page 2 — Petroleum Product Distribution
Which petroleum products dominate India's refinery output.

- Donut chart: Product share % across all years
- Clustered horizontal bar chart: Volume by product group sorted by size
- Heatmap matrix: Year × Product Group with gradient color scaling
- KPI cards: Top Product, HSD Share %, Total Production, Avg Monthly Production
- Slicers: Year (tile), Product Group

### Page 3 — Indigenous & Refinery Analysis
Company-wise domestic crude and refinery-level processing data.

- 4 KPI cards: ONGC Share %, Total Refinery Companies, Import Dependency %, Total Indigenous Production
- Stacked area chart: ONGC vs OIL vs JVC/Private vs Condensate over time
- Top 10 refineries bar chart (from FactRefineryCompanyWise — unique dataset)
- Company share donut chart
- Slicers: Year, Company Name

---

## Data Sources

All data sourced from **PPAC (Petroleum Planning & Analysis Cell)**, Government of India.

| File | Period | Description | Unit |
|---|---|---|---|
| `Monthly_Crude_Oil_Processed_by_Refineries.csv` | Apr 2022–Mar 2024 | Product-wise refinery output | 000 MT |
| `Monthly_Indigenous_Crude_Oil_Production.csv` | Apr 2022–Mar 2024 | Company-wise domestic crude | Million MT* |
| `Monthly_Production_of_Petroleum_Products_by_Refineries_Fractionators.csv` | Apr 2022–Mar 2024 | Confirmed identical to above — deleted | 000 MT |
| `2024-2025_Crude_Oil_Processed_by_Refineries.xlsx` | Apr 2024–Mar 2025 | Refinery-wise crude processed | 000 MT |
| `2025-2026_Crude_Oil_Processed_by_Refineries.xlsx` | Apr 2025–Mar 2026 | Refinery-wise crude processed | 000 MT |
| `2024-2025_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx` | Apr 2024–Mar 2025 | Company-wise domestic crude | Million MT* |
| `2025-2026_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx` | Apr 2025–Mar 2026 | Company-wise domestic crude | Million MT* |
| `2024-25_Production_of_Petroleum_Products.xlsx` | Apr 2024–Mar 2025 | Product-wise refinery output | 000 MT |
| `2025-2026_Production_of_Petroleum_Products.xlsx` | Apr 2025–Mar 2026 | Product-wise refinery output | 000 MT |

*\* Converted to 000 MT (×1000) during ETL for consistency*

---

## ETL Pipeline

```
┌──────────────────────────────────────────────────────┐
│                    E — EXTRACT                       │
│                                                      │
│   Original CSVs           PPAC Excel Files           │
│   (3 files, 2022-24)      (6 files, 2024-26)         │
│   Source: PPAC, Govt of India                        │
│   Units: 000 MT (refinery) / MMT (indigenous)        │
└─────────────────────┬────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────┐
│                  T — TRANSFORM                       │
│                                                      │
│   Python / pandas:                                   │
│   • Unit fix: MMT × 1000 → 000 MT                   │
│   • Unpivot: wide format → long format               │
│   • FY label → calendar date (APR = month 4)        │
│   • Remove junk rows (logos, totals, notes)          │
│   • Output: 4 clean CSV files                        │
│                                                      │
│   Power Query (inside Power BI):                     │
│   • Add Product Group column (conditional)           │
│   • Split aggregate rows vs company rows             │
│   • Rename columns (DAX-friendly, no spaces)         │
│   • Set correct data types                           │
│   • Append new data to existing tables               │
└─────────────────────┬────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────┐
│                   L — LOAD                           │
│                                                      │
│              [ DateTable ]  ← dimension              │
│             /    |     \     \                       │
│    FactRefinery  │  Indigenous  FactRefinery         │
│                  │  Companies   CompanyWise          │
│             Indigenous                               │
│              Totals                                  │
│                                                      │
│   + _Measures table (13 DAX measures)                │
└──────────────────────────────────────────────────────┘
```

---

## Data Model

Star schema — one date dimension, four fact tables.

| Table | Type | Description |
|---|---|---|
| `DateTable` | Dimension | Calendar Apr 2022 → Mar 2026 with Year, Month, Quarter |
| `FactRefinery` | Fact | Product-wise refinery output — 13 products, all months |
| `IndigenousProduction_Companies_FINAL` | Fact | ONGC, OIL, JVC/Private, Condensate monthly production |
| `IndigenousProduction_Totals_FINAL` | Fact | Aggregate crude totals for KPI cards |
| `FactRefineryCompanyWise` | Fact | 21 individual refineries — monthly crude processed |
| `_Measures` | Measures | All DAX calculations stored centrally |

All fact tables connect to `DateTable[Date]` via Many-to-One, Single cross-filter, Active relationship.

---

## DAX Measures

```dax
-- Core production
Total Refinery Production = SUM(FactRefinery[Quantity_000MT])

Total Indigenous Production =
SUM(IndigenousProduction_Companies_FINAL[Quantity_000MT])

Total Indigenous (Aggregated) =
CALCULATE(
    SUM(IndigenousProduction_Totals_FINAL[Quantity_000MT]),
    IndigenousProduction_Totals_FINAL[Company Name] = "Total ( Crude oil + Condensate)"
)

Total Crude Processed by Refinery =
SUM(FactRefineryCompanyWise[Quantity_000MT])

-- KPIs
Import Dependency % =
DIVIDE(
    [Total Refinery Production] - [Total Indigenous (Aggregated)],
    [Total Refinery Production], 0
) * 100

Avg Monthly Production =
AVERAGEX(VALUES(DateTable[Year-Month]), [Total Refinery Production])

ONGC Share % =
DIVIDE(
    CALCULATE(
        SUM(IndigenousProduction_Companies_FINAL[Quantity_000MT]),
        IndigenousProduction_Companies_FINAL[Company Name] = "ONGC"
    ),
    CALCULATE(
        SUM(IndigenousProduction_Companies_FINAL[Quantity_000MT]),
        ALL(IndigenousProduction_Companies_FINAL[Company Name])
    ), 0
) * 100

HSD Share % =
CALCULATE([Product Share %], FactRefinery[Product Group] = "HSD")

-- Time intelligence
Production Last Year =
CALCULATE([Total Refinery Production], SAMEPERIODLASTYEAR(DateTable[Date]))

YoY Change % =
DIVIDE([Total Refinery Production] - [Production Last Year],
       [Production Last Year], 0) * 100

MoM Change % =
VAR CurrentMonth = [Total Refinery Production]
VAR PrevMonth = CALCULATE(
    [Total Refinery Production], DATEADD(DateTable[Date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0) * 100

Production YTD =
CALCULATE([Total Refinery Production], DATESYTD(DateTable[Date]))

-- Product share
Product Share % =
DIVIDE(
    SUM(FactRefinery[Quantity_000MT]),
    CALCULATE(SUM(FactRefinery[Quantity_000MT]),
              ALL(FactRefinery[Product Group])), 0
) * 100

-- Dynamic chart title
Chart Title =
"Production Trend — " &
IF(ISFILTERED(FactRefinery[Product Group]),
   SELECTEDVALUE(FactRefinery[Product Group], "Multiple Products"),
   "All Products")
```

---

## Challenges & Solutions

### Challenge 1 — Duplicate source files
**Problem:** Two of the three original CSVs appeared identical.
**Solution:** Left Anti Join in Power Query on Date + Products + Quantity → 0 of 360 rows returned. Confirmed identical. Deleted one.

### Challenge 2 — Unit mismatch *(most critical)*
**Problem:** Original indigenous CSV stored values in Million MT but header said 000 MT. Caused a sudden jump in the line chart at Jan 2024.
**Solution:** Rebuilt both indigenous tables from scratch in Python — multiplying all original values by 1000. Same conversion applied to new PPAC files.
**Lesson:** Always verify units at source. Government datasets frequently mix reporting units across years.

### Challenge 3 — Wide vs long format
**Problem:** All PPAC Excel files stored months as columns — Power BI needs rows.
**Solution:** Python/pandas unpivot + fiscal year to calendar year conversion.

### Challenge 4 — Aggregate rows mixed with company rows
**Problem:** Indigenous CSV mixed ONGC/OIL rows with Total rows — summing all would double count.
**Solution:** Split into two tables in Power Query. DAX measure filters to "Total (Crude oil + Condensate)" only.

### Challenge 5 — Inconsistent product naming
**Problem:** HSD-VI, HSD Others, MS-VI, MS Others etc. — same product, different names.
**Solution:** Product Group column added via conditional Power Query formula covering all 15 variants.

### Challenge 6 — X-axis sort order
**Problem:** Year-Month text column sorted alphabetically not chronologically.
**Solution:** Replaced with DateTable[Date] — native chronological sort + free drill-down hierarchy.

### Challenge 7 — Power Query step ordering
**Problem:** Custom column formulas failed because column renames had not yet run.
**Solution:** Always add custom columns after the last Applied Step — all renames are complete at that point.

---

## Directory Structure

```
india-petroleum-dashboard/
│
├── README.md
│
├── data/
│   ├── raw/
│   │   ├── original_csvs/
│   │   │   ├── Monthly_Crude_Oil_Processed_by_Refineries.csv
│   │   │   ├── Monthly_Indigenous_Crude_Oil_Production.csv
│   │   │   └── Monthly_Production_of_Petroleum_Products_by_Refineries_Fractionators.csv
│   │   │
│   │   └── ppac_excel/
│   │       ├── 2024-2025_Crude_Oil_Processed_by_Refineries.xlsx
│   │       ├── 2025-2026_Crude_Oil_Processed_by_Refineries.xlsx
│   │       ├── 2024-2025_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx
│   │       ├── 2025-2026_INDIGENOUS_CRUDE_OIL_PRODUCTION.xlsx
│   │       ├── 2024-25_Production_of_Petroleum_Products.xlsx
│   │       └── 2025-2026_Production_of_Petroleum_Products.xlsx
│   │
│   └── processed/
│       ├── FactRefinery_2024_2026.csv
│       ├── FactRefineryCompanyWise.csv
│       ├── IndigenousProduction_Companies_FINAL.csv
│       └── IndigenousProduction_Totals_FINAL.csv
│
├── scripts/
│   └── process_new_data.py
│
├── powerbi/
│   └── India_Petroleum_Dashboard.pbix
│
└── docs/
    ├── dashboard_page1.png
    ├── dashboard_page2.png
    ├── dashboard_page3.png
    ├── data_model.png
    └── etl_pipeline.png
```

---

## How To Run

**Install dependencies:**
```bash
pip install pandas openpyxl
```

**Run ETL script:**
```bash
cd india-petroleum-dashboard
python scripts/process_new_data.py
```

Output files appear in `data/processed/` — append to existing Power BI tables via Power Query.

**Open dashboard:**
Open `powerbi/India_Petroleum_Dashboard.pbix` in Power BI Desktop — all data is pre-loaded.

---

## Tools Used

| Tool | Purpose |
|---|---|
| Python 3 + pandas | ETL — cleaning, unit conversion, unpivoting, date transformation |
| Power BI Desktop | Data modeling, DAX, dashboard building |
| Power Query (M) | In-tool transformation, appending, type setting |
| DAX | KPI calculations, time intelligence, dynamic titles |
| GitHub | Version control and project hosting |

---

## Notes

- All quantities in **000 Metric Tonnes** unless stated otherwise
- Data marked **(P)** is **Provisional** — subject to revision by PPAC
- India's fiscal year runs **April to March** — all date conversions account for this
- The two original refinery CSVs were confirmed identical — only one was retained

---

*Data source: Petroleum Planning & Analysis Cell (PPAC), Ministry of Petroleum & Natural Gas, Government of India*
*Built for analytical and research purposes only*

# Implementation Plan: National UDISE+ Policy Analysis & PowerBI Dashboard Optimization

This plan outlines the end-to-end analytical framework to process the complete national UDISE+ database (comprising **1.47 million schools** and **8.5 million enrolment records** across 2023-24 and 2024-25) using PySpark. It delivers high-impact insights for all 10 policy prompts and exports highly optimized, aggregated data files designed for professional, lag-free PowerBI dashboards.

---

## User Review Required

> [!IMPORTANT]
> **Dataset Size & PowerBI Performance Strategy**
> The raw school-level datasets contain **1.47 million schools** and **~8.5 million enrolment rows**. Exporting these raw tables directly to CSV/PowerBI will result in a ~250MB dataset that would cause severe performance lag or crashes on standard desktop computers.
> 
> **Our Proposed Strategy**:
> We will compute all school-level indices (such as Pupil-Teacher Ratios, Infrastructure Index, Toilet Availability, and Cohort Transition Rates) using **PySpark** first, and then export two highly optimized, aggregated tables:
> 1. **`district_policy_indicators.csv`** (~30,000 rows): Aggregated by `State`, `District`, `Rural/Urban`, `School Category`, and `School Management`. This maintains all critical dimensions for PowerBI dashboard filters (slicers) while reducing the row count by **98%**, ensuring instantaneous loading and smooth rendering.
> 2. **`cohort_transitions_state_district.csv`** (~5,000 rows): Specifically designed for year-over-year transition tracking (Class 8 $\rightarrow$ 9 and Class 10 $\rightarrow$ 11) disaggregated by Gender and Social Category (General, SC, ST, OBC).
> 
> All metrics will remain **statistically perfect** because we will export raw counts (total enrolment, total teachers, total functional toilets, etc.) and use mathematically correct **DAX measures** in PowerBI (e.g. `PTR = SUM(total_enrolment) / SUM(total_tch)`) rather than averaging pre-calculated averages.

---

## Open Questions

- *Are there any specific districts that you want to highlight as "Aspirational Districts" in your policy slides, or should we dynamically highlight the bottom 10% of districts in terms of Infrastructure and Transition Rates?* (Recommendation: Dynamically highlight the bottom 10% districts as priority action zones).
- *Would you like us to include the NFHS (National Family Health Survey) state-level nutrition/anemia benchmarks as a supplementary join to answer Prompt 7?* (Recommendation: Yes, we will join standard external NHFS-5 state anemia rates in the analysis).

---

## Proposed Changes

### Data Pipeline & Core Scripts

We will create two main files in the workspace root:

1. **`run_udise_analysis_pipeline.py`** [NEW]: A standalone, high-performance, memory-optimized PySpark script that does the heavy lifting, executes the full data joining/cleaning across both academic years, and writes the PowerBI exports.
2. **`UDISE_Data_Analysis.ipynb`** [NEW]: A premium Jupyter Notebook that loads the Parquet files, walks through each of the 10 datathon prompts step-by-step with rich markdown explanations, prints key tabular insights, and displays ASCII policy tables.

---

### Analytical Framework for all 10 Prompts

Here is how our PySpark pipeline maps UDISE columns to solve the policy prompts:

| Prompt | Policy Focus | UDISE Tables | Computational Approach in PySpark |
| :--- | :--- | :--- | :--- |
| **Prompt 1 & 2** | Regional & Social Disparities | `profile1`, `enroll1` | Group enrolment by `rural_urban`, `gender`, and social category (`item_id` 1-4 for General, SC, ST, OBC) and evaluate year-over-year cohort changes from 2023-24 to 2024-25. |
| **Prompt 3** | Toilet & Teacher Impact on Retention | `facility`, `teacher`, `enroll1` | Correlate `total_girls_func_toilet` and `female` teacher count with the Class 8 $\rightarrow$ 9 girls transition rate. |
| **Prompt 4 & 5** | PTR & Infrastructure vs Enrolment | `facility`, `teacher`, `enroll1` | Compute `ptr = total_enrolment / total_tch`. Build an `Infra_Score` (0 to 7) based on electricity, drinking water, girls toilet, boys toilet, library, playground, and computer lab. |
| **Prompt 6 & 7** | Socio-economic & Health proxies | `enroll1`, `profile1` | Extract BPL (`item_id` 13) and EWS (`item_id` 32) enrolments. Map State-level child anemia percentages (NFHS-5) to school performance. |
| **Prompt 8** | School Management & Facility Impact | `profile1`, `facility`, `enroll1` | Segment schools by `managment` (Government, Private Aided, Private Unaided) and facility access (libraries, digital class TVs) to compare retention. |
| **Prompt 9** | Girls Retention in Aspirational Districts | `profile1`, `enroll1` | Focus disaggregated analysis on identified underperforming rural districts to highlight gender gaps. |
| **Prompt 10** | Transition Bottlenecks | `enroll1` | Match schools in 23-24 and 24-25 on `pseudocode` and compute: <br>• **Upper Primary $\rightarrow$ Secondary**: Class 8 (23-24) $\rightarrow$ Class 9 (24-25)<br>• **Secondary $\rightarrow$ Sr. Secondary**: Class 10 (23-24) $\rightarrow$ Class 11 (24-25) |

---

### Component-Level File Breakdown

#### [NEW] [run_udise_analysis_pipeline.py](file:///c:/Users/nagam/Final_Projects/UDISE/run_udise_analysis_pipeline.py)
This script will execute the following steps:
- Initialize Spark session optimized with 4GB memory limits.
- Load all Parquet tables (`profile1`, `facility`, `teacher`, `enroll1`) for both years.
- Filter `enroll1` for `item_group = 1` (Social categories) to calculate clean enrolment totals.
- Compute school-level metrics:
  - Total Enrolment 23-24 (`total_enr_23`) and 2024-25 (`total_enr_24`)
  - Pupil-Teacher Ratio (`ptr_23` and `ptr_24`)
  - Infrastructure Index `infra_score_23` (sum of 7 indicators)
- Join Year 1 and Year 2 on `pseudocode` to calculate cohort transitions:
  - `c8_transition_rate = (c9_enr_24 / c8_enr_23) * 100` (capped at 100%)
  - `c10_transition_rate = (c11_enr_24 / c10_enr_23) * 100` (capped at 100%)
- Aggregate data to **District-level** with full filters: `State`, `District`, `Rural_Urban`, `School_Category`, `Management`.
- Save the results as:
  - `data/powerbi_exports/district_policy_indicators.csv`
  - `data/powerbi_exports/cohort_transitions_state_district.csv`

#### [NEW] [UDISE_Data_Analysis.ipynb](file:///c:/Users/nagam/Final_Projects/UDISE/UDISE_Data_Analysis.ipynb)
- A highly polished, executive-level notebook with detailed markdown.
- Displays structured insights for each of the 10 prompts.
- Contains direct guidelines for modeling and designing the **PowerBI dashboard** (relationships, DAX measures, and interactive KPI visuals).

---

## Verification Plan

### Automated Verification
1. **Row Count & Schema Integrity**: Validate that the exported CSV files are created in `data/powerbi_exports/` and contain all aggregated district metrics.
2. **Mathematical Accuracy**: Run validation assertions inside Spark to verify that Transition Rates are bounded between 0% and 100% (handling cases where schools close or expand).
3. **PTR Verification**: Assert that Pupil-Teacher Ratio calculations correctly filter out division by zero (schools with zero teachers).

### Manual Verification
- Review the generated summary output showing the bottom 5 states by girls' transition rates and the correlation coefficient between infrastructure score and transition rates to ensure the policy insights are logical and rich.

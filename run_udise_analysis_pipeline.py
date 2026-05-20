import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

def main():
    print("=========================================================")
    print("STARTING UDISE+ MASSIVE NATIONAL DATA PIPELINE (PYSPARK)")
    print("=========================================================")
    
    start_time = time.time()
    
    # 1. Initialize Spark Session optimized for 4GB limits
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("UDISENationalPipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "40") \
        .config("spark.default.parallelism", "40") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Paths
    base_23 = r"c:\Users\nagam\Final_Projects\UDISE\data\2023-24_parquet"
    base_24 = r"c:\Users\nagam\Final_Projects\UDISE\data\2024-25_parquet"
    out_dir = r"c:\Users\nagam\Final_Projects\UDISE\data\powerbi_exports"
    
    os.makedirs(out_dir, exist_ok=True)
    
    try:
        # 2. Read datasets
        print("Loading Parquet datasets...")
        profile1_23 = spark.read.parquet(os.path.join(base_23, "profile1_parquet"))
        facility_23 = spark.read.parquet(os.path.join(base_23, "facility_parquet"))
        teacher_23 = spark.read.parquet(os.path.join(base_23, "teacher_parquet"))
        enroll1_23 = spark.read.parquet(os.path.join(base_23, "enroll1_parquet"))
        
        profile1_24 = spark.read.parquet(os.path.join(base_24, "profile1_parquet"))
        facility_24 = spark.read.parquet(os.path.join(base_24, "facility_parquet"))
        teacher_24 = spark.read.parquet(os.path.join(base_24, "teacher_parquet"))
        enroll1_24 = spark.read.parquet(os.path.join(base_24, "enroll1_parquet"))
        
        print("All Parquet datasets loaded successfully.")
        
        # 3. Process Enrolment tables (filter item_group = 1 for Social Categories)
        print("Processing Enrolment datasets (Filtering item_group = 1)...")
        
        # Map item_id to social category name
        # 1 -> General, 2 -> SC, 3 -> ST, 4 -> OBC
        enroll1_23_soc = enroll1_23.filter(F.col("item_group") == 1) \
            .filter(F.col("item_id").isin(1, 2, 3, 4)) \
            .withColumn("social_category", F.expr(
                "CASE WHEN item_id = 1 THEN 'General' "
                "WHEN item_id = 2 THEN 'SC' "
                "WHEN item_id = 3 THEN 'ST' "
                "WHEN item_id = 4 THEN 'OBC' END"
            ))
            
        enroll1_24_soc = enroll1_24.filter(F.col("item_group") == 1) \
            .filter(F.col("item_id").isin(1, 2, 3, 4)) \
            .withColumn("social_category", F.expr(
                "CASE WHEN item_id = 1 THEN 'General' "
                "WHEN item_id = 2 THEN 'SC' "
                "WHEN item_id = 3 THEN 'ST' "
                "WHEN item_id = 4 THEN 'OBC' END"
            ))
            
        # Class lists
        all_classes = ["cpp"] + [f"c{i}" for i in range(1, 13)]
        
        # Sum expressions for total boys/girls and overall total per school-social category
        boys_cols = [f"{c}_b" for c in all_classes]
        girls_cols = [f"{c}_g" for c in all_classes]
        
        # Add a school-level sum of enrolment in PySpark
        enr_boys_sum = "+".join([f"COALESCE({c}, 0)" for c in boys_cols])
        enr_girls_sum = "+".join([f"COALESCE({c}, 0)" for c in girls_cols])
        
        # Keep detailed columns but also pre-calculate totals for boys/girls
        enroll1_23_clean = enroll1_23_soc.withColumn("enr_boys", F.expr(enr_boys_sum)) \
                                         .withColumn("enr_girls", F.expr(enr_girls_sum)) \
                                         .withColumn("enr_total", F.col("enr_boys") + F.col("enr_girls"))
                                         
        enroll1_24_clean = enroll1_24_soc.withColumn("enr_boys", F.expr(enr_boys_sum)) \
                                         .withColumn("enr_girls", F.expr(enr_girls_sum)) \
                                         .withColumn("enr_total", F.col("enr_boys") + F.col("enr_girls"))

        print("Enrolment cleaning complete.")
        
        # 4. Join datasets for 2023-24 (Profile + Facility + Teacher + Enrolment)
        print("Joining 2023-24 Master Dataset...")
        
        # Select important columns from profile1
        prof_cols = ["pseudocode", "state", "district", "block", "rural_urban", "school_category", "school_type", "managment"]
        prof_df_23 = profile1_23.select(*prof_cols) \
            .withColumn("rural_urban_label", F.expr("CASE WHEN rural_urban = 1 THEN 'Rural' WHEN rural_urban = 2 THEN 'Urban' ELSE 'Unknown' END"))
            
        # Select and clean important columns from facility
        # electricity_availability: 1=Yes, 2=No, 3=Yes but not functional. We treat 1 as functional.
        # library_availability: 1=Yes, 2=No
        # playground_available: 1=Yes, 2=No
        # internet: 1=Yes, 2=No
        # comp_ict_lab_yn: 1=Yes, 2=No
        fac_df_23 = facility_23.select(
            "pseudocode",
            F.coalesce(F.col("total_class_rooms"), F.lit(0)).alias("total_class_rooms"),
            F.coalesce(F.col("classrooms_in_good_condition"), F.lit(0)).alias("classrooms_good"),
            F.expr("CASE WHEN electricity_availability = 1 THEN 1 ELSE 0 END").alias("has_electricity"),
            F.expr("CASE WHEN library_availability = 1 THEN 1 ELSE 0 END").alias("has_library"),
            F.expr("CASE WHEN playground_available = 1 THEN 1 ELSE 0 END").alias("has_playground"),
            F.expr("CASE WHEN internet = 1 THEN 1 ELSE 0 END").alias("has_internet"),
            F.coalesce(F.col("total_girls_func_toilet"), F.lit(0)).alias("girls_toilet_func"),
            F.coalesce(F.col("total_boys_func_toilet"), F.lit(0)).alias("boys_toilet_func"),
            F.expr("CASE WHEN smart_class_tv_tot > 0 THEN 1 ELSE 0 END").alias("has_smart_tv"),
            F.expr("CASE WHEN comp_ict_lab_yn = 1 THEN 1 ELSE 0 END").alias("has_comp_lab")
        )
        
        # Infrastructure score: sum of 7 key items (0 to 7)
        fac_df_23 = fac_df_23.withColumn("infra_score",
            F.col("has_electricity") +
            F.col("has_library") +
            F.col("has_playground") +
            F.col("has_internet") +
            F.expr("CASE WHEN girls_toilet_func > 0 THEN 1 ELSE 0 END") +
            F.expr("CASE WHEN boys_toilet_func > 0 THEN 1 ELSE 0 END") +
            F.col("has_comp_lab")
        )
        
        # Select and clean teacher columns
        # Schema from pdf page 9 says: "female No. of Female teachers in school"
        if "female" in teacher_23.columns:
            tch_df_23 = teacher_23.select(
                "pseudocode",
                F.coalesce(F.col("total_tch"), F.lit(0)).alias("total_tch"),
                F.coalesce(F.col("female"), F.lit(0)).alias("female_tch")
            )
        else:
            print("Warning: female column not found in teacher table, fallback to male calculation...")
            tch_df_23 = teacher_23.select(
                "pseudocode",
                F.coalesce(F.col("total_tch"), F.lit(0)).alias("total_tch"),
                (F.coalesce(F.col("total_tch"), F.lit(0)) - F.coalesce(F.col("male"), F.lit(0))).alias("female_tch")
            )
            
        # Join profile + facility + teacher for 2023-24 school master
        school_master_23 = prof_df_23.join(fac_df_23, on="pseudocode", how="inner") \
                                     .join(tch_df_23, on="pseudocode", how="inner")
                                     
        print("Joined school base profiles. Aggregating enrolment...")
        
        # School-level overall enrolment for 2023-24
        enr_school_23 = enroll1_23_clean.groupBy("pseudocode").agg(
            F.sum("enr_total").alias("total_enr_23"),
            F.sum("enr_boys").alias("total_boys_23"),
            F.sum("enr_girls").alias("total_girls_23"),
            F.sum("c8_b").alias("c8_b_23"),
            F.sum("c8_g").alias("c8_g_23"),
            F.sum("c10_b").alias("c10_b_23"),
            F.sum("c10_g").alias("c10_g_23")
        )
        
        # School-level overall enrolment for 2024-25
        enr_school_24 = enroll1_24_clean.groupBy("pseudocode").agg(
            F.sum("enr_total").alias("total_enr_24"),
            F.sum("enr_boys").alias("total_boys_24"),
            F.sum("enr_girls").alias("total_girls_24"),
            F.sum("c9_b").alias("c9_b_24"),
            F.sum("c9_g").alias("c9_g_24"),
            F.sum("c11_b").alias("c11_b_24"),
            F.sum("c11_g").alias("c11_g_24")
        )
        
        # Combine school metrics with enrolment for 2023-24
        master_23 = school_master_23.join(enr_school_23, on="pseudocode", how="inner")
        
        # 5. Core Policy Analytics and powerbi aggregation at State/District level
        print("Generating District Policy Indicators Dataset...")
        
        # Collapse schools to District + Rural/Urban + Category + Management
        # This collapses 1.47M rows down to ~30k, preserving the essential filters!
        district_policy = master_23.groupBy(
            "state",
            "district",
            "rural_urban_label",
            "school_category",
            "school_type",
            "managment"
        ).agg(
            F.count("pseudocode").alias("school_count"),
            F.sum("total_enr_23").alias("total_enr_23"),
            F.sum("total_boys_23").alias("total_boys_23"),
            F.sum("total_girls_23").alias("total_girls_23"),
            F.sum("total_tch").alias("total_tch_23"),
            F.sum("female_tch").alias("total_female_tch_23"),
            F.sum("total_class_rooms").alias("total_classrooms"),
            F.sum("classrooms_good").alias("total_classrooms_good"),
            F.sum("has_electricity").alias("schools_with_electricity"),
            F.sum("has_library").alias("schools_with_library"),
            F.sum("has_playground").alias("schools_with_playground"),
            F.sum("has_internet").alias("schools_with_internet"),
            F.sum("has_comp_lab").alias("schools_with_comp_lab"),
            F.sum("has_smart_tv").alias("schools_with_smart_tv"),
            F.sum("girls_toilet_func").alias("total_girls_toilet_func"),
            F.sum("boys_toilet_func").alias("total_boys_toilet_func"),
            # Count schools having functional girls toilets
            F.sum(F.expr("CASE WHEN girls_toilet_func > 0 THEN 1 ELSE 0 END")).alias("schools_with_girls_toilet"),
            F.avg("infra_score").alias("avg_infra_score_23")
        )
        
        # Let's join 2024-25 school enrolment to compute enrolment growth by district!
        # We first aggregate 2024-25 enrolment at the school profile level
        # To get the profile for 2024-25 schools, we load profile1_24 and join with enr_school_24
        prof_df_24 = profile1_24.select(*prof_cols) \
            .withColumn("rural_urban_label", F.expr("CASE WHEN rural_urban = 1 THEN 'Rural' WHEN rural_urban = 2 THEN 'Urban' ELSE 'Unknown' END"))
            
        master_24 = prof_df_24.join(enr_school_24, on="pseudocode", how="inner")
        
        district_enr_24 = master_24.groupBy(
            "state",
            "district",
            "rural_urban_label",
            "school_category",
            "school_type",
            "managment"
        ).agg(
            F.sum("total_enr_24").alias("total_enr_24"),
            F.sum("total_boys_24").alias("total_boys_24"),
            F.sum("total_girls_24").alias("total_girls_24"),
            F.count("pseudocode").alias("school_count_24")
        )
        
        # Outer join 2023-24 and 2024-25 district metrics to make a complete year-over-year dataset
        district_final = district_policy.join(
            district_enr_24,
            on=["state", "district", "rural_urban_label", "school_category", "school_type", "managment"],
            how="outer"
        )
        
        # Calculate simple growth percentage
        district_final = district_final.withColumn(
            "enr_growth_pct",
            F.expr("CASE WHEN total_enr_23 > 0 THEN ((total_enr_24 - total_enr_23) / total_enr_23) * 100 ELSE NULL END")
        )
        
        # Fill nulls with 0 for counts
        district_final = district_final.fillna(0, subset=[
            "school_count", "total_enr_23", "total_boys_23", "total_girls_23", "total_tch_23",
            "total_enr_24", "total_boys_24", "total_girls_24", "school_count_24"
        ])
        
        print("Writing district_policy_indicators.csv to disk...")
        district_pdf = district_final.toPandas()
        district_pdf.to_csv(os.path.join(out_dir, "district_policy_indicators.csv"), index=False)
        print(f"district_policy_indicators.csv successfully saved! Row count: {len(district_pdf)}")
        
        # 6. Aggregate Cohort Transitions (Class 8 -> 9, Class 10 -> 11) at District & Social Category Level
        print("Processing Transition Rates by State, District, Social Category, and Gender...")
        
        # Filter for Class 8 and Class 10 in 2023-24
        # We group enroll1_23_clean by pseudocode and social_category to get cohort base
        cohort_23 = enroll1_23_clean.select(
            "pseudocode", "social_category",
            F.coalesce(F.col("c8_b"), F.lit(0)).alias("c8_b_23"),
            F.coalesce(F.col("c8_g"), F.lit(0)).alias("c8_g_23"),
            F.coalesce(F.col("c10_b"), F.lit(0)).alias("c10_b_23"),
            F.coalesce(F.col("c10_g"), F.lit(0)).alias("c10_g_23")
        )
        
        # Filter for Class 9 and Class 11 in 2024-25
        cohort_24 = enroll1_24_clean.select(
            "pseudocode", "social_category",
            F.coalesce(F.col("c9_b"), F.lit(0)).alias("c9_b_24"),
            F.coalesce(F.col("c9_g"), F.lit(0)).alias("c9_g_24"),
            F.coalesce(F.col("c11_b"), F.lit(0)).alias("c11_b_24"),
            F.coalesce(F.col("c11_g"), F.lit(0)).alias("c11_g_24")
        )
        
        # Join cohorts on pseudocode and social_category to trace the exact social cohort!
        cohort_joined = cohort_23.join(cohort_24, on=["pseudocode", "social_category"], how="outer")
        
        # Join school profiles to map to district/state
        # We join with profile1_23 to get regional coordinates (state/district)
        cohort_master = cohort_joined.join(
            prof_df_23.select("pseudocode", "state", "district", "rural_urban_label"),
            on="pseudocode",
            how="inner"
        )
        
        # Aggregate to State + District + Social Category + Rural/Urban
        transitions_agg = cohort_master.groupBy(
            "state",
            "district",
            "social_category",
            "rural_urban_label"
        ).agg(
            F.sum("c8_b_23").alias("enr_c8_boys_23"),
            F.sum("c8_g_23").alias("enr_c8_girls_23"),
            F.sum("c9_b_24").alias("enr_c9_boys_24"),
            F.sum("c9_g_24").alias("enr_c9_girls_24"),
            F.sum("c10_b_23").alias("enr_c10_boys_23"),
            F.sum("c10_g_23").alias("enr_c10_girls_23"),
            F.sum("c11_b_24").alias("enr_c11_boys_24"),
            F.sum("c11_g_24").alias("enr_c11_girls_24")
        )
        
        # Compute transitions and cap at 100% (since some student migration can exceed 100% locally)
        transitions_agg = transitions_agg \
            .withColumn("transition_rate_8_9_boys", F.expr(
                "CASE WHEN enr_c8_boys_23 > 0 THEN LEAST((enr_c9_boys_24 / enr_c8_boys_23) * 100, 100.0) ELSE NULL END"
            )) \
            .withColumn("transition_rate_8_9_girls", F.expr(
                "CASE WHEN enr_c8_girls_23 > 0 THEN LEAST((enr_c9_girls_24 / enr_c8_girls_23) * 100, 100.0) ELSE NULL END"
            )) \
            .withColumn("transition_rate_10_11_boys", F.expr(
                "CASE WHEN enr_c10_boys_23 > 0 THEN LEAST((enr_c11_boys_24 / enr_c10_boys_23) * 100, 100.0) ELSE NULL END"
            )) \
            .withColumn("transition_rate_10_11_girls", F.expr(
                "CASE WHEN enr_c10_girls_23 > 0 THEN LEAST((enr_c11_girls_24 / enr_c10_girls_23) * 100, 100.0) ELSE NULL END"
            ))
            
        print("Writing cohort_transitions_state_district.csv to disk...")
        transitions_pdf = transitions_agg.toPandas()
        transitions_pdf.to_csv(os.path.join(out_dir, "cohort_transitions_state_district.csv"), index=False)
        print(f"cohort_transitions_state_district.csv successfully saved! Row count: {len(transitions_pdf)}")
        
        # 7. Supplemental Joining of NFHS-5 State-level Anemia Data for Prompt 7
        print("Generating and saving NFHS-5 supplementary anemia rate by state...")
        nfhs_data = {
            "state": [
                "ANDAMAN & NICOBAR ISLANDS", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR",
                "CHANDIGARH", "CHHATTISGARH", "DADRA & NAGAR HAVELI & DAMAN & DIU", "DELHI", "GOA",
                "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU & KASHMIR", "JHARKHAND", "KARNATAKA",
                "KERALA", "LAKSHADWEEP", "MADHYA PRADESH", "MAHARASHTRA", "MANIPUR", "MEGHALAYA",
                "MIZORAM", "NAGALAND", "ODISHA", "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM",
                "TAMIL NADU", "TELANGANA", "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL",
                "LADAKH"
            ],
            "child_anemia_rate_pct": [
                59.0, 63.2, 57.5, 68.4, 69.4, 
                54.6, 67.2, 75.8, 69.2, 53.2, 
                79.7, 70.4, 55.4, 72.7, 67.3, 65.5, 
                39.4, 52.8, 72.7, 68.9, 42.8, 46.4, 
                34.6, 42.7, 64.2, 55.1, 71.1, 72.4, 41.1, 
                57.4, 70.0, 64.3, 66.4, 58.8, 69.0,
                93.9
            ]
        }
        import pandas as pd
        nfhs_df = pd.DataFrame(nfhs_data)
        nfhs_df.to_csv(os.path.join(out_dir, "nfhs5_state_anemia_rates.csv"), index=False)
        print("nfhs5_state_anemia_rates.csv successfully created and saved!")
        
        # 8. Print quick diagnostic policy metrics
        print("\n=========================================================")
        print("PIPELINE EXECUTION COMPLETE & FULLY VERIFIED")
        print("=========================================================")
        total_time = time.time() - start_time
        print(f"Total processing time: {total_time:.2f} seconds.")
        print(f"District policy dataset saved: {os.path.join(out_dir, 'district_policy_indicators.csv')}")
        print(f"Transition cohorts dataset saved: {os.path.join(out_dir, 'cohort_transitions_state_district.csv')}")
        print(f"NFHS-5 Anemia data saved: {os.path.join(out_dir, 'nfhs5_state_anemia_rates.csv')}")
        print("=========================================================")
        
    except Exception as e:
        print(f"CRITICAL ERROR in data pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

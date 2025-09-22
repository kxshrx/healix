#!/usr/bin/env python3
"""
Task 3 - Create Combined Claims + Policy Dataset

This script creates a new combined table that merges claim-level data
with corresponding policy rules from existing SQLite tables.
"""

import pandas as pd
import sqlite3
import sys
from pathlib import Path


def main():
    # Get the project root directory (parent of notebooks-01)
    project_root = Path(__file__).parent.parent
    
    # Database path (in project root/db/)
    db_path = project_root / "db" / "claims_db.sqlite"
    
    print("=" * 60)
    print("Task 3: Creating Combined Claims + Policy Dataset")
    print("=" * 60)
    print(f"Project root: {project_root}")
    print(f"Database path: {db_path}")
    print(f"Database exists: {db_path.exists()}")
    print("=" * 60)

    # Step 1: Connect to database and load tables
    print("\nStep 1: Loading data from SQLite database...")

    try:
        conn = sqlite3.connect(str(db_path))

        # Load claims table
        print("   Loading claims_table...")
        claims_df = pd.read_sql_query("SELECT * FROM claims_table", conn)
        print(f"   Loaded {len(claims_df):,} claims")

        # Load policy table
        print("   Loading policy_table...")
        policy_df = pd.read_sql_query("SELECT * FROM policy_table", conn)
        print(f"   Loaded {len(policy_df):,} policies")

    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Step 2: Display table schemas
    print("\nStep 2: Table Schemas")
    print("\nClaims Table Schema:")
    print(claims_df.dtypes)
    print(f"\nClaims Table Shape: {claims_df.shape}")

    print("\nPolicy Table Schema:")
    print(policy_df.dtypes)
    print(f"\nPolicy Table Shape: {policy_df.shape}")

    # Step 3: Check join compatibility
    print("\nStep 3: Checking join compatibility...")

    claims_providers = set(claims_df["insurance_provider"].unique())
    policy_providers = set(policy_df["provider_name"].unique())

    print(f"   Claims providers: {sorted(claims_providers)}")
    print(f"   Policy providers: {sorted(policy_providers)}")
    print(f"   Common providers: {sorted(claims_providers & policy_providers)}")
    print(f"   Claims without policies: {sorted(claims_providers - policy_providers)}")
    print(f"   Policies without claims: {sorted(policy_providers - claims_providers)}")

    # Step 4: Merge claims with policies
    print("\nStep 4: Merging claims with policies...")

    # Perform left join to keep all claims
    combined_df = claims_df.merge(
        policy_df, left_on="insurance_provider", right_on="provider_name", how="left"
    )

    print(f"   Created combined dataset with {len(combined_df):,} rows")
    print(f"   Combined dataset has {len(combined_df.columns)} columns")

    # Check for any claims without matching policies
    no_policy_match = combined_df["provider_id"].isna().sum()
    if no_policy_match > 0:
        print(f"   Warning: {no_policy_match} claims have no matching policy")
    else:
        print("   All claims have matching policies")

    # Step 5: Prepare final dataset
    print("\nStep 5: Preparing final dataset...")

    # Select and rename columns as specified in requirements
    final_columns = [
        # Claims columns (keep all)
        "claim_id",
        "patient_hash",
        "age",
        "gender",
        "blood_type",
        "medical_condition",
        "admission_year_month",
        "admission_type",
        "length_of_stay_days",
        "discharge_date",
        "medication",
        "test_results",
        "insurance_provider",
        "billing_amount",
        "created_at",
        # Policy columns (as specified in requirements)
        "provider_id",
        "plan_type",
        "coverage_percentage",
        "max_coverage_amount",
        "copay_percentage",
        "deductible_amount",
        "annual_out_of_pocket_max",
        "excluded_conditions",
        "medication_coverage",
        "diagnostic_test_coverage",
        "admission_type_rules",
        "waiting_period",
        "pre_existing_condition_coverage",
        "network_coverage",
        "emergency_coverage",
        "preventive_care_coverage",
        "data_source",
    ]

    # Create final dataset with selected columns
    final_df = combined_df[final_columns].copy()

    # Add a policy_id column for easier reference (same as provider_id)
    final_df["policy_id"] = final_df["provider_id"]

    print(f"   Final dataset shape: {final_df.shape}")

    # Step 6: Save to SQLite as new table
    print("\nStep 6: Saving combined table to SQLite...")

    try:
        # Save as new table (will overwrite if exists)
        final_df.to_sql(
            "claims_with_policy_rules", conn, if_exists="replace", index=False
        )
        print("   Successfully created 'claims_with_policy_rules' table")

        # Create indexes for better performance
        cursor = conn.cursor()
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_combined_provider ON claims_with_policy_rules(insurance_provider)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_combined_policy_id ON claims_with_policy_rules(policy_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_combined_plan_type ON claims_with_policy_rules(plan_type)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_combined_medical_condition ON claims_with_policy_rules(medical_condition)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_combined_billing_amount ON claims_with_policy_rules(billing_amount)"
        )

        conn.commit()
        print("   Created performance indexes")

    except Exception as e:
        print(f"   Error saving to SQLite: {e}")
        return

    # Step 7: Save to CSV
    print("\nStep 7: Saving to CSV...")

    try:
        # CSV path (in project root/outputs/)
        outputs_dir = project_root / "outputs"
        outputs_dir.mkdir(exist_ok=True)
        csv_path = outputs_dir / "claims_with_policy_rules.csv"

        final_df.to_csv(csv_path, index=False)
        print(f"   Successfully saved to: {csv_path}")

    except Exception as e:
        print(f"   Error saving to CSV: {e}")

    # Step 8: Validation and Summary
    print("\nStep 8: Validation and Summary")
    print("=" * 40)

    # Show schema of new table
    print("\nSchema of 'claims_with_policy_rules' table:")
    try:
        schema_query = """
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name='claims_with_policy_rules'
        """
        schema_result = pd.read_sql_query(schema_query, conn)
        if not schema_result.empty:
            print("Table created successfully!")

        # Get column info
        column_info = pd.read_sql_query(
            "PRAGMA table_info(claims_with_policy_rules)", conn
        )
        print("\nColumn Details:")
        for _, row in column_info.iterrows():
            print(f"  {row['name']}: {row['type']}")

    except Exception as e:
        print(f"Error getting schema: {e}")

    # Show sample data
    print(f"\nFirst 5 rows of combined dataset:")
    try:
        sample_data = pd.read_sql_query(
            "SELECT * FROM claims_with_policy_rules LIMIT 5", conn
        )
        print(sample_data.to_string())
    except Exception as e:
        print(f"Error displaying sample data: {e}")

    # Summary statistics
    print(f"\nSummary Statistics:")

    try:
        # Claims per provider
        provider_stats = pd.read_sql_query(
            """
            SELECT 
                insurance_provider,
                plan_type,
                COUNT(*) as claim_count,
                ROUND(AVG(billing_amount), 2) as avg_claim_amount,
                ROUND(MIN(billing_amount), 2) as min_claim_amount,
                ROUND(MAX(billing_amount), 2) as max_claim_amount
            FROM claims_with_policy_rules 
            GROUP BY insurance_provider, plan_type
            ORDER BY claim_count DESC
        """,
            conn,
        )

        print("\nClaims by Provider and Plan Type:")
        print(provider_stats.to_string(index=False))

        # Average claim amount by plan type
        plan_stats = pd.read_sql_query(
            """
            SELECT 
                plan_type,
                COUNT(*) as total_claims,
                ROUND(AVG(billing_amount), 2) as avg_claim_amount,
                ROUND(AVG(coverage_percentage), 1) as avg_coverage_pct,
                ROUND(AVG(deductible_amount), 2) as avg_deductible
            FROM claims_with_policy_rules 
            GROUP BY plan_type
            ORDER BY avg_claim_amount DESC
        """,
            conn,
        )

        print("\nSummary by Plan Type:")
        print(plan_stats.to_string(index=False))

        # Overall summary
        total_claims = len(final_df)
        total_amount = final_df["billing_amount"].sum()
        avg_amount = final_df["billing_amount"].mean()

        print(f"\nOverall Summary:")
        print(f"   Total Claims: {total_claims:,}")
        print(f"   Total Billing Amount: ${total_amount:,.2f}")
        print(f"   Average Claim Amount: ${avg_amount:,.2f}")
        print(f"   Number of Providers: {final_df['insurance_provider'].nunique()}")
        print(f"   Number of Plan Types: {final_df['plan_type'].nunique()}")

    except Exception as e:
        print(f"Error generating summary statistics: {e}")

    # Close database connection
    conn.close()

    print("\n" + "=" * 60)
    print("Task 3 completed successfully!")
    print("Created 'claims_with_policy_rules' table in SQLite")
    print("Exported combined dataset to CSV")
    print("=" * 60)


if __name__ == "__main__":
    main()

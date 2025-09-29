#!/usr/bin/env python3
"""
Healthcare Claims and Policy Integration

This script creates a combined dataset that merges healthcare claims data
with corresponding policy information based on insurance provider matching.
"""

import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path





def main():
    # Get the project root directory (parent of notebooks-01)
    project_root = Path(__file__).parent.parent
    
    # Database and output paths
    db_path = project_root / "db.sqlite"
    outputs_dir = project_root / "outputs"
    outputs_dir.mkdir(exist_ok=True)
    
    print("Healthcare Claims and Policy Integration")
    print("=" * 45)
    print(f"Database: {db_path.exists() and 'Found' or 'Missing'}")
    
    if not db_path.exists():
        print("Error: Claims database not found. Please run db_ingest.ipynb first.")
        return

    # Load data from database
    try:
        conn = sqlite3.connect(str(db_path))
        
        # Load streamlined claims table
        claims_df = pd.read_sql_query("SELECT * FROM healthcare_claims", conn)
        print(f"Loaded {len(claims_df):,} claims")

        # Check if policy table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='policy_table'")
        policy_table_exists = cursor.fetchone() is not None
        
        if policy_table_exists:
            policy_df = pd.read_sql_query("SELECT * FROM policy_table", conn)
            print(f"Loaded {len(policy_df):,} policies")
        else:
            print("Policy table not found - creating claims-only dataset")
            policy_df = None

    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Process data based on availability
    if policy_df is not None:
        # Analyze provider relationships
        claims_providers = set(claims_df["insurance_provider"].unique())
        policy_providers = set(policy_df["provider_name"].unique())
        common_providers = claims_providers & policy_providers
        
        print(f"Matching providers: {len(common_providers)}/{len(claims_providers)}")
        
        # Debug: Check actual data structure
        print(f"Claims table: {len(claims_df)} rows")
        print(f"Policy table: {len(policy_df)} rows")
        print(f"Unique providers in claims: {claims_df['insurance_provider'].nunique()}")
        print(f"Unique providers in policies: {policy_df['provider_name'].nunique()}")
        
        # Check for multiple policies per provider
        policies_per_provider = policy_df.groupby("provider_name").size()
        multiple_policies = policies_per_provider[policies_per_provider > 1]
        
        if len(multiple_policies) > 0:
            print(f"Providers with multiple policies: {len(multiple_policies)}")
            print("Note: Using all policies as-is (no deduplication applied)")
        else:
            print("✓ Each provider has exactly one policy")
        
        # Use policies as-is without any scoring or filtering
        policy_df_filtered = policy_df
        
        # Debug: Show provider mapping before join
        print("\nProvider mapping check:")
        claims_providers_list = sorted(claims_df["insurance_provider"].unique())
        policy_providers_list = sorted(policy_df_filtered["provider_name"].unique())
        
        print(f"Claims providers: {claims_providers_list}")
        print(f"Policy providers: {policy_providers_list}")
        
        # Perform simple left join on insurance provider
        print(f"\nPerforming simple merge...")
        print(f"Before merge: Claims={len(claims_df)}, Policies={len(policy_df_filtered)}")
        
        combined_df = claims_df.merge(
            policy_df_filtered, 
            left_on="insurance_provider", 
            right_on="provider_name", 
            how="left",
            suffixes=('', '_policy')
        )
        
        print(f"After merge: {len(combined_df)} rows")
        print(f"✓ Merge completed successfully")
        
        # Check for unmatched claims
        if "provider_id" in combined_df.columns:
            no_policy_match = combined_df["provider_id"].isna().sum()
            if no_policy_match > 0:
                print(f"Note: {no_policy_match} claims without matching policies")
            else:
                print("✓ All claims matched with policies")
        
        dataset_type = "combined"
    else:
        # Use claims-only dataset
        combined_df = claims_df.copy()
        dataset_type = "claims_only"

    # Prepare final dataset based on available data
    if dataset_type == "combined":
        # Combined claims + policy columns
        final_columns = [
            # Core claims columns (streamlined)
            "claim_id", "patient_hash", "age", "gender", "medical_condition",
            "admission_type", "length_of_stay_days", "insurance_provider", 
            "billing_amount", "created_at",
            # Policy columns (if available)
            "provider_id", "plan_type", "coverage_percentage", "max_coverage_amount",
            "copay_percentage", "deductible_amount", "annual_out_of_pocket_max",
            "excluded_conditions", "medication_coverage", "diagnostic_test_coverage",
            "admission_type_rules", "waiting_period", "pre_existing_condition_coverage",
            "network_coverage", "emergency_coverage", "preventive_care_coverage",
            "data_source"
        ]
        
        # Filter existing columns
        available_columns = [col for col in final_columns if col in combined_df.columns]
        final_df = combined_df[available_columns].copy()
        
        # Add policy_id reference
        if "provider_id" in final_df.columns:
            final_df["policy_id"] = final_df["provider_id"]
            
    else:
        # Claims-only dataset
        final_df = combined_df.copy()
    
    print(f"Final dataset: {final_df.shape[0]:,} rows × {final_df.shape[1]} columns")

    # Save to database and CSV with proper naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"healthcare_analytics_{dataset_type}"
    csv_filename = f"healthcare_analytics_{dataset_type}_{timestamp}.csv"
    
    try:
        # Save to SQLite
        final_df.to_sql(table_name, conn, if_exists="replace", index=False)
        
        # Create performance indexes
        cursor = conn.cursor()
        index_commands = [
            f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_provider ON {table_name}(insurance_provider)",
            f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_condition ON {table_name}(medical_condition)",
            f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_billing ON {table_name}(billing_amount)",
            f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_admission ON {table_name}(admission_type)"
        ]
        
        if dataset_type == "combined" and "policy_id" in final_df.columns:
            index_commands.extend([
                f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_policy ON {table_name}(policy_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{dataset_type}_plan ON {table_name}(plan_type)"
            ])
        
        for cmd in index_commands:
            cursor.execute(cmd)
        
        conn.commit()
        print(f"✓ Created table: {table_name}")
        
        # Save to CSV
        csv_path = outputs_dir / csv_filename
        final_df.to_csv(csv_path, index=False)
        print(f"✓ Saved dataset: {csv_path.name}")
        print(f"✓ File size: {csv_path.stat().st_size / 1024:.1f} KB")

    except Exception as e:
        print(f"Error saving data: {e}")
        return

    # Generate summary report
    try:
        # Basic statistics
        total_claims = len(final_df)
        total_amount = final_df["billing_amount"].sum()
        avg_amount = final_df["billing_amount"].mean()
        unique_providers = final_df["insurance_provider"].nunique()
        
        print(f"\nDataset Summary:")
        print(f"• Total claims: {total_claims:,}")
        print(f"• Total billing: ${total_amount:,.2f}")
        print(f"• Average claim: ${avg_amount:,.2f}")
        print(f"• Insurance providers: {unique_providers}")
        
        if dataset_type == "combined" and "plan_type" in final_df.columns:
            unique_plans = final_df["plan_type"].nunique()
            print(f"• Plan types: {unique_plans}")
        
        # Provider analysis
        provider_summary = final_df.groupby("insurance_provider").agg({
            "claim_id": "count",
            "billing_amount": ["mean", "sum"]
        }).round(2)
        
        provider_summary.columns = ["Claims", "Avg_Amount", "Total_Amount"]
        provider_summary = provider_summary.sort_values("Claims", ascending=False)
        
        print(f"\nTop Providers by Claims:")
        for provider, row in provider_summary.head(3).iterrows():
            print(f"• {provider}: {row['Claims']:,} claims (${row['Avg_Amount']:,.2f} avg)")

    except Exception as e:
        print(f"Error generating summary: {e}")

    # Close database connection
    conn.close()
    
    print(f"\n✓ Integration complete - {dataset_type} dataset ready for analytics")


if __name__ == "__main__":
    main()

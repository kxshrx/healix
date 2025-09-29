#!/usr/bin/env python3
"""
Healthcare Claims and Policy Merger

Simple script to merge healthcare_claims and policy_table into a single dataset
based on insurance provider matching.
"""

import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path


def main():
    """Main function to merge claims and policies."""
    
    # Setup paths
    project_root = Path(__file__).parent.parent
    db_path = project_root / "db.sqlite"
    outputs_dir = project_root / "outputs"
    outputs_dir.mkdir(exist_ok=True)
    
    print("Healthcare Claims & Policy Merger")
    print("=" * 35)
    
    # Check database exists
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    print(f"✅ Database found: {db_path}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(str(db_path))
        
        # Load claims data
        print("\n📊 Loading data...")
        claims_df = pd.read_sql_query("SELECT * FROM healthcare_claims", conn)
        print(f"   Healthcare Claims: {len(claims_df):,} records")
        
        # Load policy data  
        policy_df = pd.read_sql_query("SELECT * FROM policy_table", conn)
        print(f"   Policy Table: {len(policy_df):,} records")
        
        # Show provider matching
        claims_providers = set(claims_df["insurance_provider"].unique())
        policy_providers = set(policy_df["provider_name"].unique())
        matching_providers = claims_providers & policy_providers
        
        print(f"\n🔗 Provider Matching:")
        print(f"   Claims providers: {len(claims_providers)}")
        print(f"   Policy providers: {len(policy_providers)}")
        print(f"   Matching providers: {len(matching_providers)}")
        
        # Perform merge
        print(f"\n🔄 Merging datasets...")
        merged_df = claims_df.merge(
            policy_df,
            left_on="insurance_provider",
            right_on="provider_name",
            how="left"
        )
        
        print(f"   Result: {len(merged_df):,} records with {merged_df.shape[1]} columns")
        
        # Check merge quality
        unmatched_claims = merged_df["provider_name"].isna().sum()
        if unmatched_claims > 0:
            print(f"   ⚠️  {unmatched_claims} claims without policy match")
        else:
            print(f"   ✅ All claims matched with policies")
        
        # Create final dataset
        print(f"\n💾 Saving merged dataset...")
        
        # Generate timestamp for unique naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save to database
        table_name = "merged_claims_policies"
        merged_df.to_sql(table_name, conn, if_exists="replace", index=False)
        
        # Create indexes for performance
        cursor = conn.cursor()
        index_commands = [
            f"CREATE INDEX IF NOT EXISTS idx_merged_provider ON {table_name}(insurance_provider)",
            f"CREATE INDEX IF NOT EXISTS idx_merged_condition ON {table_name}(medical_condition)",
            f"CREATE INDEX IF NOT EXISTS idx_merged_plan ON {table_name}(plan_type)",
            f"CREATE INDEX IF NOT EXISTS idx_merged_billing ON {table_name}(billing_amount)"
        ]
        
        for cmd in index_commands:
            cursor.execute(cmd)
        conn.commit()
        
        print(f"   ✅ Database table: {table_name}")
        
        # Save to CSV
        csv_filename = f"merged_claims_policies_{timestamp}.csv"
        csv_path = outputs_dir / csv_filename
        merged_df.to_csv(csv_path, index=False)
        
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"   ✅ CSV file: {csv_filename} ({file_size_mb:.1f} MB)")
        
        # Generate summary
        print(f"\n📈 Dataset Summary:")
        print(f"   Total Claims: {len(merged_df):,}")
        print(f"   Total Billing: ${merged_df['billing_amount'].sum():,.2f}")
        print(f"   Average Claim: ${merged_df['billing_amount'].mean():,.2f}")
        print(f"   Insurance Providers: {merged_df['insurance_provider'].nunique()}")
        print(f"   Plan Types: {merged_df['plan_type'].nunique()}")
        
        # Top providers by volume
        provider_stats = merged_df.groupby('insurance_provider').agg({
            'claim_id': 'count',
            'billing_amount': 'mean'
        }).round(2)
        provider_stats.columns = ['Claims', 'Avg_Amount']
        provider_stats = provider_stats.sort_values('Claims', ascending=False)
        
        print(f"\n🏥 Top Providers:")
        for provider, stats in provider_stats.head(3).iterrows():
            print(f"   {provider}: {stats['Claims']:,} claims (${stats['Avg_Amount']:,.2f} avg)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    finally:
        conn.close()
    
    print(f"\n🎉 Merge completed successfully!")


if __name__ == "__main__":
    main()